import logging
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

from croniter import croniter
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.utils.models import (
    SnowflakeJob,
    SnowflakeConnection,
    SnowflakeDatabase,
    SnowflakeSchema,
    SnowflakeCrawlAudit,
    SnowflakeQueryRecord,
)
import snowflake.connector

logger = logging.getLogger("snowflake_crawler")


def _due_to_run(cron_expr: str, last_run: Optional[datetime], now: datetime) -> bool:
    try:
        # For first run (last_run is None), use a time in the past to ensure we get the next scheduled time
        start_time = last_run or (now - timedelta(minutes=1))
        itr = croniter(cron_expr, start_time)
        next_time = itr.get_next(datetime)
        return next_time <= now
    except Exception:
        logger.warning("Invalid cron expression: %s", cron_expr)
        return False


def _fetch_delta_query_history(conn: SnowflakeConnection, since: datetime) -> list[dict]:
    sf_conn = snowflake.connector.connect(
        user=conn.username,
        password=conn.password,
        account=conn.account,
        warehouse=conn.warehouse,
        role=conn.role,
    )
    try:
        cursor = sf_conn.cursor()
        # Limit to selected databases/schemas if configured
        selected_db_ids = [db.id for db in conn.databases if db.is_selected]
        selected_schemas = []
        for db in conn.databases:
            if db.is_selected:
                for sc in db.schemas:
                    if sc.is_selected:
                        selected_schemas.append((db.database_name, sc.schema_name))

        where_parts = ["start_time > to_timestamp_tz(%(since)s)"]
        if selected_schemas:
            # Build OR conditions for selected schemas
            schema_conditions = []
            for i, (dbname, sname) in enumerate(selected_schemas):
                schema_conditions.append(f"(database_name=%(db_name_{i})s AND schema_name=%(schema_name_{i})s)")
            where_parts.append("(" + " OR ".join(schema_conditions) + ")")

        sql = (
            "SELECT query_id, query_text, database_name, schema_name, user_name, start_time, end_time, "
            "rows_produced, rows_inserted, rows_updated, rows_deleted "
            "FROM snowflake.account_usage.query_history WHERE " + " AND ".join(where_parts) + " ORDER BY start_time"
        )
        
        # Build parameter dictionary
        bind_params = {"since": since.isoformat()}
        if selected_schemas:
            for i, (dbname, sname) in enumerate(selected_schemas):
                bind_params[f"db_name_{i}"] = dbname
                bind_params[f"schema_name_{i}"] = sname
        
        cursor.execute(sql, bind_params)
        cols = [c[0].lower() for c in cursor.description]
        rows = [dict(zip(cols, r)) for r in cursor.fetchall()]
        return rows
    finally:
        try:
            sf_conn.close()
        except Exception:
            pass


def run_crawl_for_connection(db: Session, job: SnowflakeJob, now: datetime) -> None:
    conn: SnowflakeConnection = (
        db.query(SnowflakeConnection).filter(SnowflakeConnection.id == job.connection_id, SnowflakeConnection.is_active == True).first()
    )
    if not conn:
        logger.error("❌ Connection not found for job: %s", job.connection_id)
        return

    since = job.last_run_time or (now - timedelta(days=30))
    batch_id = uuid.uuid4()
    
    audit = SnowflakeCrawlAudit(
        batch_id=batch_id,
        connection_id=conn.id,
        scheduled_at=now,
        status="running",
    )
    db.add(audit)
    db.flush()

    try:
        rows = _fetch_delta_query_history(conn, since)
        
        max_end = since
        to_insert = []
        for r in rows:
            end_time = r.get("end_time") or r.get("start_time")
            if end_time and isinstance(end_time, str):
                try:
                    end_time = datetime.fromisoformat(end_time)
                except Exception:
                    end_time = None
            if end_time and end_time > max_end:
                max_end = end_time
            rec = SnowflakeQueryRecord(
                batch_id=batch_id,
                connection_id=conn.id,
                query_id=r.get("query_id"),
                query_text=r.get("query_text"),
                database_name=r.get("database_name"),
                schema_name=r.get("schema_name"),
                user_name=r.get("user_name"),
                start_time=r.get("start_time"),
                end_time=r.get("end_time"),
                rows_produced=r.get("rows_produced"),
                rows_inserted=r.get("rows_inserted"),
                rows_updated=r.get("rows_updated"),
                rows_deleted=r.get("rows_deleted"),
            )
            to_insert.append(rec)
        
        if to_insert:
            db.bulk_save_objects(to_insert)
            
        job.last_run_time = max_end
        audit.status = "success"
        audit.rows_fetched = len(to_insert)
        audit.finished_at = datetime.now(timezone.utc)
        db.commit()
        
        if len(to_insert) > 0:
            logger.info("📊 Crawl completed: %d rows fetched, watermark: %s", 
                       len(to_insert), max_end.strftime("%Y-%m-%d %H:%M:%S"))
        else:
            logger.info("ℹ️  Crawl completed: No new data found, watermark: %s", 
                       max_end.strftime("%Y-%m-%d %H:%M:%S"))
                   
    except Exception as e:
        db.rollback()
        audit.status = "failed"
        audit.error_message = str(e)
        audit.finished_at = datetime.now(timezone.utc)
        db.commit()
        logger.exception("💥 Crawl failed: %s", str(e))


def polling_worker(stop_event: threading.Event, interval_seconds: int = 600):
    logger.info("🚀 Starting Snowflake crawler worker (interval: %d seconds)", interval_seconds)
    cycle_count = 0
    
    while not stop_event.is_set():
        cycle_count += 1
        start_ts = time.time()
        now = datetime.now(timezone.utc)
        
        db: Session = SessionLocal()
        try:
            jobs = db.query(SnowflakeJob).filter(SnowflakeJob.is_active == True).all()
            
            if not jobs:
                logger.debug("⏸️  No active jobs found")
            else:
                due_jobs = 0
                for job in jobs:
                    if job.cron_expression and _due_to_run(job.cron_expression, job.last_run_time, now):
                        due_jobs += 1
                        logger.info("⏰ Job due: %s (cron: %s)", str(job.id)[:8], job.cron_expression)
                        run_crawl_for_connection(db, job, now)
                
                if due_jobs > 0:
                    logger.info("✅ Processed %d due jobs", due_jobs)
                
        except Exception as e:
            logger.exception("❌ Worker error: %s", str(e))
        finally:
            db.close()

        elapsed = time.time() - start_ts
        sleep_for = max(1.0, interval_seconds - elapsed)
        stop_event.wait(sleep_for)
    
    logger.info("🛑 Snowflake crawler worker stopped")


