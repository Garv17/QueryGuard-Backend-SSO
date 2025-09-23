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
        itr = croniter(cron_expr, (last_run or now))
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

        params = {"since": since.isoformat()}
        where_parts = ["start_time > to_timestamp_tz(%(since)s)"]
        if selected_schemas:
            where_parts.append("(" + " OR ".join(["(database_name=%s AND schema_name=%s)"] * len(selected_schemas)) + ")")

        sql = (
            "SELECT query_id, query_text, database_name, schema_name, user_name, start_time, end_time, "
            "rows_produced, rows_inserted, rows_updated, rows_deleted "
            "FROM snowflake.account_usage.query_history WHERE " + " AND ".join(where_parts) + " ORDER BY start_time"
        )
        binds = []
        if selected_schemas:
            for dbname, sname in selected_schemas:
                binds += [dbname, sname]
        cursor.execute(sql, binds if binds else None)
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
        logger.warning("Job connection not found or inactive: %s", job.connection_id)
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
        logger.info("Crawl success conn=%s batch=%s rows=%d", str(conn.id), str(batch_id), len(to_insert))
    except Exception as e:
        db.rollback()
        audit.status = "failed"
        audit.error_message = str(e)
        audit.finished_at = datetime.now(timezone.utc)
        db.commit()
        logger.exception("Crawl failed conn=%s batch=%s", str(job.connection_id), str(batch_id))


def polling_worker(stop_event: threading.Event, interval_seconds: int = 300):
    logger.info("Starting Snowflake polling worker with interval=%s", interval_seconds)
    while not stop_event.is_set():
        start_ts = time.time()
        now = datetime.now(timezone.utc)
        db: Session = SessionLocal()
        try:
            jobs = (
                db.query(SnowflakeJob)
                .filter(SnowflakeJob.is_active == True)
                .all()
            )
            for job in jobs:
                if job.cron_expression and _due_to_run(job.cron_expression, job.last_run_time, now):
                    run_crawl_for_connection(db, job, now)
        except Exception:
            logger.exception("Worker loop error")
        finally:
            db.close()

        elapsed = time.time() - start_ts
        sleep_for = max(1.0, interval_seconds - elapsed)
        stop_event.wait(sleep_for)


