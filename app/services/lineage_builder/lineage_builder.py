import sys
import os
# Add the project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from sql_lineage_builder import build_lineage
import logging
import sqllineage_lineage
import pandas as pd
import ast
import subprocess
from sqlalchemy import create_engine, text, update, func
from filter_clause_columns import get_dependent_columns
from sqlalchemy.orm import Session
import uuid
from datetime import datetime, timezone
from app.utils.models import (
    ColumnLevelLineage,
    FilterClauseColumnLineage,
    LineageLoadWatermark
)

logger = logging.getLogger("lineage")   # Named logger instead of root
logger.setLevel(logging.DEBUG)

if not logger.handlers:  # prevent duplicate handlers
    file_handler = logging.FileHandler("lineage.log", mode="a", encoding="utf-8")
    file_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_formatter = logging.Formatter("%(levelname)s - %(message)s")
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

# Global list to collect all lineage results
all_lineages = []
all_edges = []

def extract_sql_lineage_source_to_target(sql: str) -> dict:
    """Extract SQL lineage showing both source-to-temp-view and temp-view-to-target mappings"""
    lineage = build_lineage(sql, dialect="snowflake", enhanced_mode=True)

    temp_view_mappings = []
    target_mappings = []

    for mapping in lineage.get("source_to_target", []):
        parts = mapping.split(" <- ")
        if len(parts) == 2:
            left_side = parts[0].lower()
            right_side = parts[1].lower()

            if "__dbt_tmp" in left_side and "__dbt_tmp" not in right_side:
                temp_view_mappings.append(mapping)
            elif "__dbt_tmp" in right_side and "__dbt_tmp" not in left_side:
                target_mappings.append(mapping)
            else:
                if len(temp_view_mappings) < len(target_mappings):
                    temp_view_mappings.append(mapping)
                else:
                    target_mappings.append(mapping)

    source_to_temp_view = {}
    for mapping in temp_view_mappings:
        parts = mapping.split(" <- ")
        if len(parts) == 2:
            target_col = parts[0].lower()
            source_col = parts[1].lower()
            source_to_temp_view.setdefault(target_col, []).append(source_col)

    temp_view_to_target = {}
    for mapping in target_mappings:
        parts = mapping.split(" <- ")
        if len(parts) == 2:
            target_col = parts[0].lower()
            source_col = parts[1].lower()
            temp_view_to_target.setdefault(target_col, []).append(source_col)

    final_lineage = {}
    for target, tmp_list in temp_view_to_target.items():
        sources = []
        for tmp in tmp_list:
            if tmp in source_to_temp_view:
                sources.extend(source_to_temp_view[tmp])
        final_lineage[target] = sources if sources else None

    return final_lineage

def consolidate_lineage(all_lineages: list, all_edges: list) -> pd.DataFrame:
    """
    Consolidate dict-style and edge-style lineage into a single DataFrame
    with normalization, deduplication, and filtering.
    """
    all_lineages_records = []

    # Process dict-style lineage (from extract_sql_lineage_source_to_target)
    for lineage_dict, query_id, query_type, session_id in all_lineages:
        for target, sources in lineage_dict.items():
            if sources:
                for src in sources:
                    src_db, src_schema, src_table, src_col = sqllineage_lineage.parse_full_column(src)
                    tgt_db, tgt_schema, tgt_table, tgt_col = sqllineage_lineage.parse_full_column(target)
                    all_lineages_records.append({
                        "source_database": src_db,
                        "source_schema": src_schema,
                        "source_table": src_table,
                        "source_column": src_col,
                        "target_database": tgt_db,
                        "target_schema": tgt_schema,
                        "target_table": tgt_table,
                        "target_column": tgt_col,
                        "query_id": query_id,
                        "query_type": query_type,
                        "session_id": session_id,
                        "dbt_model_file_path": None,
                        "dependency_score": 1
                    })
            else:
                tgt_db, tgt_schema, tgt_table, tgt_col = sqllineage_lineage.parse_full_column(target)
                all_lineages_records.append({
                    "source_database": None,
                    "source_schema": None,
                    "source_table": None,
                    "source_column": None,
                    "target_database": tgt_db,
                    "target_schema": tgt_schema,
                    "target_table": tgt_table,
                    "target_column": tgt_col,
                    "query_id": query_id,
                    "query_type": query_type,
                    "session_id": session_id,
                    "dbt_model_file_path": None,
                    "dependency_score": 1
                })

    if all_lineages_records:
        all_lineages_records_df = pd.DataFrame(all_lineages_records)
        all_lineages_records_df.drop_duplicates(
                    subset=[
                        "source_database",
                        "source_schema",
                        "source_table",
                        "source_column",
                        "target_database",
                        "target_schema",
                        "target_table",
                        "target_column"
                    ],
                    inplace=True
                )
    else:
        all_lineages_records_df = pd.DataFrame()
    
    if all_edges:
        all_edges_records_df = pd.DataFrame(all_edges)

        # Deduplication on source→target
        all_edges_records_df.drop_duplicates(
            subset=[
                "source_database", "source_schema", "source_table", "source_column",
                "target_database", "target_schema", "target_table", "target_column"
            ],
            inplace=True
        )

        # Apply filters
        mask = (
            (all_edges_records_df["source_database"].notna() | all_edges_records_df["source_schema"].notna()) &  # Keep if at least one exists
            (all_edges_records_df["source_schema"].str.lower().fillna("") != "<default>") &
            (all_edges_records_df["target_schema"].str.lower().fillna("") != "<default>") &
            (~all_edges_records_df["source_table"].str.lower().fillna("").str.contains("__dbt_tmp")) &
            (~all_edges_records_df["target_table"].str.lower().fillna("").str.contains("__dbt_tmp")) &
            (all_edges_records_df["source_column"].str.strip().fillna("") != "*")
        )

        all_edges_records_df = all_edges_records_df[mask]
    else:
        all_edges_records_df = pd.DataFrame()

    # Concatenate the two DataFrames
    df = pd.concat([all_lineages_records_df, all_edges_records_df], ignore_index=True)

    return df


def apply_scd_type2(engine, model_class, current_df: pd.DataFrame, historical_df: pd.DataFrame, org_id: uuid.UUID, batch_id: uuid.UUID, connection_id: uuid.UUID):
    # Define natural key
    key_cols = ["org_id", "connection_id", "target_database", "target_schema", "target_table", "target_column"]

    # Step 1: Merge to identify existing vs new
    merged = current_df.merge(
        historical_df[key_cols + ["id"]],
        on=key_cols,
        how="left",
        indicator=True
    )

    # Rows that already exist (matched keys)
    existing_records = merged[merged["_merge"] == "both"]
    # Rows that are completely new
    new_records = merged[merged["_merge"] == "left_only"]

    # Step 2: Deactivate old rows in DB
    deactivated_count = 0
    if not existing_records.empty:
        ids_to_update = existing_records["id"].tolist()
        with Session(engine) as session:
            session.execute(
                update(ColumnLevelLineage)
                .where(ColumnLevelLineage.id.in_(ids_to_update))
                .values(is_active=0, updated_at=func.timezone('UTC', func.now()))
            )
            session.commit()
        deactivated_count = len(ids_to_update)

    # Step 3: Prepare rows to insert
    rows_to_insert_list = []

    # a) Take new versions of existing keys from consolidated_df
    if not existing_records.empty:
        updated_records = current_df.merge(
            existing_records[key_cols],
            on=key_cols,
            how="inner"
        )
        rows_to_insert_list.append(updated_records)

    # b) Add completely new lineage
    if not new_records.empty:
        rows_to_insert_list.append(new_records.drop(columns=["id", "_merge"], errors="ignore"))

    inserted_count = 0
    if rows_to_insert_list:
        rows_to_insert = pd.concat(rows_to_insert_list).drop(columns=["id", "_merge"], errors="ignore").copy()

        # set is_active=1
        rows_to_insert["is_active"] = 1

        # Step 4: Insert via ORM
        records = rows_to_insert.to_dict(orient="records")
        with Session(engine) as session:
            lineage_objects = []
            for record in records:
                lineage_objects.append(
                    model_class(
                        id=uuid.uuid4(),
                        org_id=org_id,
                        batch_id=batch_id,
                        connection_id=connection_id,
                        source_database=record.get("source_database"),
                        source_schema=record.get("source_schema"),
                        source_table=record.get("source_table"),
                        source_column=record.get("source_column"),
                        target_database=record.get("target_database"),
                        target_schema=record.get("target_schema"),
                        target_table=record.get("target_table"),
                        target_column=record.get("target_column"),
                        query_id=record.get("query_id"),
                        query_type=record.get("query_type"),
                        session_id=record.get("session_id"),
                        dependency_score=record.get("dependency_score"),
                        dbt_model_file_path=record.get("dbt_model_file_path"),
                        is_active=record.get("is_active", 1)
                    )
                )
            session.bulk_save_objects(lineage_objects)
            session.commit()
        inserted_count = len(records)

    return deactivated_count, inserted_count

def insert_lineage(engine, model_class, df: pd.DataFrame, org_id: uuid.UUID, batch_id: uuid.UUID, connection_id: uuid.UUID):
 
    if df.empty:
        return 0

    with Session(engine) as session:
        objects_to_insert = []
        for _, row in df.iterrows():
            lineage_obj = model_class(
                id=uuid.uuid4(),
                org_id=org_id,
                batch_id=batch_id,
                connection_id=connection_id,
                source_database=row.get("source_database"),
                source_schema=row.get("source_schema"),
                source_table=row.get("source_table"),
                source_column=row.get("source_column"),
                target_database=row.get("target_database"),
                target_schema=row.get("target_schema"),
                target_table=row.get("target_table"),
                target_column=row.get("target_column"),
                query_id=row.get("query_id"),  # JSONB/list is supported
                query_type=row.get("query_type"),
                session_id=row.get("session_id"),
                dependency_score=row.get("dependency_score"),
                dbt_model_file_path=row.get("dbt_model_file_path")
            )
            objects_to_insert.append(lineage_obj)

        if objects_to_insert:
            session.bulk_save_objects(objects_to_insert)
            session.commit()
            return len(objects_to_insert)
    return 0


def lineage_builder(org_id, conn_id, batch_id):
    try:
        pg_engine = sqllineage_lineage.get_pg_engine()
        fetch_query_history_df, information_schema_columns_df, historical_column_level_lineage_df, historical_filter_clause_column_lineage_df = sqllineage_lineage.fetch_query_access_history_and_information_schema_columns(pg_engine, org_id, conn_id, batch_id)
        logger.info("fetch_query_history_df, information_schema_columns_df, historical_column_level_lineage_df and  historical_filter_clause_column_lineage_df retrieved")
        last_processed_at = fetch_query_history_df["created_at"].max()
        final_df = sqllineage_lineage.combine_queries_by_session(fetch_query_history_df)
        final_df['base_objects_accessed'] = final_df['base_objects_accessed'].apply(ast.literal_eval)
        for query_id, query_text, query_type, session_id, base_objects_accessed, database_name, schema_name in final_df[['query_id', 'query_text', 'query_type', 'session_id', 'base_objects_accessed', 'database_name', 'schema_name']].values:
            try:
                cleaned_query = sqllineage_lineage.detect_and_replace_named_parameters(query_text, static_value="null")
                try:
                    final_lineage = extract_sql_lineage_source_to_target(cleaned_query)
                except Exception as e:
                    logging.error(f"[{query_id}] extract_sql_lineage failed: {e}")
                    final_lineage = {}

#             # Check if lineage is useless (all None values or empty)
                if not final_lineage or all(v is None for v in final_lineage.values()):
                    logging.info(f"[{query_id}] Falling back to parse_lineage_text()...")
                    cleaned_query = cleaned_query.upper()
                    lineage_process = subprocess.run(
                        ["sqllineage", "-e", cleaned_query, "-l", "column", "--dialect=snowflake"],
                        capture_output=True,
                        text=True
                    )

                    lineage_output = lineage_process.stdout
                    if lineage_process.returncode != 0:
                        logging.warning(f"[{query_id}] sqllineage warning: {lineage_process.stderr}")

                    all_edges.extend(sqllineage_lineage.parse_lineage_text(
                        query_id,
                        cleaned_query,
                        query_type,
                        session_id,
                        base_objects_accessed,
                        database_name,
                        schema_name,
                        information_schema_columns_df,
                        lineage_output,
                    ))

                # Only collect valid lineage
                if final_lineage and not all(v is None for v in final_lineage.values()):
                    all_lineages.append((final_lineage, query_id, query_type, session_id))
                    logging.info(f"[{query_id}] Lineage collected.")

            except Exception as loop_err:
                logging.error(f"[{query_id}] Unexpected error while processing query: {loop_err}", exc_info=True)
                # continue to next query without breaking the loop
                continue


        final_df["query_id"] = final_df["query_id"].apply(
                lambda x: str(x) if isinstance(x, list) else x
            )
        consolidated_df = consolidate_lineage(all_lineages, all_edges)
        consolidated_df["query_id"] = consolidated_df["query_id"].apply(
                lambda x: str(x) if isinstance(x, list) else x
            )

        filter_clause_df = pd.merge(consolidated_df, final_df, on="query_id", how="inner")
        rows = get_dependent_columns(filter_clause_df)
    
        if rows:
            final_filter_clause_df = pd.DataFrame(rows)
            final_filter_clause_df.drop_duplicates(
            subset=[
                "source_database", "source_schema", "source_table", "source_column",
                "target_database", "target_schema", "target_table", "target_column"
            ],
            inplace=True
            )
            mask = ~(
            final_filter_clause_df["source_database"].fillna("").eq("") &
            final_filter_clause_df["source_schema"].fillna("").eq("")
            )

            final_filter_clause_df = final_filter_clause_df[mask]
        else:
            final_filter_clause_df = pd.DataFrame()

        if not consolidated_df.empty and not final_filter_clause_df.empty:
            if not historical_column_level_lineage_df.empty and not historical_filter_clause_column_lineage_df.empty:
                deactivated_column_level_lineage, inserted_column_level_lineage, = apply_scd_type2(pg_engine, ColumnLevelLineage, consolidated_df, historical_column_level_lineage_df, org_id, batch_id, conn_id)
                logger.info(f"{deactivated_column_level_lineage} records deactivated in ColumnLevelLineage table, "f"{inserted_column_level_lineage} new records inserted in ColumnLevelLineage table.")

                deactivated_filter_clause_column_lineage, inserted_filter_clause_column_lineage, = apply_scd_type2(pg_engine, FilterClauseColumnLineage, final_filter_clause_df, historical_filter_clause_column_lineage_df, org_id, batch_id, conn_id)
                logger.info(f"{deactivated_filter_clause_column_lineage} records deactivated in FilterClauseColumnLineage table, "f"{inserted_filter_clause_column_lineage} new records inserted in FilterClauseColumnLineage table.")

            else:
                inserted_count = insert_lineage(
                    pg_engine, ColumnLevelLineage, consolidated_df, org_id=org_id, batch_id=batch_id, connection_id=conn_id
                )

                logger.info(f"Inserted {inserted_count} lineage records into column_level_lineage")

                inserted_count_filter_clause = insert_lineage(
                    pg_engine, FilterClauseColumnLineage, final_filter_clause_df, org_id=org_id, batch_id=batch_id, connection_id=conn_id
                )

                logger.info(f"Inserted {inserted_count_filter_clause} lineage records into filter_clause_column_lineage")


            with Session(pg_engine) as session:
                watermark = LineageLoadWatermark(
                org_id=org_id,
                connection_id=conn_id,
                batch_id=batch_id,
                last_processed_at=last_processed_at
                )
                session.add(watermark)
                session.commit()
                
                logger.info(f"Updated watermark for batch {batch_id}")
        else:
            logger.info(f"No lineage to process")

    except Exception as e:
        logger.critical("Fatal error in main execution: %s", e)


if __name__ == "__main__":
    lineage_builder(org_id, conn_id, batch_id)
