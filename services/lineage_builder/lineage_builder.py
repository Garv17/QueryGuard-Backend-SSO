from sql_lineage_builder import build_lineage
import logging
import sqllineage_lineage
import pandas as pd
import ast
import subprocess
from sqlalchemy import create_engine, text
from filter_clause_columns import get_dependent_columns
import sys


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

def main(conn_id):
    try:
        pg_engine = sqllineage_lineage.get_pg_engine()
        fetch_query_history_df, information_schema_columns_df = sqllineage_lineage.fetch_query_access_history_and_information_schema_columns(pg_engine, conn_id)
        logger.info("fetch_query_history_df and information_schema_columns_df retrieved")
        latest_batch_id = fetch_query_history_df["batch_id"].iloc[0]
        last_processed_at = fetch_query_history_df["created_at"].max()
        connection_id = fetch_query_history_df["connection_id"].iloc[0]
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

        if not consolidated_df.empty:
            with pg_engine.begin() as connection:
                consolidated_df.to_sql("column_level_lineage", connection, if_exists="append", index=False)
                logger.info(f"Inserted {len(consolidated_df)} lineage records into column_level_lineage")

        if not final_filter_clause_df.empty:
            with pg_engine.begin() as connection:
                final_filter_clause_df.to_sql("filter_clause_dependent_column_lineage", connection, if_exists="append", index=False)
                logger.info(f"Inserted {len(final_filter_clause_df)} lineage records into filter_clause_dependent_column_lineage")


    except Exception as e:
        logger.critical("Fatal error in main execution: %s", e)


if __name__ == "__main__":
    main()
