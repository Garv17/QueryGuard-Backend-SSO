import pandas as pd
import csv
import os
import re
from sqlglot import parse_one, exp
import logging
import ast
import sys
import sqlglot
from sqlglot.expressions import (
    Column, Table, Subquery, Union, With, Select, Window, Join, Identifier
)

# Logging Setup
logger = logging.getLogger("lineage.filter_clause_columns")

def safe_name(x):
  try:
    if x is None:
        return None
    return getattr(x, "name", str(x))
  except Exception as e:
        logger.error("safe_name error: %s", e, exc_info=True)
        return None

def is_base_fqn(q: str) -> bool:
  try:
    return q.count(".") >= 3
  except Exception as e:
        logger.error("is_base_fqn error: %s", e, exc_info=True)
        return False

def split_fqn(fqn: str):
    try:
        if not fqn:
            return None, None, None, None
        parts = fqn.split(".")
        if len(parts) >= 4:
            return parts[-4], parts[-3], parts[-2], parts[-1]
        if len(parts) == 3:
            return None, parts[0], parts[1], parts[2]
        if len(parts) == 2:
            return None, None, parts[0], parts[1]
        return None, None, None, parts[0]
    except Exception as e:
        logger.error("split_fqn error: %s", e, exc_info=True)
        return None, None, None, None

def join_table_fqn(catalog, db, table):
    try:
        return ".".join([p for p in [catalog, db, table] if p])
    except Exception as e:
        logger.error("join_table_fqn error: %s", e, exc_info=True)
        return None

def merge_filter_maps(acc, add):
    try:
        if not add:
            return acc
        for k, vals in add.items():
            acc.setdefault(k, set()).update(vals)
        return acc
    except Exception as e:
        logger.error("merge_filter_maps error: %s", e, exc_info=True)
        return acc

def finalize_filters(filters_set_dict):
    try:
        keys = [
            "where",
            "group_by",
            "having",
            "join_on",
            "order_by",
            "qualify",
            "window_partition_by",
            "window_order_by",
        ]
        return {k: sorted(filters_set_dict.get(k, set())) for k in keys}
    except Exception as e:
        logger.error("finalize_filters error: %s", e, exc_info=True)
        return {}

def iter_columns(value):
    try:
        if not value:
            return
        if isinstance(value, (list, tuple)):
            for v in value:
                yield from iter_columns(v)
            return
        if isinstance(value, Column):
            yield value
            return
        yield from value.find_all(Column)
    except Exception as e:
        logger.error("iter_columns error: %s", e, exc_info=True)
        return

def iter_identifiers(value):
    try:
        if not value:
            return
        if isinstance(value, (list, tuple)):
            for v in value:
                yield from iter_identifiers(v)
            return
        if isinstance(value, Identifier):
            yield value
            return
        yield from value.find_all(Identifier)
    except Exception as e:
        logger.error("iter_identifiers error: %s", e, exc_info=True)
        return

def build_select_scope(sel: Select):
    try:
        alias_to_table = {}
        subquery_aliases = {}

        for table in sel.find_all(Table):
            parent_select = table.parent
            while parent_select and not isinstance(parent_select, Select):
                parent_select = parent_select.parent
            if parent_select is not sel:
                continue

            full_name = ".".join(
                filter(None, [safe_name(table.catalog), safe_name(table.db), safe_name(table.this)])
            )
            key = safe_name(table.alias) or safe_name(table.this)
            if key:
                alias_to_table[key] = full_name

        for sq in sel.find_all(Subquery):
            parent_select = sq.parent
            while parent_select and not isinstance(parent_select, Select):
                parent_select = parent_select.parent
            if parent_select is not sel:
                continue

            if sq.alias:
                subquery_aliases[safe_name(sq.alias)] = sq.this

        with_expr = sel.args.get("with")
        if isinstance(with_expr, With):
            for cte in with_expr.find_all(Subquery):
                if cte.alias:
                    subquery_aliases[safe_name(cte.alias)] = cte.this

        return alias_to_table, subquery_aliases
    except Exception as e:
        logger.error("build_select_scope error: %s", e, exc_info=True)
        return {}, {}

def qualify_col_in_scope(col: Column, alias_to_table: dict) -> str:
    try:
        col_name = safe_name(col.this)
        tbl = safe_name(col.table)
        if tbl and tbl in alias_to_table:
            return f"{alias_to_table[tbl]}.{col_name}"
        elif tbl:
            return f"{tbl}.{col_name}"
        else:
            return col_name
    except Exception as e:
        logger.error("qualify_col_in_scope error: %s", e, exc_info=True)
        return None

def extract_clause_lineage_for_select(sel: Select, alias_to_table: dict):
    try:
        out = {
            "where": set(),
            "group_by": set(),
            "having": set(),
            "join_on": set(),
            "order_by": set(),
            "qualify": set(),
            "window_partition_by": set(),
            "window_order_by": set(),
        }

        for c in iter_columns(sel.args.get("where")):
            out["where"].add(qualify_col_in_scope(c, alias_to_table))
        for c in iter_columns(sel.args.get("group")):
            out["group_by"].add(qualify_col_in_scope(c, alias_to_table))
        for c in iter_columns(sel.args.get("having")):
            out["having"].add(qualify_col_in_scope(c, alias_to_table))
        for c in iter_columns(sel.args.get("qualify")):
            out["qualify"].add(qualify_col_in_scope(c, alias_to_table))
        for c in iter_columns(sel.args.get("order")):
            out["order_by"].add(qualify_col_in_scope(c, alias_to_table))

        for j in sel.args.get("joins", []) or []:
            if isinstance(j, Join):
                for c in iter_columns(j.args.get("on")):
                    out["join_on"].add(qualify_col_in_scope(c, alias_to_table))
                for ident in iter_identifiers(j.args.get("using")):
                    out["join_on"].add(safe_name(ident))

        for w in sel.find_all(Window):
            parent_select = w.parent
            while parent_select and not isinstance(parent_select, Select):
                parent_select = parent_select.parent
            if parent_select is not sel:
                continue

            for c in iter_columns(w.args.get("partition_by")):
                out["window_partition_by"].add(qualify_col_in_scope(c, alias_to_table))
            for c in iter_columns(w.args.get("order")):
                out["window_order_by"].add(qualify_col_in_scope(c, alias_to_table))

        return out
    except Exception as e:
        logger.error("extract_clause_lineage_for_select error: %s", e, exc_info=True)
        return {}

def collect_output_lineage_for_select(sel: Select):
    try:
        alias_to_table, _subq = build_select_scope(sel)
        filters = extract_clause_lineage_for_select(sel, alias_to_table)

        outputs = {}
        for expr in getattr(sel, "selects", []) or []:
            out_alias = safe_name(expr.alias_or_name)
            if not out_alias:
                continue
            srcs = set()
            for c in expr.find_all(Column):
                srcs.add(qualify_col_in_scope(c, alias_to_table))
            for ident in expr.find_all(Identifier):
                srcs.add(safe_name(ident))
            outputs[out_alias] = srcs

        return outputs, filters
    except Exception as e:
        logger.error("collect_output_lineage_for_select error: %s", e, exc_info=True)
        return {}, {}

def expand_sources_in_select_to_base(sel, target_col: str, _seen=None):
    try:
        if _seen is None:
            _seen = set()

        alias_to_table, subq_aliases = build_select_scope(sel)
        outputs, filters = collect_output_lineage_for_select(sel)

        produced_here = None
        for out_name in outputs:
            if out_name and target_col and out_name.lower() == target_col.lower():
                produced_here = out_name
                break
        if not produced_here:
            return None, None

        key = (id(sel), produced_here.lower())
        if key in _seen:
            return set(), filters
        _seen.add(key)

        base_sources = set()
        for src in outputs[produced_here]:
            if not src:
                continue

            if is_base_fqn(src):
                base_sources.add(src)
                continue

            if "." in src:
                alias, inner_col = src.split(".", 1)

                if alias in alias_to_table:
                    base_sources.add(f"{alias_to_table[alias]}.{inner_col}")
                    continue

                if alias in subq_aliases:
                    inner_sel = subq_aliases[alias]
                    inner_sources, _inner_filters = expand_all_occurrences(inner_sel, inner_col)
                    base_sources.update(inner_sources)
                    if _inner_filters:
                        filters = merge_filter_maps(filters, _inner_filters)
                    continue

            resolved_via_projection = False
            for other_out, other_srcs in outputs.items():
                if other_out and other_out.lower() == src.lower():
                    resolved_via_projection = True
                    for c2 in other_srcs:
                        if is_base_fqn(c2):
                            base_sources.add(c2)
                        elif "." in c2:
                            alias2, inner_col2 = c2.split(".", 1)
                            if alias2 in alias_to_table:
                                base_sources.add(f"{alias_to_table[alias2]}.{inner_col2}")
                            elif alias2 in subq_aliases:
                                inner_sel2 = subq_aliases[alias2]
                                inner_sources2, _inner_filters2 = expand_all_occurrences(inner_sel2, inner_col2)
                                base_sources.update(inner_sources2)
                                if _inner_filters2:
                                    filters = merge_filter_maps(filters, _inner_filters2)
                    break
            if resolved_via_projection:
                continue

            for _sub_alias, inner_sel in subq_aliases.items():
                inner_sources3, _inner_filters3 = expand_all_occurrences(inner_sel, src)
                if inner_sources3:
                    base_sources.update(inner_sources3)
                if _inner_filters3:
                    filters = merge_filter_maps(filters, _inner_filters3)

            if len(alias_to_table) == 1:
                only_alias = next(iter(alias_to_table))
                base_sources.add(f"{alias_to_table[only_alias]}.{src}")

        logger.debug("Expanded %s -> %s", target_col, base_sources)
        return base_sources, filters

    except Exception as e:
        logger.error("expand_sources_in_select_to_base error: %s", e, exc_info=True)
        return set(), {}


def expand_all_occurrences(root, target_col: str):
    try:
        agg_sources = set()
        agg_filters = {}

        for sel in root.find_all(Select):
            srcs, filt = expand_sources_in_select_to_base(sel, target_col)
            if srcs is not None:
                agg_sources.update(srcs)
                agg_filters = merge_filter_maps(agg_filters, filt)

        logger.debug("expand_all_occurrences for %s -> %s", target_col, agg_sources)
        return agg_sources, agg_filters

    except Exception as e:
        logger.error("expand_all_occurrences error: %s", e, exc_info=True)
        return set(), {}


def unwrap_root_query(node):
    try:
        root = node
        expr = getattr(root, "args", {}).get("expression")
        if expr is not None:
            root = expr
        if isinstance(root, Subquery):
            root = root.this
        return root
    except Exception as e:
        logger.error("unwrap_root_query error: %s", e, exc_info=True)
        return node


def collect_top_level_order_columns(parsed_root, query_root):
    try:
        orders = set()
        for n in (parsed_root, query_root):
            order_expr = getattr(n, "args", {}).get("order")
            for c in iter_columns(order_expr):
                name = safe_name(c.this)
                if name:
                    orders.add(name)
        logger.debug("Top-level order columns: %s", orders)
        return orders
    except Exception as e:
        logger.error("collect_top_level_order_columns error: %s", e, exc_info=True)
        return set()


def get_wrapper_target_table_fqn(parsed):
    try:
        if getattr(parsed, "key", None) == "INSERT":
            t = parsed.this
            if isinstance(t, Table):
                return ".".join(filter(None, [safe_name(t.catalog), safe_name(t.db), safe_name(t.this)]))
        if getattr(parsed, "key", None) == "CREATE":
            t = parsed.this
            if isinstance(t, Table):
                return ".".join(filter(None, [safe_name(t.catalog), safe_name(t.db), safe_name(t.this)]))
        return None
    except Exception as e:
        logger.error("get_wrapper_target_table_fqn error: %s", e, exc_info=True)
        return None

def get_column_full_lineage(fully_qualified_source_column_name: str,
                            fully_qualified_target_column_name: str,
                            sql_query: str):
    """
    Returns lineage mapping from source -> target with filters.
    """
    try:
        parsed = sqlglot.parse_one(sql_query, read="snowflake")
        query_root = unwrap_root_query(parsed)

        src_cat, src_db, src_table, src_col = split_fqn(fully_qualified_source_column_name)
        _tgt_cat, _tgt_db, _tgt_table, tgt_col = split_fqn(fully_qualified_target_column_name)

        sources, filters = expand_all_occurrences(query_root, tgt_col or fully_qualified_target_column_name)

        top_order = collect_top_level_order_columns(parsed, query_root)
        if top_order:
            filters = merge_filter_maps(filters, {"order_by": top_order})

        _wrapper_target_fqn = get_wrapper_target_table_fqn(parsed)

        src_table_fqn = join_table_fqn(src_cat, src_db, src_table).lower() if any([src_cat, src_db, src_table]) else None
        src_col_lower = src_col.lower() if src_col else None

        filtered_sources = []
        for s in sources:
            s_lower = s.lower()
            if is_base_fqn(s):
                parts = s_lower.split(".")
                table_fqn = ".".join(parts[:-1])
                col_part = parts[-1]
                if src_table_fqn:
                    if table_fqn == src_table_fqn and (not src_col_lower or src_col_lower == "*" or col_part == src_col_lower):
                        filtered_sources.append(s)
                else:
                    if not src_col_lower or src_col_lower == "*" or col_part == src_col_lower:
                        filtered_sources.append(s)
            else:
                if not src_table_fqn:
                    if not src_col_lower or src_col_lower == "*" or s_lower.split(".")[-1] == src_col_lower:
                        filtered_sources.append(s)

        final_sources = filtered_sources if filtered_sources else sorted(sources)

        return {
            "source_columns": sorted(set(final_sources)),
            "filters": finalize_filters(filters),
        }
    except Exception as e:
        logger.error("get_column_full_lineage error: %s", e, exc_info=True)
        return {"source_columns": [], "filters": {}}
    
def union_filter_dicts(f1: dict, f2: dict) -> dict:
    """
    Merge filter dictionaries by taking the union of values for each key.
    """
    try:
        keys = [
            "where",
            "group_by",
            "having",
            "join_on",
            "order_by",
            "qualify",
            "window_partition_by",
            "window_order_by",
        ]
        out = {}
        for k in keys:
            s1 = set(f1.get(k, []) or [])
            s2 = set(f2.get(k, []) or [])
            out[k] = sorted(s1 | s2)

        logger.debug(f"union_filter_dicts output: {out}")
        return out

    except Exception as e:
        logger.error(f"Error in union_filter_dicts: {e}", exc_info=True)
        return {}


def get_bidirectional_column_lineage(
    fully_qualified_source_column_name: str,
    fully_qualified_target_column_name: str,
    sql_query: str
) -> dict:
    """
    Get bidirectional lineage (src -> tgt and tgt -> src), combining sources and filters.
    """
    try:
        # Forward: src -> tgt
        forward = get_column_full_lineage(
            fully_qualified_source_column_name,
            fully_qualified_target_column_name,
            sql_query,
        )
        logger.debug(f"Forward lineage: {forward}")

        # Reverse: tgt -> src
        reverse = get_column_full_lineage(
            fully_qualified_target_column_name,
            fully_qualified_source_column_name,
            sql_query,
        )
        logger.debug(f"Reverse lineage: {reverse}")

        combined_sources = sorted(
            set(forward.get("source_columns", [])) | set(reverse.get("source_columns", []))
        )
        combined_filters = union_filter_dicts(
            forward.get("filters", {}),
            reverse.get("filters", {})
        )

        result = {
            "source_columns": combined_sources,
            "filters": combined_filters,
        }
        logger.info(f"Bidirectional lineage result: {result}")
        return result

    except Exception as e:
        logger.error(f"Error in get_bidirectional_column_lineage: {e}", exc_info=True)
        return {
            "source_columns": [],
            "filters": {}
        }
    
# Parse fully qualified column names
def parse_full_column(qualified_col):
    try:
        parts = qualified_col.split(".")
        if len(parts) == 4:
            return parts[0], parts[1], parts[2], parts[3]
        elif len(parts) == 3:
            return None, parts[0], parts[1], parts[2]
        elif len(parts) == 2:
            return None, None, parts[0], parts[1]
        else:
            return None, None, None, qualified_col
    except Exception as e:
        logger.error("Error parsing qualified column: %s", qualified_col, exc_info=True)
        return (None, None, None, qualified_col)
    
def sanitize_identifier(identifier: str) -> str:
    """
    Convert Snowflake-style quoted identifiers into safe names.
    Example:
      "row#" -> rownum_row
      "First Name" -> first_name
      "123abc" -> col_123abc
    """
    s = identifier.strip('"')  # remove quotes
    s = s.replace(" ", "_")    # spaces → underscores
    s = re.sub(r'[^a-zA-Z0-9_]', '_', s)  # replace special chars
    if re.match(r'^\d', s):   # if starts with number
        s = "col_" + s
    return s.lower()


def preprocess_sql(sql: str) -> str:
    """
    Dynamically clean Snowflake SQL for sqlglot parsing.
    """
    sql = re.sub(r'array\s*\(([^)]+)\)', r'\1', sql, flags=re.IGNORECASE)
    sql = re.sub(r'""([^""]+)""', lambda m: sanitize_identifier(m.group(1)), sql)
    return sql


# Resolve Recursive CTE Select Star & Subquery Problem 
def resolve_cte_column_source_issue_recursively(cte_name: str | None, column_name: str, sql: str) -> list[dict] | None:
    try:
        sql = preprocess_sql(sql)
        root = parse_one(sql, read="snowflake")

        # Build maps for CTEs and subqueries (name/alias -> Select/Query expr)
        def cte_map_of(parsed_expr):
            return {cte.alias_or_name: cte.this for cte in parsed_expr.find_all(exp.CTE)}

        def subquery_map_of(parsed_expr):
            out = {}
            for subq in parsed_expr.find_all(exp.Subquery):
                if subq.alias_or_name:
                    out[subq.alias_or_name] = subq.this  # typically a Select/Union
            return out

        cte_map = cte_map_of(root)
        subq_map = subquery_map_of(root)
        all_map = {**cte_map, **subq_map}  # everything addressable by name/alias

        # Helpers -------------------------------------------------------------

        def tables_in_select_scope(sel: exp.Select) -> list[exp.Table]:
            tables = []
            frm = sel.args.get("from")
            if frm is not None:
                tables.extend(list(frm.find_all(exp.Table)))
            for j in sel.args.get("joins") or []:
                tables.extend(list(j.find_all(exp.Table)))
            return tables

        def select_outputs_expr_for(sel: exp.Select, out_name: str):
            """
            Return the expression node that produces `out_name` in this SELECT,
            or None if this SELECT doesn't output that column.
            """
            for e in sel.expressions:
                alias = e.alias_or_name
                if alias and alias.lower() == out_name.lower():
                    return e.this if isinstance(e, exp.Alias) else e
                if isinstance(e, exp.Column) and e.name.lower() == out_name.lower():
                    return e  # bare column, same name as output
            return None

        def cte_outputs_column(cte_expr, col: str) -> bool:
            for sel in cte_expr.find_all(exp.Select):
                if select_outputs_expr_for(sel, col):
                    return True
            return False

        def unique_dicts(rows):
            seen = set()
            out = []
            for r in rows:
                key = (
                    r.get("source_database",""),
                    r.get("source_schema",""),
                    r.get("source_table",""),
                    r.get("source_column","")
                )
                if key not in seen:
                    seen.add(key)
                    out.append(r)
            return out

        # Core tracer ---------------------------------------------------------

        def trace_expr_to_sources(expr, sel_scope: exp.Select, current_cte_name: str, visited: set) -> list[dict]:
            """
            Given the expression node that produces the target column in `sel_scope`,
            return a list of {source_database, source_schema, source_table, source_column}.
            """
            # Normalize simple wrappers
            while isinstance(expr, (exp.Alias, exp.Paren, exp.Cast)):
                expr = expr.this

            # If it's a simple column, resolve it
            if isinstance(expr, exp.Column):
                src_col = expr.name
                prefix = expr.table  # table alias or name if qualified

                if prefix:
                    # Find the table in the current SELECT scope matching this prefix
                    for t in tables_in_select_scope(sel_scope):
                        if t.alias_or_name == prefix or t.name == prefix:
                            # If it points to another CTE/Subquery, recurse
                            target_key = None
                            if t.name in all_map:
                                target_key = t.name
                            elif t.alias_or_name in all_map:
                                target_key = t.alias_or_name

                            if target_key and target_key not in visited:
                                return find_sources_in_query(
                                    all_map[target_key], src_col, target_key, visited | {current_cte_name}
                                )
                            # Otherwise it's a base/physical table
                            return [{
                                "source_database": t.catalog or "",
                                "source_schema": t.db or "",
                                "source_table": t.name,
                                "source_column": src_col,
                            }]
                    # No matching table in scope; ambiguous → no guess
                    return []

                # Unqualified column: try to find which FROM source provides it
                candidates = []
                for t in tables_in_select_scope(sel_scope):
                    key = t.name if t.name in all_map else (t.alias_or_name if t.alias_or_name in all_map else None)
                    if key and key not in visited and cte_outputs_column(all_map[key], src_col):
                        candidates.append(key)
                results = []
                for key in candidates:
                    results += find_sources_in_query(all_map[key], src_col, key, visited | {current_cte_name})
                if results:
                    return unique_dicts(results)

                # If no CTE/subquery claims it, and exactly one base table in scope, attribute to it.
                base_tables = [
                    t for t in tables_in_select_scope(sel_scope)
                    if not (t.name in all_map or t.alias_or_name in all_map)
                ]
                if len(base_tables) == 1:
                    t = base_tables[0]
                    return [{
                        "source_database": t.catalog or "",
                        "source_schema": t.db or "",
                        "source_table": t.name,
                        "source_column": src_col,
                    }]
                # Ambiguous: give up rather than over-collect
                return []

            # For CASE/COALESCE/Binary Ops/Functions: collect columns used inside
            cols = list(expr.find_all(exp.Column))
            out = []
            for c in cols:
                out += trace_expr_to_sources(c, sel_scope, current_cte_name, visited)
            return unique_dicts(out)

        def find_sources_in_query(query_expr, target_col: str, current_cte_name: str, visited: set) -> list[dict]:
            """
            Find the expression that produces `target_col` inside `query_expr` (a Select/Union/etc),
            and trace it down to base sources.
            """
            # If it's a UNION/other set op, try each SELECT branch
            if isinstance(query_expr, (exp.Union, exp.Intersect, exp.Except)):
                results = []
                for side in (query_expr.left, query_expr.right):
                    sel = side if isinstance(side, exp.Select) else next(side.find_all(exp.Select), None)
                    if sel is not None:
                        expr = select_outputs_expr_for(sel, target_col)
                        if expr is not None:
                            results += trace_expr_to_sources(expr, sel, current_cte_name, visited)
                return unique_dicts(results)

            # Regular SELECT
            sel = query_expr if isinstance(query_expr, exp.Select) else next(query_expr.find_all(exp.Select), None)
            if sel is None:
                return []

            expr = select_outputs_expr_for(sel, target_col)
            if expr is None:
                return []

            return trace_expr_to_sources(expr, sel, current_cte_name, visited)

        # Extended behavior: if no cte_name passed, search across all top-level CTEs/subqueries
        if cte_name is None:
            results = []
            for name, expr in all_map.items():
                if cte_outputs_column(expr, column_name):
                    results += find_sources_in_query(expr, column_name, name, visited=set())
            return unique_dicts(results) or None

        # Kick off from the requested CTE and column
        if cte_name not in all_map:
            return None

        lineage = find_sources_in_query(all_map[cte_name], column_name, cte_name, visited=set())
        return lineage or None

    except Exception as e:
        logger.error("Error resolving recursive cte problem: %s", e, exc_info=True)
        return None
    
# Detect and Replace Named Parameters
def detect_and_replace_named_parameters(query: str, static_value: str = "null"):
    try:
        # Protect string literals inside single quotes
        string_literals = re.findall(r"'[^']*'", query)
        protected_literals = {s: f"__STRING_LITERAL_{i}__" for i, s in enumerate(string_literals)}
        
        for orig, placeholder in protected_literals.items():
            query = query.replace(orig, placeholder)

        # Regex to match :param but not ::type
        param_pattern = r"(?<!:):([a-zA-Z_][a-zA-Z0-9_]*)"
        matches = re.findall(param_pattern, query)

        if matches:
            query = re.sub(param_pattern, static_value, query)

        # Restore string literals
        for orig, placeholder in protected_literals.items():
            query = query.replace(placeholder, orig)

        return query
    except Exception as e:
        logger.error("Error in detect_and_replace_named_parameters: %s", e, exc_info=True)
        return query

def find_column_table_name(qualified_names, column_name, base_objects_accessed):
    try:
        column_name_lower = column_name.lower()
        qualified_names_lower = [q.lower() for q in qualified_names]
        
        for table in base_objects_accessed:
            object_name = table['objectName']
            object_name_lower = object_name.lower()
            
            if object_name_lower in qualified_names_lower:
                for col in table['columns']:
                    if col['columnName'].lower() == column_name_lower:
                        return object_name  # return original table name 

        return None
    except Exception as e:
        logger.error("Error in find_column_table_name: %s", e, exc_info=True)
        return None
    
def get_dependent_columns(df):
    try:
        rows = []
        for _, row in df.iterrows():
            if pd.notna(row['source_database']) and pd.notna(row['source_schema']) \
            and pd.notna(row['source_table']) and pd.notna(row['source_column']):
                fully_qualified_source_column_name = (
                    row['source_database'].lower() + '.' +
                    row['source_schema'].lower() + '.' +
                    row['source_table'].lower() + '.' +
                    row['source_column'].lower()
                )
            else:
                fully_qualified_source_column_name = None

            # fully_qualified_source_column_name = row['source_database'].lower() + '.' + row['source_schema'].lower() + '.' +row['source_table'].lower() + '.' +row['source_column'].lower()
            fully_qualified_target_column_name = row['target_database'].lower() + '.' + row['target_schema'].lower() + '.' +row['target_table'].lower() + '.' +row['target_column'].lower()
            sql_query = row.get('query_text', '')
            base_objects_accessed = row.get('base_objects_accessed', {})
            query_id = row.get('query_id', '')
            query_type = row.get('query_type', 'UNKNOWN')
            session_id = row.get('session_id', None)
            dependency_score = row.get('dependency_score', 0)
            dbt_model_file_path = row.get('dbt_model_file_path', '')
            cleaned_query = detect_and_replace_named_parameters(sql_query, static_value="null")
            result = get_bidirectional_column_lineage(fully_qualified_source_column_name, fully_qualified_target_column_name, cleaned_query)

            # Loop through all filters
            for clause, cols in result["filters"].items():
                for col in cols:
                    col = col.lower()
                    f_db, f_schema, f_table, f_col = parse_full_column(col)
                    if f_db is None and f_schema is None:
                        cte_result = resolve_cte_column_source_issue_recursively(f_table, f_col, cleaned_query)

                        if cte_result:
                            if len(cte_result) == 1:
                                f_db = cte_result[0]['source_database'].lower()
                                f_schema = cte_result[0]['source_schema'].lower()
                                f_table = cte_result[0]['source_table'].lower()
                                f_col = cte_result[0]['source_column'].lower()
                            else:
                                qualified_names = [
                                        f"{entry['source_database']}.{entry['source_schema']}.{entry['source_table']}"
                                        for entry in cte_result
                                    ]
                                relevant_qualified_table_name = find_column_table_name(qualified_names, f_col, base_objects_accessed)
                                if relevant_qualified_table_name:
                                    relevant_qualified_table_name_list = relevant_qualified_table_name.split('.')
                                    f_db = relevant_qualified_table_name_list[0].lower()
                                    f_schema = relevant_qualified_table_name_list[1].lower()
                                    f_table = relevant_qualified_table_name_list[2].lower()

                    rows.append({
                        "source_database": f_db,
                        "source_schema": f_schema,
                        "source_table": f_table,
                        "source_column": f_col,
                        "target_database": row['target_database'].lower(),
                        "target_schema": row['target_schema'].lower(),
                        "target_table": row['target_table'].lower(),
                        "target_column": row['target_column'].lower(),
                        "query_id": query_id,
                        "query_type": query_type,
                        "session_id": session_id,
                        "dependency_score": dependency_score,
                        "dbt_model_file_path": dbt_model_file_path
                    })

        return rows
        
    except Exception as e:
        logger.error("Error get_dependent_columns for query_id %s: %s", query_id, e, exc_info=True)