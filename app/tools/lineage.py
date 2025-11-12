from typing import Dict, Any, List, Optional, Set
from langchain.agents import Tool
from langchain.schema import Document
from app.vector_db import get_qa_chain, CHAT_LLM, get_retriever
from app.services.impact_analysis import schema_detection_rag, fetch_queries, IterativeConfig, _docs_to_snippets, _safe_json_parse
import json
import logging

logger = logging.getLogger(__name__)


# Prompts for recursive lineage extraction
LINEAGE_PLAN_PROMPT = """
You are a lineage analyst. You will receive a lineage question and a set of context snippets from a lineage knowledge base.

Your task:
1) Extract ONLY TARGET entities (downstream columns) that have a DIRECT dependency relationship with the source entity mentioned in the question.
   - Look for explicit references showing source_column → target_column relationships
   - DO NOT include columns just because they're in the same table as a dependent column
   - ONLY include columns that are explicitly shown to depend on the source entity in the snippets
2) For each target entity found, generate a query to find ITS downstream dependencies.
3) Return a STRICT JSON as text with the following structure:
       "found_entities": ["database.schema.table.column", ...], 
       "next_queries": ["What are the downstream dependencies of database.schema.table.column?", ...], 
       "notes": "..."

CRITICAL INSTRUCTIONS:
- Extract ONLY TARGET entities that have a DIRECT dependency on the source entity from the question
- DO NOT include all columns from a table - only include columns that are explicitly shown to depend on the source
- For example, if the source is "edw.gl_summary.deptcd" and you see "allocationrule.departmentlist" depends on it, ONLY include "departmentlist", NOT all other columns from allocationrule
- For each target entity in "found_entities", create a query in "next_queries" like: "What are the downstream dependencies of [target_entity]?"
- Use EXACT entity names from snippets (e.g., "transform_zone.edw.allocationrule.departmentlist")
- If no target entities with direct dependencies are found, return empty lists
- Be SELECTIVE - only include entities with clear dependency relationships

Lineage Question:
{question}

Context Snippets (may be empty):
{snippets}

Respond only with JSON text, do not include any extra explanation.
"""

LINEAGE_FINAL_PROMPT = """
You are a lineage analyst tasked with generating a **complete multi-hop downstream lineage report**.

You are given:
1. The lineage question (identifies the source entity)
2. ALL retrieved lineage context (multi-hop) as raw snippets from multiple iterations

Your task:
- Analyze ALL snippets to build a complete downstream lineage graph
- Identify which entities are at DEPTH 1 (directly depend on source), DEPTH 2 (depend on depth 1), DEPTH 3 (depend on depth 2), etc.
- Trace the complete chain: source → depth 1 → depth 2 → depth 3 → depth 4 → depth 5
- Include ALL downstream entities found in the snippets, grouped by their depth level

HOW TO IDENTIFY DEPTH LEVELS:
- Depth 1: Entities that directly reference the source entity in snippets
- Depth 2: Entities that reference depth 1 entities (but NOT the source)
- Depth 3: Entities that reference depth 2 entities (but NOT depth 1 or source)
- Continue this pattern for depth 4, 5, etc.

---

Your output must be valid JSON with these keys:
{{
  "lineage_report": "<the full lineage report, format attached below>",
  "source_entity": "<database.schema.table.column>",
  "downstream_entities": [
    {{
      "depth": 1,
      "database": "...",
      "schema": "...",
      "table": "...",
      "column": "..."
    }}
  ]
}}

---

### 📑 Lineage Report Format (Markdown)

**Source Entity:** {source_entity}

**Complete Downstream Lineage:**

#### Depth 1 (direct dependencies - ONLY columns that directly depend on the source):
1. `database.schema.table.column`
   - Explanation: How this SPECIFIC column directly depends on the source entity {source_entity}
   - IMPORTANT: Only list this column if there is an explicit dependency relationship shown in snippets

#### Depth 2 (ONLY columns that depend on depth 1 columns):
1. `database.schema.table.column`
   - Explanation: How this SPECIFIC column depends on a specific depth 1 column (specify which depth 1 column)
   - IMPORTANT: Only list this column if there is an explicit dependency relationship with a depth 1 column

#### Depth 3 (ONLY columns that depend on depth 2 columns):
1. `database.schema.table.column`
   - Explanation: How this SPECIFIC column depends on a specific depth 2 column (specify which depth 2 column)
   - IMPORTANT: Only list this column if there is an explicit dependency relationship with a depth 2 column

(Continue for depth 4, 5 if found)

**REMEMBER:**
- DO NOT list all columns from a table just because one column in that table has a dependency
- ONLY list columns that have an explicit, direct dependency relationship shown in the snippets
- For example, if "allocationrule.departmentlist" depends on source, ONLY list "departmentlist", NOT "accountfrom", "accountto", etc. unless they also have explicit dependencies

---

**CRITICAL INSTRUCTIONS:**
- Analyze ALL snippets carefully to identify downstream entities that have DIRECT dependency relationships
- ONLY include columns that are explicitly shown to depend on the source entity (for depth 1) or on depth 1 entities (for depth 2), etc.
- DO NOT include all columns from a table just because one column in that table depends on the source
- Group entities by their actual depth level (how many hops from source)
- If an entity appears in multiple snippets, determine its correct depth based on the dependency chain
- Be SELECTIVE - only include entities with clear, explicit dependency relationships shown in the snippets
- The "lineage_report" field MUST contain the FULL detailed lineage report in Markdown format
- DO NOT just say "lineage has been extracted" - you MUST include the actual lineage details
- The lineage_report should show ONLY columns that are directly impacted by the source entity at each depth
- For each entity listed, provide a clear explanation of HOW it depends on the source (or previous depth entity)
- Ensure the JSON is strictly valid (no trailing commas, no comments, no markdown around it)
- Do not hallucinate entities not found in snippets
- If a depth has no entries, omit that depth section
- The lineage_report field is what will be shown to the user - make it accurate and focused on actual dependencies

---

Lineage Question:
{question}

Source Entity: {source_entity}

ALL Retrieved Snippets (from multiple iterations):
{snippets}
"""


LINEAGE_INTENT_CLASSIFIER_PROMPT = """
You are a lineage query classifier. Analyze the following question and determine the user's intent.

Question: {question}

Classify the intent as one of:
1. "upstream" - User wants to know what FEEDS INTO or is the SOURCE OF a column/table
   Examples: "which columns feed into X", "what is the source of X", "where does X come from", "what columns are used to populate X"
   
2. "downstream" - User wants to know what DEPENDS ON or is IMPACTED BY a column/table
   Examples: "trace lineage for X", "what depends on X", "downstream dependencies of X", "what is impacted by X"
   
3. "general" - General lineage question that doesn't clearly indicate direction

Respond with ONLY one word: "upstream", "downstream", or "general"
"""


def classify_lineage_intent(question: str) -> str:
    """
    Use LLM to classify whether the question is asking for upstream, downstream, or general lineage.
    Returns: "upstream", "downstream", or "general"
    """
    try:
        prompt = LINEAGE_INTENT_CLASSIFIER_PROMPT.format(question=question)
        response = CHAT_LLM.invoke(prompt)
        intent = response.content if hasattr(response, "content") else str(response)
        intent = intent.strip().lower()
        
        # Validate response
        if intent in ["upstream", "downstream", "general"]:
            logger.info("Lineage intent classified as: %s for question: %s", intent, question[:100])
            return intent
        else:
            logger.warning("Unexpected intent classification: %s, defaulting to general", intent)
            return "general"
    except Exception as e:
        logger.error("Error classifying lineage intent: %s, defaulting to general", str(e))
        return "general"


def get_recursive_lineage_for_org(org_id: str, question: str, max_depth: int = 5, k_per_query: int = 8) -> str:
    """
    Recursively extract complete downstream lineage up to max_depth levels.
    Similar to schema_detection_rag but focused on lineage tracing.
    """
    # Double-check intent using LLM (in case agent reformulated upstream query as downstream)
    intent = classify_lineage_intent(question)
    if intent == "upstream":
        logger.warning("Detected upstream query in recursive function, redirecting to upstream handler")
        return get_upstream_lineage_for_org(org_id=org_id, question=question, k=k_per_query)
    
    used_queries: List[str] = []
    collected_docs: List[Document] = []
    seen_texts: Set[str] = set()
    seen_entities: Set[str] = set()  # Track entities we've already queried
    frontier: List[str] = [question]
    retriever = get_retriever(org_id, k=k_per_query)

    # Iterative retrieval
    for iteration in range(max_depth):
        new_docs: List[Document] = []
        for q in frontier:
            if q in used_queries:
                continue
            used_queries.append(q)
            docs = retriever.get_relevant_documents(q)
            logger.info(
                "lineage.recursive.retrieval - org=%s iteration=%d query='%s' docs=%d",
                org_id,
                iteration + 1,
                q[:80],
                len(docs or []),
            )
            for d in docs:
                if d.page_content not in seen_texts:
                    seen_texts.add(d.page_content)
                    new_docs.append(d)
                    collected_docs.append(d)
        
        if not new_docs:
            logger.info("No new documents found at iteration %d, stopping", iteration + 1)
            break
        
        snippets_text = _docs_to_snippets(new_docs)
        plan = CHAT_LLM.invoke(LINEAGE_PLAN_PROMPT.format(question=question, snippets=snippets_text))
        plan_text = plan.content if hasattr(plan, "content") else str(plan)
        plan_json = _safe_json_parse(plan_text)
        
        found_entities = plan_json.get("found_entities", []) or []
        next_queries_raw = plan_json.get("next_queries", []) or []
        
        # Build queries for entities we haven't seen yet
        next_queries: List[str] = []
        for entity in found_entities:
            entity_lower = entity.lower().strip()
            if entity_lower and entity_lower not in seen_entities:
                seen_entities.add(entity_lower)
                # Generate a query for this entity's downstream dependencies
                query = f"What are the downstream dependencies of {entity}?"
                if query not in used_queries:
                    next_queries.append(query)
        
        # Also add any explicitly suggested queries that aren't duplicates
        for q in next_queries_raw:
            q_clean = q.strip()
            if q_clean and q_clean not in used_queries:
                next_queries.append(q_clean)
        
        logger.info(
            "lineage.recursive.planning - iteration=%d found_entities=%d next_queries=%d",
            iteration + 1,
            len(found_entities),
            len(next_queries),
        )
        
        frontier = next_queries
        
        if not frontier:
            logger.info("No more queries to follow at iteration %d, stopping", iteration + 1)
            break

    # Generate final lineage report
    all_snippets_text = _docs_to_snippets(collected_docs) if collected_docs else "(no snippets retrieved)"
    
    # Extract source entity from question (improved heuristics)
    source_entity = question
    question_lower = question.lower()
    
    # Try to extract entity from common patterns
    if "lineage for" in question_lower:
        parts = question_lower.split("lineage for")
        if len(parts) > 1:
            entity_part = parts[1]
            # Handle "X column in Y" pattern
            if " in " in entity_part:
                # Extract "X column" part
                source_entity = entity_part.split(" in ")[0].strip()
                # Remove "column" if present
                source_entity = source_entity.replace(" column", "").replace("column ", "").strip()
                # Get the table part
                table_part = entity_part.split(" in ")[1].split(" to")[0].strip()
                # Combine: table.column
                source_entity = f"{table_part}.{source_entity}"
            else:
                source_entity = entity_part.split("to")[0].strip()
    elif "trace" in question_lower and "lineage" in question_lower:
        # Pattern: "Trace lineage for X"
        parts = question_lower.split("lineage")
        if len(parts) > 1:
            remaining = parts[1]
            if "for" in remaining:
                source_entity = remaining.split("for")[1].split("to")[0].strip()
    elif "downstream" in question_lower:
        # Pattern: "What are downstream dependencies of X"
        if "of" in question_lower:
            source_entity = question_lower.split("of")[1].strip().rstrip("?")
    
    # Clean up the source entity
    source_entity = source_entity.strip().rstrip("?.,")
    
    # If we have a column name but need to construct full path, try to infer
    # For pattern like "deptcd column in edw.gl_summary", we want "edw.gl_summary.deptcd"
    if "." not in source_entity and " in " in question_lower:
        # Try to extract table from "in X" pattern
        if " in " in question_lower:
            table_part = question_lower.split(" in ")[1].split(" to")[0].split(" column")[0].strip()
            if table_part:
                source_entity = f"{table_part}.{source_entity}"
    
    logger.info(
        "lineage.recursive.final - org=%s collected_docs=%d source_entity='%s'",
        org_id,
        len(collected_docs),
        source_entity[:100],
    )
    
    final = CHAT_LLM.invoke(LINEAGE_FINAL_PROMPT.format(question=question, snippets=all_snippets_text, source_entity=source_entity))
    final_text = final.content if hasattr(final, "content") else str(final)
    
    logger.info("lineage.recursive.final.response - response_length=%d", len(final_text))
    
    # Try to parse JSON
    final_json = _safe_json_parse(final_text)
    
    # Extract lineage_report from JSON
    lineage_report = final_json.get("lineage_report", "")
    
    # If JSON parsing failed or lineage_report is empty, try to extract from markdown/text
    if not lineage_report or lineage_report == "No complete lineage found.":
        logger.warning("lineage.recursive.final - JSON parsing may have failed, trying fallback extraction")
        
        # Try to extract markdown content between markers
        if "**Complete Downstream Lineage:**" in final_text or "#### Depth" in final_text:
            # Extract the markdown section
            if "**Complete Downstream Lineage:**" in final_text:
                parts = final_text.split("**Complete Downstream Lineage:**")
                if len(parts) > 1:
                    lineage_report = parts[1].strip()
                    # Remove any trailing JSON closing braces
                    if lineage_report.endswith("}"):
                        lineage_report = lineage_report.rsplit("}", 1)[0].strip()
        elif "lineage_report" in final_text.lower():
            # Try to extract from JSON-like structure even if not valid JSON
            import re
            match = re.search(r'"lineage_report"\s*:\s*"([^"]+)"', final_text, re.DOTALL)
            if not match:
                match = re.search(r'"lineage_report"\s*:\s*"([^"]*(?:\\.|[^"\\])*)"', final_text, re.DOTALL)
            if match:
                lineage_report = match.group(1).replace('\\n', '\n').replace('\\"', '"')
        
        # If still empty, use the full response (might be markdown already)
        if not lineage_report or len(lineage_report.strip()) < 50:
            logger.warning("lineage.recursive.final - Using full response as fallback")
            lineage_report = final_text.strip()
            # Remove JSON wrapper if present
            if lineage_report.startswith("{") and '"lineage_report"' in lineage_report:
                # Try to extract just the report content
                try:
                    import json
                    temp_json = json.loads(final_text)
                    lineage_report = temp_json.get("lineage_report", final_text)
                except:
                    pass
    
    # Check if the report is just a confirmation message (too short or generic)
    lineage_report_lower = lineage_report.lower() if lineage_report else ""
    is_generic_confirmation = any(phrase in lineage_report_lower for phrase in [
        "successfully extracted",
        "has been extracted",
        "extraction complete",
        "lineage extracted",
    ]) and len(lineage_report.strip()) < 200
    
    # Final fallback: if we have collected docs but no meaningful report, create a detailed summary
    if (not lineage_report or len(lineage_report.strip()) < 200 or is_generic_confirmation) and collected_docs:
        logger.warning(
            "lineage.recursive.final - Report too short or generic (%d chars), creating detailed summary from collected docs",
            len(lineage_report.strip()) if lineage_report else 0
        )
        
        # Use LLM to generate a summary from the collected snippets
        summary_prompt = f"""You are a lineage analyst. Generate a complete downstream lineage report from the following snippets.

Source Entity: {source_entity}

Snippets:
{all_snippets_text[:4000]}

Generate a detailed lineage report showing:
1. Source entity
2. Depth 1 (direct dependencies)
3. Depth 2 (dependencies of depth 1)
4. Continue for all depths found

Format as Markdown with clear depth levels. Include entity names and brief explanations."""
        
        try:
            summary_response = CHAT_LLM.invoke(summary_prompt)
            summary_text = summary_response.content if hasattr(summary_response, "content") else str(summary_response)
            if summary_text and len(summary_text.strip()) > 100:
                lineage_report = summary_text.strip()
                logger.info("lineage.recursive.final - Generated summary from LLM, length=%d", len(lineage_report))
            else:
                raise ValueError("LLM summary too short")
        except Exception as e:
            logger.warning("lineage.recursive.final - LLM summary failed: %s, using snippet summary", str(e))
            # Fallback to snippet-based summary
            lineage_report = f"**Source Entity:** {source_entity}\n\n**Complete Downstream Lineage:**\n\n"
            lineage_report += f"Found {len(collected_docs)} lineage documents across multiple depth levels.\n\n"
            lineage_report += "**Lineage Information:**\n\n"
            lineage_report += all_snippets_text[:3000]  # First 3000 chars of snippets
    
    if not lineage_report or len(lineage_report.strip()) < 50:
        lineage_report = f"**Source Entity:** {source_entity}\n\n**Status:** Found {len(collected_docs)} relevant lineage documents. Unable to generate formatted report. Please review the lineage data directly."
    
    logger.info("lineage.recursive.final.result - report_length=%d, is_generic=%s", len(lineage_report), is_generic_confirmation)
    return lineage_report


UPSTREAM_LINEAGE_PROMPT = """
You are a lineage analyst. You will receive a question asking which columns feed into a target column, and context snippets from a lineage knowledge base.

Your task:
1) Identify the TARGET column from the question (e.g., "departmentlist in dimsharedservicesallocationrule" or "dimsharedservicesallocationrule.departmentlist")
2) From the snippets, extract ONLY the SPECIFIC source columns that directly feed into this target column
3) DO NOT include all columns from a source table - only include columns that are explicitly shown to feed into the target

CRITICAL RULES - READ CAREFULLY:
- If the question asks "which columns feed into departmentlist in dimsharedservicesallocationrule", 
  you need to find columns that FEED INTO (are sources of) departmentlist, NOT columns that departmentlist feeds into
- Look for patterns like: source_column → target_column (where target is the one mentioned in the question)
- If you see "allocationrule.departmentlist" → "dimsharedservicesallocationrule.departmentlist" in snippets,
  ONLY list "allocationrule.departmentlist" as the source, NOT other columns from allocationrule
- DO NOT list columns just because they're in the same table as a source column
- Only list columns that have an explicit, direct dependency relationship shown in the snippets
- Be EXTREMELY SELECTIVE - if a snippet shows 10 columns from allocationrule table, but only 1 feeds into the target, list ONLY that 1 column
- Format your answer as a numbered list with ONLY the specific columns that feed into the target

Question:
{question}

Context Snippets:
{snippets}

Provide a clear, numbered list of ONLY the specific columns that feed into the target. 
Example: If target is "dimsharedservicesallocationrule.departmentlist" and only "allocationrule.departmentlist" feeds into it, 
your answer should be:
1. `transform_zone.edw.allocationrule.departmentlist`

Do NOT include other columns from allocationrule unless they also explicitly feed into the target.
"""


def get_upstream_lineage_for_org(org_id: str, question: str, k: int = 8) -> str:
    """
    Extract upstream lineage (what feeds into a target column).
    Uses retriever + CHAT_LLM with structured prompt to ensure only specific source columns are shown.
    """
    retriever = get_retriever(org_id, k=k)
    docs = retriever.get_relevant_documents(question)
    
    snippets_text = _docs_to_snippets(docs) if docs else "(no snippets found)"
    
    # Use CHAT_LLM with structured prompt
    prompt = UPSTREAM_LINEAGE_PROMPT.format(question=question, snippets=snippets_text)
    response = CHAT_LLM.invoke(prompt)
    answer = response.content if hasattr(response, "content") else str(response)
    
    # Post-process to ensure specificity
    if answer:
        # Check if answer lists too many columns (might be including all columns from a table)
        lines = [line.strip() for line in answer.split('\n') if line.strip()]
        column_lines = [line for line in lines if ('`' in line or '.' in line) and any(c.isalnum() for c in line)]
        
        # If we have many columns, add a note to verify
        if len(column_lines) > 3:
            answer = f"{answer}\n\n⚠️ Note: Please verify that each listed column has an explicit dependency relationship with the target column. Only columns that directly feed into the target should be included."
    
    # Add source previews
    previews: List[str] = []
    for doc in docs[:3]:  # Limit to first 3 for brevity
        preview = (doc.page_content or "").strip()[:200]
        if preview:
            previews.append(preview)
    
    sources_block = ("\n\nSource Context:\n- " + "\n- ".join(previews)) if previews else ""
    return (answer or "No upstream dependencies found.") + sources_block


def get_lineage_answer_for_org(org_id: str, question: str, k: int = 8, original_question: Optional[str] = None) -> str:
    """
    Query the org's lineage vector store and return an answer with brief source previews.
    
    Uses LLM-based intent classification to determine if question is asking for:
    - Upstream lineage (what feeds into X): uses enhanced QA chain
    - Downstream lineage (what depends on X): uses recursive extraction
    - General lineage: uses simple QA chain
    
    Args:
        original_question: The original user question (if available) to help detect upstream queries
    """
    # Use original question for classification if available (helps when agent reformulates)
    classification_question = original_question or question
    
    # Classify intent using LLM
    intent = classify_lineage_intent(classification_question)
    
    question_lower = question.lower()
    
    if intent == "upstream":
        logger.info("Using upstream lineage extraction for question: %s (original: %s)", question[:100], original_question[:100] if original_question else "N/A")
        return get_upstream_lineage_for_org(org_id=org_id, question=original_question or question, k=k)
    elif intent == "downstream":
        # Check if user wants complete/downstream lineage with recursive extraction
        needs_recursive = any(keyword in question_lower for keyword in [
            "complete", "downstream", "full", "all", "entire", "trace", "follow"
        ])
        
        if needs_recursive:
            logger.info("Using recursive downstream lineage extraction for question: %s", question[:100])
            return get_recursive_lineage_for_org(org_id=org_id, question=question, max_depth=5, k_per_query=k)
        else:
            # Simple downstream query
            logger.info("Using simple downstream lineage extraction for question: %s", question[:100])
            qa = get_qa_chain(org_id=org_id, k=k)
            result: Dict[str, Any] = qa.invoke({"query": question})
            answer = result.get("result", "") or result.get("answer", "")
            previews: List[str] = []
            for doc in result.get("source_documents", []) or []:
                preview = (getattr(doc, "page_content", "") or "").splitlines()[:5]
                if preview:
                    previews.append("\n".join(preview))
            sources_block = ("\n\nSources:\n- " + "\n- ".join(previews)) if previews else ""
            return (answer or "No answer found.") + sources_block
    
    # Simple QA chain for basic lineage questions
    qa = get_qa_chain(org_id=org_id, k=k)
    result: Dict[str, Any] = qa.invoke({"query": question})
    answer = result.get("result", "") or result.get("answer", "")

    previews: List[str] = []
    for doc in result.get("source_documents", []) or []:
        preview = (getattr(doc, "page_content", "") or "").splitlines()[:5]
        if preview:
            previews.append("\n".join(preview))

    sources_block = ("\n\nSources:\n- " + "\n- ".join(previews)) if previews else ""
    return (answer or "No answer found.") + sources_block


def build_org_lineage_tool(org_id: str, k: int = 8) -> Tool:
    """
    Build a LangChain Tool bound to a specific org_id for lineage Q&A.
    """
    def _fn(question: str) -> str:
        # Store original question for upstream detection
        return get_lineage_answer_for_org(org_id=org_id, question=question, k=k, original_question=question)

    return Tool(
        name="extract_lineage",
        func=_fn,
        description=(
            "Answer lineage questions using the organization's vector store. "
            "IMPORTANT: Distinguish between upstream and downstream queries:\n"
            "- UPSTREAM: Questions asking 'which columns feed into X', 'what feeds X', 'source of X' - shows what feeds INTO a column\n"
            "- DOWNSTREAM: Questions asking 'trace lineage for X', 'downstream dependencies of X' - shows what depends ON a column\n"
            "For downstream questions with 'complete', 'full', 'all', or 'trace', automatically extracts multi-hop lineage up to 5 depths. "
            "For upstream questions, shows only the specific source columns that directly feed into the target. "
            "Input is a natural-language question (e.g., 'Which columns feed into departmentlist in dimsharedservicesallocationrule' or 'Trace lineage for edw.gl_summary.deptcd to downstream columns')."
        ),
    )


def get_query_history_search_for_org(
    org_id: str,
    question: str,
    max_iters: Optional[int] = 5,
) -> str:
    """
    Analyze schema/SQL changes and return impacted queries using the org's vector store.
    
    Takes a natural language question about a schema change (e.g., "I am going to remove x column")
    and returns a formatted response with impacted queries.
    """
    # Use schema_detection_rag to find impacted queries
    cfg = IterativeConfig(max_iters=max_iters) if max_iters else None
    result = schema_detection_rag(change_text=question, org_id=org_id, cfg=cfg)
    
    impact_report = result.get("impact_report", "")
    affected_query_ids = result.get("affected_query_ids", [])
    
    # Fetch actual query texts for the affected query IDs
    regression_queries = []
    if affected_query_ids:
        regression_queries = fetch_queries(affected_query_ids)
    
    # Format the response
    response_parts = []
    
    if impact_report:
        response_parts.append("📊 Impact Analysis Report:\n" + impact_report)
    
    if affected_query_ids:
        response_parts.append(f"\n🔍 Found {len(affected_query_ids)} impacted query IDs: {', '.join(affected_query_ids[:10])}")
        if len(affected_query_ids) > 10:
            response_parts.append(f"(and {len(affected_query_ids) - 10} more)")
    
    if regression_queries:
        response_parts.append("\n📝 Impacted Queries:")
        for idx, query_info in enumerate(regression_queries[:10], 1):  # Limit to first 10 for brevity
            query_id = query_info.get("query_id", "Unknown")
            query_text = query_info.get("query_text", "")
            preview = query_text[:200] + "..." if len(query_text) > 200 else query_text
            response_parts.append(f"\n{idx}. Query ID: {query_id}\n   Preview: {preview}")
        
        if len(regression_queries) > 10:
            response_parts.append(f"\n... and {len(regression_queries) - 10} more queries")
    else:
        response_parts.append("\n⚠️ No impacted queries found in query history.")
    
    return "\n".join(response_parts) if response_parts else "No impact analysis results found."


def build_org_query_history_tool(org_id: str, max_iters: Optional[int] = 5) -> Tool:
    """
    Build a LangChain Tool bound to a specific org_id for query history search.
    Analyzes schema changes and finds impacted queries.
    """
    def _fn(question: str) -> str:
        return get_query_history_search_for_org(org_id=org_id, question=question, max_iters=max_iters)
    
    return Tool(
        name="query_history_search",
        func=_fn,
        description=(
            "Analyze schema/SQL changes and find impacted queries. Input is a natural-language description of a change "
            "(e.g., 'I am going to remove column X from table Y'). ALWAYS return a numbered list with each item's "
            "query_id and a short SQL preview (first ~200 chars)."
        ),
    )


__all__ = [
    "get_lineage_answer_for_org",
    "build_org_lineage_tool",
    "get_query_history_search_for_org",
    "build_org_query_history_tool",
    "CHAT_LLM",
]


