from typing import Dict, Any, List, Optional
from langchain.agents import Tool
from app.vector_db import get_qa_chain, LLM
from app.services.impact_analysis import schema_detection_rag, fetch_queries, IterativeConfig


def get_lineage_answer_for_org(org_id: str, question: str, k: int = 8) -> str:
    """
    Query the org's lineage vector store and return an answer with brief source previews.
    """
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
        return get_lineage_answer_for_org(org_id=org_id, question=question, k=k)

    return Tool(
        name="extract_lineage",
        func=_fn,
        description="Answer lineage questions using the organization's vector store. Input is a natural-language question.",
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
    "LLM",
]


