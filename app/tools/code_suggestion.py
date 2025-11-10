from typing import List, Dict, Any, Optional
from langchain.agents import Tool
from app.tools.pr_repo import fetch_pr_analyses_for_org, fetch_pr_files_with_contents
from app.vector_db import LLM
import logging
import json

logger = logging.getLogger(__name__)


def analyze_impact_and_suggest_code(
    org_id: str,
    repo_full_name: str,
    pr_number: int,
) -> Dict[str, Any]:
    """
    Analyze PR impact report and suggest code changes to mitigate impacts.
    
    Steps:
    1. Fetch PR analysis (impact report, affected queries, etc.)
    2. Fetch PR file contents
    3. Use LLM to analyze impact types and generate code suggestions
    """
    logger.info(f"Analyzing impact and suggesting code for PR #{pr_number} in {repo_full_name}")
    
    # Step 1: Fetch PR analysis
    analyses = fetch_pr_analyses_for_org(
        org_id=org_id,
        repo_full_name=repo_full_name,
        pr_number=pr_number,
        limit=1,
    )
    
    if not analyses:
        return {"error": f"No PR analysis found for PR #{pr_number} in {repo_full_name}"}
    
    analysis = analyses[0]
    analysis_data = analysis.get("analysis_data", {})
    files_data = analysis_data.get("files", []) if isinstance(analysis_data, dict) else []
    
    if not files_data:
        return {"error": "No file analysis data found in PR analysis"}
    
    # Step 2: Fetch PR file contents
    file_contents = fetch_pr_files_with_contents(
        org_id=org_id,
        repo_full_name=repo_full_name,
        pr_number=pr_number,
        which_ref="head",
    )
    
    if "error" in file_contents:
        logger.warning(f"Could not fetch file contents: {file_contents.get('error')}")
        # Continue anyway - we can still suggest based on impact report
    
    # Step 3: Build context for LLM
    suggestions_by_file: List[Dict[str, Any]] = []
    
    for file_analysis in files_data:
        filename = None
        impact_analysis = file_analysis.get("impact_analysis", "")
        affected_query_ids = file_analysis.get("affected_query_ids", [])
        source_metadata = file_analysis.get("source_metadata", [])
        sql_change = file_analysis.get("sql_change", "")
        
        # Try to extract filename from sql_change or find matching file
        if sql_change:
            for line in sql_change.splitlines():
                if "File:" in line:
                    parts = line.split("File:")
                    if len(parts) > 1:
                        filename = parts[1].strip().split()[0] if parts[1].strip() else None
                        break
        
        # Find matching file content
        file_content_obj = None
        if "files" in file_contents and isinstance(file_contents, dict):
            for f in file_contents.get("files", []):
                if f.get("filename") == filename or (not filename and f.get("status") == "added"):
                    file_content_obj = f
                    break
        
        # Generate suggestions using LLM
        suggestion_prompt = _build_suggestion_prompt(
            filename=filename or "unknown",
            sql_change=sql_change,
            impact_analysis=impact_analysis,
            affected_query_ids=affected_query_ids,
            source_metadata=source_metadata,
            file_content=file_content_obj.get("content") if file_content_obj else None,
        )
        
        try:
            llm_response = LLM.invoke(suggestion_prompt)
            suggestion_text = getattr(llm_response, "content", str(llm_response))
            
            # Try to parse structured JSON from response
            suggestion_json = _parse_suggestion_json(suggestion_text)
            
            suggestions_by_file.append({
                "filename": filename or "unknown",
                "sql_change": sql_change,
                "impact_analysis": impact_analysis,
                "affected_query_ids": affected_query_ids,
                "source_metadata": source_metadata,
                "file_content": file_content_obj.get("content") if file_content_obj else None,
                "suggestions": suggestion_json if suggestion_json else {"raw": suggestion_text},
            })
        except Exception as e:
            logger.error(f"Failed to generate suggestions for {filename}: {e}", exc_info=True)
            suggestions_by_file.append({
                "filename": filename or "unknown",
                "error": f"Failed to generate suggestions: {str(e)}",
            })
    
    return {
        "pr_number": pr_number,
        "repo_full_name": repo_full_name,
        "suggestions_by_file": suggestions_by_file,
    }


def _build_suggestion_prompt(
    filename: str,
    sql_change: str,
    impact_analysis: str,
    affected_query_ids: List[str],
    source_metadata: List[Dict[str, Any]],
    file_content: Optional[str],
) -> str:
    """
    Build a prompt for the LLM to analyze impact and suggest code changes.
    """
    prompt = f"""You are a data engineering expert analyzing a SQL schema change and its downstream impacts.

**File:** {filename}

**SQL Change:**
{sql_change[:2000] if sql_change else "N/A"}

**Impact Analysis Report:**
{impact_analysis[:3000] if impact_analysis else "N/A"}

**Affected Query IDs:** {', '.join(affected_query_ids[:10]) if affected_query_ids else "None"}

**Source Metadata (Impacted Tables/Columns):**
{json.dumps(source_metadata[:20], indent=2) if source_metadata else "None"}

**Current File Content:**
{file_content[:2000] if file_content else "File not available or newly added"}

---

**Your Task:**

Analyze the impact report and identify:
1. **Impact Types**: What types of impacts are present? (e.g., column removal, table dependency, view breakage, ETL pipeline impact, etc.)
2. **Affected Components**: Which downstream tables, views, queries, or processes are affected?
3. **Code Changes Needed**: For each impact type, suggest specific code changes to:
   - Update downstream queries/views that reference the changed column/table
   - Modify ETL processes that depend on the changed schema
   - Update data transformations that use the affected columns
   - Fix any SELECT * statements that will break
   - Update any filters, joins, or calculations using the changed columns

**Output Format (JSON):**
{{
  "impact_types": ["type1", "type2", ...],
  "suggestions": [
    {{
      "impact_type": "column_removal",
      "affected_component": "table_name or query_id",
      "description": "What needs to be changed and why",
      "code_changes": [
        {{
          "file": "path/to/file.sql",
          "old_code": "SELECT deptcd FROM ...",
          "new_code": "SELECT department_code FROM ...",
          "explanation": "Replace removed column with alternative"
        }}
      ],
      "priority": "high|medium|low"
    }}
  ],
  "summary": "Brief summary of all suggested changes"
}}

Respond with valid JSON only, no additional text.
"""
    return prompt


def _parse_suggestion_json(text: str) -> Optional[Dict[str, Any]]:
    """
    Try to extract JSON from LLM response.
    """
    try:
        # Try direct JSON parse
        return json.loads(text)
    except Exception:
        pass
    
    try:
        # Try to find JSON block
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            return json.loads(text[start:end + 1])
    except Exception:
        pass
    
    return None


def build_org_code_suggestion_tool(org_id: str) -> Tool:
    """
    Build a LangChain Tool for code suggestions based on PR analysis.
    """
    def _fn(question: str) -> str:
        # Parse repo and PR number from question
        import re as _re
        repo_full_name = None
        pr_number = None
        
        # Repo pattern: owner/repo
        m = _re.search(r"([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)", question or "")
        if m:
            repo_full_name = m.group(1)
        
        # PR number
        n = _re.search(r"\bpr\s*#?(\d+)\b|\b#(\d+)\b|\bpr\s+(\d+)\b", (question or "").lower())
        if n:
            pr_number = int(next(g for g in n.groups() if g))
        
        if not repo_full_name or not pr_number:
            return f"Error: Could not parse repo and PR number from question. Found repo={repo_full_name}, pr={pr_number}"
        
        result = analyze_impact_and_suggest_code(
            org_id=org_id,
            repo_full_name=repo_full_name,
            pr_number=pr_number,
        )
        
        if "error" in result:
            return f"Error: {result['error']}"
        
        # Format response
        parts = [f"Code suggestions for PR #{pr_number} in {repo_full_name}:\n"]
        
        for file_suggestion in result.get("suggestions_by_file", []):
            filename = file_suggestion.get("filename", "unknown")
            parts.append(f"\n📄 File: {filename}")
            
            if "error" in file_suggestion:
                parts.append(f"  Error: {file_suggestion['error']}")
                continue
            
            suggestions = file_suggestion.get("suggestions", {})
            if isinstance(suggestions, dict) and "suggestions" in suggestions:
                for idx, sug in enumerate(suggestions["suggestions"], 1):
                    parts.append(f"\n  Suggestion {idx}:")
                    parts.append(f"    Impact Type: {sug.get('impact_type', 'unknown')}")
                    parts.append(f"    Affected: {sug.get('affected_component', 'unknown')}")
                    parts.append(f"    Priority: {sug.get('priority', 'medium')}")
                    parts.append(f"    Description: {sug.get('description', '')}")
                    
                    code_changes = sug.get("code_changes", [])
                    if code_changes:
                        parts.append(f"    Code Changes ({len(code_changes)}):")
                        for cc in code_changes:
                            parts.append(f"      File: {cc.get('file', 'unknown')}")
                            parts.append(f"      Old: {cc.get('old_code', '')[:100]}")
                            parts.append(f"      New: {cc.get('new_code', '')[:100]}")
            else:
                parts.append(f"  Raw suggestions: {str(suggestions)[:500]}")
        
        # Add structured data for frontend
        parts.append("\n\nDATA:\n" + json.dumps(result)[:4000])
        
        return "\n".join(parts)
    
    return Tool(
        name="code_suggestion",
        func=_fn,
        description=(
            "Analyze PR impact reports and suggest code changes to mitigate downstream impacts. "
            "Input should include repo name (owner/repo) and PR number (e.g., 'suggest code changes for PR 63 in owner/repo'). "
            "Returns specific code suggestions based on impact analysis."
        ),
    )


__all__ = [
    "analyze_impact_and_suggest_code",
    "build_org_code_suggestion_tool",
]

