from typing import List, Dict, Any, Optional, Tuple
import os
import json
import uuid
import requests
import psycopg2
import psycopg2.extras
from langchain.agents import Tool
try:
    from github import GithubIntegration  # PyGithub
except Exception:  # pragma: no cover
    GithubIntegration = None  # type: ignore

from app.vector_db import get_db_connection


GITHUB_API_BASE = "https://api.github.com"
GITHUB_APP_ID = os.getenv("GITHUB_APP_ID")
PRIVATE_KEY = os.getenv("GITHUB_PRIVATE_KEY")


def _fetch_rows(query: str, params: Tuple[Any, ...]) -> List[Dict[str, Any]]:
    with get_db_connection() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        # Normalize params to primitive types psycopg2 can adapt
        norm_params = []
        for p in params:
            if isinstance(p, uuid.UUID):
                norm_params.append(str(p))
            else:
                norm_params.append(p)
        cur.execute(query, tuple(norm_params))
        rows = cur.fetchall()
        return [dict(r) for r in rows]


def fetch_pr_analyses_for_org(
    org_id: str,
    repo_full_name: Optional[str] = None,
    pr_number: Optional[int] = None,
    limit: int = 20,
) -> List[Dict[str, Any]]:
    filters: List[str] = ["org_id = %s::uuid"]
    params: List[Any] = [str(org_id)]
    if repo_full_name:
        filters.append("repo_full_name = %s")
        params.append(repo_full_name)
    if pr_number is not None:
        filters.append("pr_number = %s")
        params.append(pr_number)
    where = " AND ".join(filters)
    q = f"""
        SELECT id, org_id, installation_id, repository_id, repo_full_name, pr_number, pr_title, analysis_data, created_at
        FROM github_pr_analyses
        WHERE {where}
        ORDER BY created_at DESC
        LIMIT %s
    """
    params.append(limit)
    return _fetch_rows(q, tuple(params))


def _get_installation_for_repo(org_id: str, repo_full_name: str) -> Optional[Dict[str, Any]]:
    q = """
        SELECT gi.id as installation_pk, gi.installation_id as installation_external_id, gr.id as repo_pk,
               gr.full_name, gr.default_branch
        FROM github_installations gi
        JOIN github_repositories gr ON gr.installation_id = gi.id
        WHERE gi.org_id = %s::uuid AND gr.full_name = %s AND gi.is_active = TRUE
        LIMIT 1
    """
    rows = _fetch_rows(q, (str(org_id), repo_full_name))
    return rows[0] if rows else None


def _get_installation_token(installation_external_id: str) -> Optional[str]:
    if not (GITHUB_APP_ID and PRIVATE_KEY) or GithubIntegration is None:
        return None
    try:
        gi = GithubIntegration(int(GITHUB_APP_ID), PRIVATE_KEY)
        return gi.get_access_token(int(installation_external_id)).token
    except Exception:
        return None


def fetch_repo_tree(
    org_id: str,
    repo_full_name: str,
    recursive: bool = True,
) -> Dict[str, Any]:
    inst = _get_installation_for_repo(org_id, repo_full_name)
    if not inst:
        return {"error": "Installation/repository not found or not linked to org"}
    token = _get_installation_token(inst["installation_external_id"])
    if not token:
        return {"error": "GitHub Integration not configured"}

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github.v3+json",
    }

    # Get branch SHA
    default_branch = inst.get("default_branch") or "main"
    branch_resp = requests.get(f"{GITHUB_API_BASE}/repos/{repo_full_name}/branches/{default_branch}", headers=headers)
    if branch_resp.status_code != 200:
        return {"error": f"Failed to fetch branch: {branch_resp.text}"}
    sha = branch_resp.json().get("commit", {}).get("sha")
    if not sha:
        return {"error": "Failed to resolve commit sha"}

    params = {"recursive": 1} if recursive else {}
    tree_resp = requests.get(f"{GITHUB_API_BASE}/repos/{repo_full_name}/git/trees/{sha}", headers=headers, params=params)
    if tree_resp.status_code != 200:
        return {"error": f"Failed to fetch tree: {tree_resp.text}"}
    return tree_resp.json()


def build_org_pr_repo_tool(org_id: str, default_limit: int = 10) -> Tool:
    """
    Build a tool that can:
    - Fetch stored PR analyses for the org (optionally for a given repo/pr).
    - Fetch repository tree to provide codebase context.

    Input: natural language like "show pr 123 in org repo owner/name" or just a repo name.
    Output: concise text summary plus selected JSON blobs for the agent to reference.
    """
    def _safe_json(obj: Any) -> str:
        def _default(o):
            try:
                import uuid as _uuid
                if isinstance(o, _uuid.UUID):
                    return str(o)
            except Exception:
                pass
            return str(o)
        try:
            return json.dumps(obj, default=_default)
        except Exception:
            # Fallback stringify
            return json.dumps(json.loads(json.dumps(obj, default=_default)))

    def _fn(question: str) -> str:
        # Parse repo and PR number heuristically
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

        analyses = fetch_pr_analyses_for_org(org_id=org_id, repo_full_name=repo_full_name, pr_number=pr_number, limit=default_limit)
        tree = fetch_repo_tree(org_id=org_id, repo_full_name=repo_full_name) if repo_full_name else None

        parts: List[str] = []
        if analyses:
            parts.append(f"Found {len(analyses)} stored PR analysis record(s).")
            for idx, a in enumerate(analyses[:5], 1):
                title = a.get("pr_title") or ""
                parts.append(f"{idx}. {a.get('repo_full_name')} PR #{a.get('pr_number')} {('- ' + title) if title else ''}")
                # Try to surface a concise impact summary if present
                try:
                    files = a.get("analysis_data", {}).get("files", []) if isinstance(a.get("analysis_data"), dict) else []
                    if files:
                        first = files[0]
                        impact_preview = (first.get("impact_analysis", "") or "").strip().splitlines()[:6]
                        if impact_preview:
                            parts.append("   Impact Preview: " + " ".join(impact_preview))
                except Exception:
                    pass
        else:
            parts.append("No stored PR analyses found for the given filters.")

        # Append compact JSON for the agent/frontend
        # Flatten useful fields for frontend: files with impact, affected_query_ids, regression_queries
        flattened: List[Dict[str, Any]] = []
        for a in analyses:
            ad = a.get("analysis_data", {}) if isinstance(a.get("analysis_data"), dict) else {}
            files = ad.get("files", []) if isinstance(ad, dict) else []
            for f in files:
                flattened.append({
                    "repo_full_name": a.get("repo_full_name"),
                    "pr_number": a.get("pr_number"),
                    "pr_title": a.get("pr_title"),
                    "impact_analysis": f.get("impact_analysis"),
                    "affected_query_ids": f.get("affected_query_ids", []),
                    "regression_queries": f.get("regression_queries", []),
                    "source_metadata": f.get("source_metadata", []),
                    "sql_change": f.get("sql_change"),
                })

        payload = {
            "analyses": analyses,
            "repo_tree": tree,
            "files": flattened,
        }
        parts.append("\nDATA:\n" + _safe_json(payload)[:4000])
        return "\n".join(parts)

    return Tool(
        name="pr_repo_analysis",
        func=_fn,
        description=(
            "Fetch stored PR analyses for the org and (optionally) the GitHub repo tree. "
            "Include owner/repo and optional PR number in the input for precision."
        ),
    )


__all__ = [
    "fetch_pr_analyses_for_org",
    "fetch_repo_tree",
    "build_org_pr_repo_tool",
]


