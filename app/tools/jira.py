from typing import Dict, Any, Optional, List
from langchain.agents import Tool
import uuid
import psycopg2
import psycopg2.extras
import requests
from requests.auth import HTTPBasicAuth
import logging

from app.vector_db import get_db_connection
from app.vector_db import LLM

logger = logging.getLogger(__name__)


def _fetch_rows(query: str, params: tuple) -> List[Dict[str, Any]]:
    """Helper to fetch rows from database"""
    with get_db_connection() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        norm_params = []
        for p in params:
            if isinstance(p, uuid.UUID):
                norm_params.append(str(p))
            else:
                norm_params.append(p)
        cur.execute(query, tuple(norm_params))
        rows = cur.fetchall()
        return [dict(r) for r in rows]


def get_active_jira_connection(org_id: str) -> Optional[Dict[str, Any]]:
    """
    Get the active Jira connection for an organization.
    Returns connection details or None if not found.
    """
    q = """
        SELECT id, connection_name, server_url, username, api_token, project_key, issue_type
        FROM jira_connections
        WHERE org_id = %s::uuid AND is_active = TRUE
        LIMIT 1
    """
    rows = _fetch_rows(q, (str(org_id),))
    return rows[0] if rows else None


def create_jira_issue(
    server_url: str,
    username: str,
    api_token: str,
    project_key: str,
    summary: str,
    description: str,
    issue_type: str,
    priority: Optional[str] = None,
    assignee: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create a Jira issue using the Jira API.
    Returns dict with key, id, and url.
    """
    try:
        server_url = server_url.rstrip('/')
        auth = HTTPBasicAuth(username, api_token)
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        
        # Build issue data
        issue_data = {
            "fields": {
                "project": {"key": project_key},
                "summary": summary,
                "description": description,
                "issuetype": {"name": issue_type}
            }
        }
        
        # Add optional fields
        if priority:
            issue_data["fields"]["priority"] = {"name": priority}
        
        if assignee:
            issue_data["fields"]["assignee"] = {"name": assignee}
        
        response = requests.post(
            f"{server_url}/rest/api/2/issue",
            auth=auth,
            headers=headers,
            json=issue_data,
            timeout=15
        )
        
        if response.status_code == 201:
            issue_response = response.json()
            logger.info("Created Jira issue: %s", issue_response.get("key"))
            return {
                "key": issue_response["key"],
                "id": issue_response["id"],
                "url": f"{server_url}/browse/{issue_response['key']}"
            }
        else:
            error_msg = f"Failed to create issue: {response.status_code} - {response.text}"
            logger.error(error_msg)
            return {"error": error_msg}
    except Exception as e:
        error_msg = f"Failed to create Jira issue: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {"error": error_msg}


def create_jira_ticket_for_org(
    org_id: str,
    summary: str,
    description: str,
    issue_type: Optional[str] = None,
    priority: Optional[str] = None,
    assignee: Optional[str] = None,
    pr_url: Optional[str] = None,
    analysis_report_url: Optional[str] = None,
    user_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create a Jira ticket for an organization.
    Uses the org's active Jira connection.
    """
    logger.info(f"Creating Jira ticket for org_id={org_id}, summary={summary[:50]}...")
    
    # Get active Jira connection
    connection = get_active_jira_connection(org_id)
    if not connection:
        return {"error": "No active Jira connection found for this organization"}
    
    # Use connection defaults if not provided
    final_issue_type = issue_type or connection.get("issue_type") or "Task"
    
    # Create the Jira issue
    issue_result = create_jira_issue(
        server_url=connection["server_url"],
        username=connection["username"],
        api_token=connection["api_token"],
        project_key=connection["project_key"],
        summary=summary,
        description=description,
        issue_type=final_issue_type,
        priority=priority,
        assignee=assignee,
    )
    
    if "error" in issue_result:
        return issue_result
    
    # Save ticket to database
    try:
        # Generate UUID for the ticket id
        ticket_id = str(uuid.uuid4())
        
        q = """
            INSERT INTO jira_tickets (
                id, connection_id, ticket_key, ticket_url, summary, description,
                issue_type, status, priority, assignee, pr_url, analysis_report_url, created_by
            )
            VALUES (
                %s::uuid, %s::uuid, %s, %s, %s, %s, %s, 'Open', %s, %s, %s, %s, %s::uuid
            )
            RETURNING id, ticket_key, ticket_url, summary, description, issue_type, status, priority, assignee, pr_url, analysis_report_url, created_at
        """
        # Use provided user_id or placeholder (user_id should be passed from chat endpoint)
        created_by = str(user_id) if user_id else str(uuid.uuid4())  # Placeholder if not provided
        
        params = (
            ticket_id,
            str(connection["id"]),
            issue_result["key"],
            issue_result["url"],
            summary,
            description,
            final_issue_type,
            priority or "Medium",
            assignee,
            pr_url,
            analysis_report_url,
            created_by,
        )
        
        with get_db_connection() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            # Normalize params
            norm_params = []
            for p in params:
                if isinstance(p, uuid.UUID):
                    norm_params.append(str(p))
                else:
                    norm_params.append(p)
            cur.execute(q, tuple(norm_params))
            row = cur.fetchone()
            conn.commit()
            
            if row:
                ticket = dict(row)
                # Convert datetime objects to ISO strings for JSON serialization
                ticket_serializable = {}
                for k, v in ticket.items():
                    if hasattr(v, 'isoformat'):  # datetime objects
                        ticket_serializable[k] = v.isoformat()
                    elif isinstance(v, uuid.UUID):
                        ticket_serializable[k] = str(v)
                    else:
                        ticket_serializable[k] = v
                
                logger.info(f"Saved Jira ticket to database: {ticket['ticket_key']}")
                return {
                    "success": True,
                    "ticket": ticket_serializable,
                    "jira_issue": issue_result,
                }
            else:
                logger.warning("Failed to save ticket to database")
                return {
                    "success": True,
                    "ticket": None,
                    "jira_issue": issue_result,
                    "warning": "Ticket created in Jira but not saved to database",
                }
    except Exception as e:
        logger.error(f"Failed to save ticket to database: {e}", exc_info=True)
        return {
            "success": True,
            "ticket": None,
            "jira_issue": issue_result,
            "warning": f"Ticket created in Jira but database save failed: {str(e)}",
        }


def build_org_jira_tool(org_id: str, user_id: Optional[str] = None) -> Tool:
    """
    Build a LangChain Tool for creating Jira tickets.
    The tool will parse natural language requests and create tickets.
    """
    def _fn(question: str) -> str:
        """
        Parse the question to extract ticket creation details and create a Jira ticket.
        Uses LLM to extract structured information from natural language.
        """
        import json
        
        # Use LLM to extract ticket information from the question
        extract_prompt = f"""Extract Jira ticket creation details from this request:

"{question}"

Extract the following information:
- summary: A clear, concise summary of the issue
- description: Detailed description of the issue
- issue_type: Type of issue (Task, Bug, Story, etc.) - default to "Task" if not specified
- priority: Priority level (Highest, High, Medium, Low, Lowest) - default to "Medium" if not specified
- assignee: Email address of assignee (if mentioned)
- pr_url: PR URL if mentioned
- analysis_report_url: Analysis report URL if mentioned

Respond with ONLY valid JSON in this format:
{{
  "summary": "...",
  "description": "...",
  "issue_type": "Task",
  "priority": "Medium",
  "assignee": null,
  "pr_url": null,
  "analysis_report_url": null
}}
"""
        
        try:
            llm_response = LLM.invoke(extract_prompt)
            response_text = getattr(llm_response, "content", str(llm_response))
            
            # Try to parse JSON from response
            ticket_data = None
            try:
                ticket_data = json.loads(response_text)
            except Exception:
                # Try to find JSON block
                start = response_text.find("{")
                end = response_text.rfind("}")
                if start != -1 and end != -1:
                    ticket_data = json.loads(response_text[start:end + 1])
            
            if not ticket_data:
                return f"Error: Could not parse ticket information from request. Please provide: summary, description, and optionally issue_type, priority, assignee, pr_url, analysis_report_url."
            
            # Create the ticket
            result = create_jira_ticket_for_org(
                org_id=org_id,
                summary=ticket_data.get("summary", ""),
                description=ticket_data.get("description", ""),
                issue_type=ticket_data.get("issue_type"),
                priority=ticket_data.get("priority"),
                assignee=ticket_data.get("assignee"),
                pr_url=ticket_data.get("pr_url"),
                analysis_report_url=ticket_data.get("analysis_report_url"),
                user_id=user_id,
            )
            
            if "error" in result:
                return f"Error: {result['error']}"
            
            # Format response
            parts = ["✅ Jira ticket created successfully!"]
            
            if "jira_issue" in result:
                issue = result["jira_issue"]
                parts.append(f"Ticket Key: {issue.get('key')}")
                parts.append(f"Ticket URL: {issue.get('url')}")
            
            if "ticket" in result and result["ticket"]:
                ticket = result["ticket"]
                parts.append(f"\nSummary: {ticket.get('summary')}")
                parts.append(f"Type: {ticket.get('issue_type')}")
                parts.append(f"Priority: {ticket.get('priority')}")
                if ticket.get("assignee"):
                    parts.append(f"Assignee: {ticket.get('assignee')}")
            
            if "warning" in result:
                parts.append(f"\n⚠️ Warning: {result['warning']}")
            
            # Add structured data for frontend (handle datetime serialization)
            def _json_serializer(obj):
                """JSON serializer for objects not serializable by default json code"""
                if hasattr(obj, 'isoformat'):  # datetime objects
                    return obj.isoformat()
                elif isinstance(obj, uuid.UUID):
                    return str(obj)
                raise TypeError(f"Type {type(obj)} not serializable")
            
            try:
                result_json = json.dumps(result, default=_json_serializer)[:2000]
                parts.append("\nDATA:\n" + result_json)
            except Exception as e:
                logger.warning(f"Failed to serialize result to JSON: {e}")
                # Fallback: convert datetime to string manually
                result_copy = {}
                for k, v in result.items():
                    if hasattr(v, 'isoformat'):
                        result_copy[k] = v.isoformat()
                    elif isinstance(v, dict):
                        result_copy[k] = {k2: (v2.isoformat() if hasattr(v2, 'isoformat') else v2) for k2, v2 in v.items()}
                    else:
                        result_copy[k] = v
                parts.append("\nDATA:\n" + json.dumps(result_copy, default=str)[:2000])
            
            return "\n".join(parts)
            
        except Exception as e:
            logger.error(f"Failed to create Jira ticket: {e}", exc_info=True)
            return f"Error: Failed to create Jira ticket: {str(e)}"
    
    return Tool(
        name="create_jira_ticket",
        func=_fn,
        description=(
            "Create a Jira ticket for tracking issues, bugs, or tasks. "
            "Input should include: summary, description, and optionally issue_type, priority, assignee (email), pr_url, analysis_report_url. "
            "Example: 'Create a Jira ticket: Fix issue for impacted tables. Description: Downstream table is broken. Assign to: user@example.com'"
        ),
    )


__all__ = [
    "get_active_jira_connection",
    "create_jira_ticket_for_org",
    "build_org_jira_tool",
]

