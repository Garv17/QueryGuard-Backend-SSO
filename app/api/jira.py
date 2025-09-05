# POST /jira/test-connection → test connection
# POST /jira/save-connection → save connection
# GET /jira/connections → list connections
# POST /jira/create-ticket → create jira ticket
# GET /jira/tickets → list tickets for organization
# GET /jira/projects/{connection_id} → get available projects
# GET /jira/issue-types/{connection_id}/{project_key} → get issue types for project

from fastapi import APIRouter, HTTPException, Depends, status
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from typing import List, Optional
from pydantic import BaseModel, HttpUrl
import requests
from requests.auth import HTTPBasicAuth
import json
import uuid
from uuid import UUID
from datetime import datetime
from app.database import get_db
from app.utils.models import JiraConnection, JiraTicket, User
from app.api.auth import get_current_user

router = APIRouter(prefix="/jira", tags=["Jira"])


# --- Helpers ---
def test_jira_connection(server_url: str, username: str, api_token: str) -> bool:
    """Test Jira connection by making a simple API call"""
    try:
        # Remove trailing slash if present
        server_url = server_url.rstrip('/')
        
        # Test connection by getting current user info
        auth = HTTPBasicAuth(username, api_token)
        headers = {"Accept": "application/json"}
        
        response = requests.get(
            f"{server_url}/rest/api/2/myself",
            auth=auth,
            headers=headers,
            timeout=10
        )
        
        if response.status_code == 200:
            return True
        else:
            raise HTTPException(
                status_code=400, 
                detail=f"Connection failed: {response.status_code} - {response.text}"
            )
    except requests.exceptions.RequestException as e:
        raise HTTPException(
            status_code=400, 
            detail=f"Connection failed: {str(e)}"
        )

def get_jira_projects(server_url: str, username: str, api_token: str) -> List[dict]:
    """Get list of projects from Jira"""
    try:
        server_url = server_url.rstrip('/')
        auth = HTTPBasicAuth(username, api_token)
        headers = {"Accept": "application/json"}
        
        response = requests.get(
            f"{server_url}/rest/api/2/project",
            auth=auth,
            headers=headers,
            timeout=10
        )
        
        if response.status_code == 200:
            projects = response.json()
            return [{"key": p["key"], "name": p["name"], "id": p["id"]} for p in projects]
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Failed to get projects: {response.status_code} - {response.text}"
            )
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=400, detail=f"Failed to get projects: {str(e)}")

def get_jira_issue_types(server_url: str, username: str, api_token: str, project_key: str) -> List[dict]:
    """Get issue types for a specific project"""
    try:
        server_url = server_url.rstrip('/')
        auth = HTTPBasicAuth(username, api_token)
        headers = {"Accept": "application/json"}
        
        response = requests.get(
            f"{server_url}/rest/api/2/project/{project_key}",
            auth=auth,
            headers=headers,
            timeout=10
        )
        
        if response.status_code == 200:
            project_data = response.json()
            issue_types = project_data.get("issueTypes", [])
            return [{"id": it["id"], "name": it["name"], "description": it.get("description", "")} for it in issue_types]
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Failed to get issue types: {response.status_code} - {response.text}"
            )
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=400, detail=f"Failed to get issue types: {str(e)}")

def create_jira_issue(
    server_url: str, 
    username: str, 
    api_token: str, 
    project_key: str,
    summary: str,
    description: str,
    issue_type: str,
    priority: Optional[str] = None,
    assignee: Optional[str] = None
) -> dict:
    """Create a Jira issue"""
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
            return {
                "key": issue_response["key"],
                "id": issue_response["id"],
                "url": f"{server_url}/browse/{issue_response['key']}"
            }
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Failed to create issue: {response.status_code} - {response.text}"
            )
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=400, detail=f"Failed to create issue: {str(e)}")


# --- Models ---
class JiraConnectionRequest(BaseModel):
    connection_name: str
    server_url: HttpUrl
    username: str  # Email for Atlassian Cloud
    api_token: str
    project_key: str
    issue_type: str = "Task"

class JiraConnectionResponse(BaseModel):
    id: UUID
    connection_name: str
    server_url: str
    username: str
    project_key: str
    issue_type: str
    is_active: bool
    created_at: datetime

    class Config:
        from_attributes = True

class CreateTicketRequest(BaseModel):
    connection_id: str
    summary: str
    description: str
    issue_type: Optional[str] = None
    priority: Optional[str] = None
    assignee: Optional[str] = None
    pr_url: Optional[str] = None
    analysis_report_url: Optional[str] = None

class TicketResponse(BaseModel):
    id: UUID
    ticket_key: str
    ticket_url: str
    summary: str
    description: str
    issue_type: str
    status: str
    priority: str
    assignee: str | None = None
    pr_url: str | None = None
    analysis_report_url: str | None = None
    created_at: datetime

    class Config:
        from_attributes = True

class ProjectResponse(BaseModel):
    key: str
    name: str
    id: str

class IssueTypeResponse(BaseModel):
    id: str
    name: str
    description: str


# --- Endpoints ---
@router.post("/test-connection")
def test_connection(
    conn: JiraConnectionRequest, 
    current_user: User = Depends(get_current_user)
):
    """Test Jira connection before saving"""
    success = test_jira_connection(
        server_url=str(conn.server_url),
        username=conn.username,
        api_token=conn.api_token
    )
    
    if success:
        return {"message": "Connection successful"}


@router.post("/save-connection", response_model=JiraConnectionResponse, status_code=status.HTTP_201_CREATED)
def save_connection(
    conn: JiraConnectionRequest, 
    current_user: User = Depends(get_current_user), 
    db: Session = Depends(get_db)
):
    """Save Jira connection after successful test"""
    # Test connection before saving
    test_jira_connection(str(conn.server_url), conn.username, conn.api_token)

    # Check if connection name already exists for this org
    existing_conn = db.query(JiraConnection).filter(
        JiraConnection.org_id == current_user.org_id,
        JiraConnection.connection_name == conn.connection_name,
        JiraConnection.is_active == True
    ).first()
    
    if existing_conn:
        raise HTTPException(
            status_code=400, 
            detail="Connection name already exists for this organization"
        )

    # Check if only one active connection per org (business rule)
    active_conn = db.query(JiraConnection).filter(
        JiraConnection.org_id == current_user.org_id,
        JiraConnection.is_active == True
    ).first()
    
    if active_conn:
        raise HTTPException(
            status_code=400,
            detail="Only one active Jira connection allowed per organization"
        )

    new_connection = JiraConnection(
        org_id=current_user.org_id,
        connection_name=conn.connection_name,
        server_url=str(conn.server_url),
        username=conn.username,
        api_token=conn.api_token,
        project_key=conn.project_key,
        issue_type=conn.issue_type
    )
    
    try:
        db.add(new_connection)
        db.commit()
        db.refresh(new_connection)
        return new_connection
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=400, detail="Failed to save connection")


@router.get("/connections", response_model=List[JiraConnectionResponse])
def list_connections(
    current_user: User = Depends(get_current_user), 
    db: Session = Depends(get_db)
):
    """List all Jira connections for the organization"""
    connections = db.query(JiraConnection).filter(
        JiraConnection.org_id == current_user.org_id,
        JiraConnection.is_active == True
    ).all()
    return connections


@router.get("/projects/{connection_id}", response_model=List[ProjectResponse])
def get_projects(
    connection_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get available projects for a Jira connection"""
    try:
        conn_uuid = uuid.UUID(connection_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid connection ID format")

    # Get connection details
    connection = db.query(JiraConnection).filter(
        JiraConnection.id == conn_uuid,
        JiraConnection.org_id == current_user.org_id,
        JiraConnection.is_active == True
    ).first()
    
    if not connection:
        raise HTTPException(status_code=404, detail="Jira connection not found")

    projects = get_jira_projects(
        server_url=connection.server_url,
        username=connection.username,
        api_token=connection.api_token
    )
    
    return projects


@router.get("/issue-types/{connection_id}/{project_key}", response_model=List[IssueTypeResponse])
def get_issue_types(
    connection_id: str,
    project_key: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get available issue types for a project"""
    try:
        conn_uuid = uuid.UUID(connection_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid connection ID format")

    # Get connection details
    connection = db.query(JiraConnection).filter(
        JiraConnection.id == conn_uuid,
        JiraConnection.org_id == current_user.org_id,
        JiraConnection.is_active == True
    ).first()
    
    if not connection:
        raise HTTPException(status_code=404, detail="Jira connection not found")

    issue_types = get_jira_issue_types(
        server_url=connection.server_url,
        username=connection.username,
        api_token=connection.api_token,
        project_key=project_key
    )
    
    return issue_types


@router.post("/create-ticket", response_model=TicketResponse, status_code=status.HTTP_201_CREATED)
def create_ticket(
    ticket_request: CreateTicketRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Create a Jira ticket"""
    try:
        conn_uuid = uuid.UUID(ticket_request.connection_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid connection ID format")

    # Get connection details
    connection = db.query(JiraConnection).filter(
        JiraConnection.id == conn_uuid,
        JiraConnection.org_id == current_user.org_id,
        JiraConnection.is_active == True
    ).first()
    
    if not connection:
        raise HTTPException(status_code=404, detail="Jira connection not found")

    # Use connection defaults if not provided
    issue_type = ticket_request.issue_type or connection.issue_type
    
    # Create the Jira issue
    issue_result = create_jira_issue(
        server_url=connection.server_url,
        username=connection.username,
        api_token=connection.api_token,
        project_key=connection.project_key,
        summary=ticket_request.summary,
        description=ticket_request.description,
        issue_type=issue_type,
        priority=ticket_request.priority,
        assignee=ticket_request.assignee
    )
    
    # Save ticket details to database
    new_ticket = JiraTicket(
        connection_id=conn_uuid,
        ticket_key=issue_result["key"],
        ticket_url=issue_result["url"],
        summary=ticket_request.summary,
        description=ticket_request.description,
        issue_type=issue_type,
        status="Open",  # Default status
        priority=ticket_request.priority or "Medium",
        assignee=ticket_request.assignee,
        pr_url=ticket_request.pr_url,
        analysis_report_url=ticket_request.analysis_report_url,
        created_by=current_user.id
    )
    
    try:
        db.add(new_ticket)
        db.commit()
        db.refresh(new_ticket)
        return new_ticket
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=400, detail="Failed to save ticket details")


@router.get("/tickets", response_model=List[TicketResponse])
def list_tickets(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """List all Jira tickets for the organization"""
    tickets = db.query(JiraTicket).join(JiraConnection).filter(
        JiraConnection.org_id == current_user.org_id,
        JiraConnection.is_active == True
    ).all()
    
    return tickets


@router.delete("/connections/{connection_id}")
def deactivate_connection(
    connection_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Deactivate a Jira connection (soft delete)"""
    try:
        conn_uuid = uuid.UUID(connection_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid connection ID format")

    connection = db.query(JiraConnection).filter(
        JiraConnection.id == conn_uuid,
        JiraConnection.org_id == current_user.org_id,
        JiraConnection.is_active == True
    ).first()
    
    if not connection:
        raise HTTPException(status_code=404, detail="Jira connection not found")

    connection.is_active = False
    
    try:
        db.commit()
        return {"message": "Jira connection deactivated successfully"}
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=400, detail="Failed to deactivate connection")