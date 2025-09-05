# GitHub App Installation and Management Endpoints
# GET /github/install → redirect to GitHub App installation
# GET /github/callback → handle GitHub App installation callback
# GET /github/installations → list installations for organization
# GET /github/repositories/{installation_id} → list repositories for installation
# POST /github/webhook → handle GitHub webhook events (PR events)
# POST /github/process-pr → process PR changes and add comment

from fastapi import APIRouter, HTTPException, Depends, Request, Response, status
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from typing import List, Optional
from pydantic import BaseModel
import json
import requests
from app.database import get_db
from app.utils.models import GitHubInstallation, GitHubRepository, Organization, User
from app.api.auth import get_current_user
import uuid
from uuid import UUID
from datetime import datetime

router = APIRouter(prefix="/github", tags=["GitHub"])

# --- Configuration ---
# TODO: Update these values for your GitHub App
GITHUB_APP_URL = "https://github.com/apps/queryguardai-poc"
CALLBACK_URL = "https://your-backend.com/github/callback"  # Update this
GITHUB_API_BASE = "https://api.github.com"
WEBHOOK_SECRET = "your-webhook-secret"  # Update this with your GitHub App webhook secret

# GitHub App permissions and events
GITHUB_PERMISSIONS = {
    "contents": "read",
    "pull_requests": "read", 
    "metadata": "read"
}

GITHUB_EVENTS = [
    "pull_request"
]


# --- Models ---
class InstallationResponse(BaseModel):
    id: UUID
    installation_id: str
    account_type: str
    account_login: str
    repository_selection: str
    permissions: str | None
    events: str | None
    is_active: bool
    created_at: datetime
    updated_at: datetime | None

    class Config:
        from_attributes = True

class RepositoryResponse(BaseModel):
    id: UUID
    repo_id: str
    repo_name: str
    full_name: str
    private: bool
    description: str | None
    default_branch: str | None
    created_at: datetime
    updated_at: datetime | None

    class Config:
        from_attributes = True

class PRProcessRequest(BaseModel):
    installation_id: str
    repo_full_name: str
    pr_number: int
    pr_title: str
    pr_body: str | None = None


# --- Helpers ---
def get_github_installation_info(installation_id: str, access_token: str) -> dict:
    """Get installation information from GitHub API"""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/vnd.github.v3+json"
    }
    
    response = requests.get(f"{GITHUB_API_BASE}/app/installations/{installation_id}", headers=headers)
    if response.status_code != 200:
        raise HTTPException(status_code=400, detail=f"Failed to get installation info: {response.text}")
    
    return response.json()

def get_github_repositories(installation_id: str, access_token: str) -> List[dict]:
    """Get repositories for an installation from GitHub API"""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/vnd.github.v3+json"
    }
    
    response = requests.get(f"{GITHUB_API_BASE}/installation/repositories", headers=headers)
    if response.status_code != 200:
        raise HTTPException(status_code=400, detail=f"Failed to get repositories: {response.text}")
    
    data = response.json()
    return data.get("repositories", [])

def get_installation_access_token(installation_id: str, jwt_token: str) -> str:
    """Get installation access token using JWT"""
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Accept": "application/vnd.github.v3+json"
    }
    
    response = requests.post(f"{GITHUB_API_BASE}/app/installations/{installation_id}/access_tokens", headers=headers)
    if response.status_code != 201:
        raise HTTPException(status_code=400, detail=f"Failed to get access token: {response.text}")
    
    data = response.json()
    return data.get("token")

def verify_webhook_signature(payload: bytes, signature: str) -> bool:
    """Verify GitHub webhook signature"""
    import hmac
    import hashlib
    
    if not signature.startswith("sha256="):
        return False
    
    expected_signature = signature[7:]  # Remove "sha256=" prefix
    calculated_signature = hmac.new(
        WEBHOOK_SECRET.encode(),
        payload,
        hashlib.sha256
    ).hexdigest()
    
    return hmac.compare_digest(expected_signature, calculated_signature)

def add_comment_to_pr(access_token: str, repo_full_name: str, pr_number: int, comment: str) -> bool:
    """Add a comment to a pull request"""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/vnd.github.v3+json"
    }
    
    data = {"body": comment}
    response = requests.post(
        f"{GITHUB_API_BASE}/repos/{repo_full_name}/issues/{pr_number}/comments",
        headers=headers,
        json=data
    )
    
    return response.status_code == 201


# --- Endpoints ---
@router.get("/install")
def github_install(org_id: str, request: Request):
    """Redirect to GitHub App installation with org_id as state"""
    try:
        # Validate org_id format
        uuid.UUID(org_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid organization ID format")
    
    # Build GitHub App installation URL with state parameter
    install_url = f"{GITHUB_APP_URL}/installations/new?state={org_id}"
    
    return RedirectResponse(url=install_url)


@router.get("/callback")
def github_callback(
    installation_id: str,
    setup_action: str,
    state: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """Handle GitHub App installation callback"""
    
    # If no state parameter, ignore the installation (not from our flow)
    if not state:
        return {"message": "Installation ignored - no state parameter"}
    
    try:
        # Validate state (org_id) format
        org_uuid = uuid.UUID(state)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid state parameter")
    
    # Check if organization exists
    organization = db.query(Organization).filter(
        Organization.id == org_uuid,
        Organization.is_active == True
    ).first()
    
    if not organization:
        raise HTTPException(status_code=404, detail="Organization not found")
    
    # Check if installation already exists
    existing_installation = db.query(GitHubInstallation).filter(
        GitHubInstallation.installation_id == installation_id
    ).first()
    
    if existing_installation:
        raise HTTPException(status_code=400, detail="Installation already exists")
    
    try:
        # TODO: You'll need to implement JWT token generation for your GitHub App
        # For now, we'll store basic installation info
        # In production, you'd use the GitHub API to get full installation details
        
        new_installation = GitHubInstallation(
            installation_id=installation_id,
            org_id=org_uuid,
            account_type="Organization",  # Will be updated with actual data
            account_login="",  # Will be updated with actual data
            repository_selection="all",  # Will be updated with actual data
            permissions=json.dumps(GITHUB_PERMISSIONS),
            events=json.dumps(GITHUB_EVENTS)
        )
        
        db.add(new_installation)
        db.commit()
        db.refresh(new_installation)
        
        return new_installation
        
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=400, detail="Failed to save installation")


@router.get("/installations", response_model=List[InstallationResponse])
def list_installations(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    """List all GitHub installations for the organization"""
    installations = db.query(GitHubInstallation).filter(
        GitHubInstallation.org_id == current_user.org_id,
        GitHubInstallation.is_active == True
    ).all()
    
    return installations


@router.get("/repositories/{installation_id}", response_model=List[RepositoryResponse])
def list_repositories(installation_id: str, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    """List repositories for a specific installation"""
    try:
        inst_uuid = uuid.UUID(installation_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid installation ID format")
    
    # Verify installation belongs to user's org
    installation = db.query(GitHubInstallation).filter(
        GitHubInstallation.id == inst_uuid,
        GitHubInstallation.org_id == current_user.org_id,
        GitHubInstallation.is_active == True
    ).first()
    
    if not installation:
        raise HTTPException(status_code=404, detail="GitHub installation not found")
    
    repositories = db.query(GitHubRepository).filter(
        GitHubRepository.installation_id == inst_uuid
    ).all()
    
    return repositories


@router.post("/sync-repositories/{installation_id}")
def sync_repositories(installation_id: str, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Sync repositories for an installation (manual trigger)"""
    try:
        inst_uuid = uuid.UUID(installation_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid installation ID format")
    
    # Verify installation belongs to user's org
    installation = db.query(GitHubInstallation).filter(
        GitHubInstallation.id == inst_uuid,
        GitHubInstallation.org_id == current_user.org_id,
        GitHubInstallation.is_active == True
    ).first()
    
    if not installation:
        raise HTTPException(status_code=404, detail="GitHub installation not found")
    
    try:
        # TODO: Implement actual GitHub API calls here
        # This would require your GitHub App's JWT token and access token
        
        # For now, return a placeholder response
        return {
            "message": "Repository sync initiated",
            "installation_id": installation_id,
            "note": "GitHub API integration needs to be implemented with your App's credentials"
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to sync repositories: {str(e)}")


@router.delete("/installations/{installation_id}")
def deactivate_installation(installation_id: str, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Deactivate a GitHub installation (soft delete)"""
    try:
        inst_uuid = uuid.UUID(installation_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid installation ID format")
    
    # Verify installation belongs to user's org
    installation = db.query(GitHubInstallation).filter(
        GitHubInstallation.id == inst_uuid,
        GitHubInstallation.org_id == current_user.org_id,
        GitHubInstallation.is_active == True
    ).first()
    
    if not installation:
        raise HTTPException(status_code=404, detail="GitHub installation not found")
    
    installation.is_active = False
    db.commit()
    
    return {"message": "GitHub installation deactivated successfully"}


@router.post("/webhook")
async def github_webhook(request: Request):
    """Handle GitHub webhook events (PR events)"""
    # Get the raw body for signature verification
    body = await request.body()
    
    # Get GitHub signature header
    signature = request.headers.get("X-Hub-Signature-256")
    if not signature:
        raise HTTPException(status_code=401, detail="Missing signature")
    
    # Verify webhook signature
    if not verify_webhook_signature(body, signature):
        raise HTTPException(status_code=401, detail="Invalid signature")
    
    # Get event type
    event_type = request.headers.get("X-GitHub-Event")
    if event_type != "pull_request":
        return {"message": f"Ignoring {event_type} event"}
    
    # Parse the webhook payload
    try:
        payload = json.loads(body.decode())
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")
    
    # Extract PR information
    action = payload.get("action")
    if action not in ["opened", "reopened"]:
        return {"message": f"Ignoring PR {action} action"}
    
    pr_data = payload.get("pull_request", {})
    repo_data = payload.get("repository", {})
    installation_data = payload.get("installation", {})
    
    # Extract relevant information
    pr_number = pr_data.get("number")
    pr_title = pr_data.get("title")
    pr_body = pr_data.get("body")
    repo_full_name = repo_data.get("full_name")
    installation_id = str(installation_data.get("id"))
    
    # TODO: Store PR data in database for processing
    # TODO: Trigger downstream analysis based on PR changes
    
    return {
        "message": "PR webhook processed",
        "pr_number": pr_number,
        "repo": repo_full_name,
        "installation_id": installation_id,
        "action": action
    }


@router.post("/process-pr")
def process_pr_changes(pr_request: PRProcessRequest, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Process PR changes and add comment to PR"""
    try:
        inst_uuid = uuid.UUID(pr_request.installation_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid installation ID format")
    
    # Verify installation belongs to user's org
    installation = db.query(GitHubInstallation).filter(
        GitHubInstallation.id == inst_uuid,
        GitHubInstallation.org_id == current_user.org_id,
        GitHubInstallation.is_active == True
    ).first()
    
    if not installation:
        raise HTTPException(status_code=404, detail="GitHub installation not found")
    
    try:
        # TODO: Implement JWT token generation for your GitHub App
        # For now, we'll use a placeholder access token
        # In production, you'd generate a JWT token and get an access token
        
        # Placeholder for access token (replace with actual implementation)
        access_token = "placeholder_access_token"
        
        # Add comment to PR
        comment = "Changes Processed By Query Guard AI"
        success = add_comment_to_pr(
            access_token=access_token,
            repo_full_name=pr_request.repo_full_name,
            pr_number=pr_request.pr_number,
            comment=comment
        )
        
        if success:
            return {
                "message": "PR processed successfully",
                "comment_added": True,
                "pr_number": pr_request.pr_number,
                "repo": pr_request.repo_full_name
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to add comment to PR")
            
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to process PR: {str(e)}")
