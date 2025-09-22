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
import os
import logging
from urllib.parse import unquote
from app.services.impact_analysis import schema_detection_rag, dbt_model_detection_rag, fetch_queries, store_analysis_result
from github import GithubIntegration, Github
from sqlalchemy import and_

router = APIRouter(prefix="/github", tags=["GitHub"])

# --- Configuration ---
# TODO: Update these values for your GitHub App
GITHUB_APP_URL = "https://github.com/apps/queryguardai-poc"
CALLBACK_URL = "https://queryguard-backend-dev.onrender.com/github/callback"
GITHUB_API_BASE = "https://api.github.com"
WEBHOOK_SECRET = os.getenv("GITHUB_WEBHOOK_SECRET", "")
GITHUB_APP_ID = os.getenv("GITHUB_APP_ID")
PRIVATE_KEY = os.getenv("GITHUB_PRIVATE_KEY")

logger = logging.getLogger("github")
logging.basicConfig(level=logging.INFO)

# GitHub App permissions and events
GITHUB_PERMISSIONS = {
    "contents": "read",
    "pull_requests": "read", 
    "metadata": "read"
}

GITHUB_EVENTS = [
    "pull_request"
]
git_integration: Optional[GithubIntegration] = None
if GITHUB_APP_ID and PRIVATE_KEY:
    try:
        git_integration = GithubIntegration(int(GITHUB_APP_ID), PRIVATE_KEY)
    except Exception:
        git_integration = None



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
        logger.warning("/github/install - invalid org_id: %s", org_id)
        raise HTTPException(status_code=400, detail="Invalid organization ID format")
    
    # Build GitHub App installation URL with state parameter
    install_url = f"{GITHUB_APP_URL}/installations/new?state={org_id}"
    # https://github.com/apps/queryguardai-poc/installations/new?state=/76d33fb3-6062-456b-a211-4aec9971f8be
    logger.info("/github/install - redirecting to %s", install_url)
    return RedirectResponse(url=install_url)


@router.get("/callback")
def github_callback(
    installation_id: str,
    setup_action: Optional[str] = None,
    state: Optional[str] = None,
    code: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """Handle GitHub App installation callback"""
    logger.info("GitHub callback received installation_id=%s setup_action=%s state=%s", installation_id, setup_action, state)

    # If no state parameter, ignore the installation (not from our flow)
    if not state:
        return {"message": "Installation ignored - no state parameter"}
    
    try:
        # Decode and normalize state; GitHub may send it URL-encoded and with a leading '/'
        normalized_state = unquote(state)
        if normalized_state.startswith('/'):
            normalized_state = normalized_state[1:]
        # Validate state (org_id) format
        org_uuid = uuid.UUID(normalized_state)
    except ValueError:
        logger.warning("/github/callback - invalid state format: %s", state)
        raise HTTPException(status_code=400, detail="Invalid state parameter")
    
    # Check if organization exists
    organization = db.query(Organization).filter(
        Organization.id == org_uuid,
        Organization.is_active == True
    ).first()
    
    if not organization:
        logger.warning("/github/callback - organization not found: %s", org_uuid)
        raise HTTPException(status_code=404, detail="Organization not found")
    
    # Check if installation already exists
    existing_installation = db.query(GitHubInstallation).filter(
        GitHubInstallation.installation_id == installation_id
    ).first()
    
    if existing_installation:
        logger.info("/github/callback - installation already exists: %s", installation_id)
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
        logger.info("/github/callback - installation saved id=%s org_id=%s", new_installation.id, org_uuid)

        # Best-effort: immediately sync repositories for this installation
        try:
            if git_integration:
                access_token = git_integration.get_access_token(int(installation_id)).token
                headers = {
                    "Authorization": f"Bearer {access_token}",
                    "Accept": "application/vnd.github.v3+json",
                }
                repos_resp = requests.get(f"{GITHUB_API_BASE}/installation/repositories", headers=headers)
                if repos_resp.status_code == 200:
                    repos_data = repos_resp.json().get("repositories", [])

                    existing_repos = db.query(GitHubRepository).filter(
                        GitHubRepository.installation_id == new_installation.id
                    ).all()
                    existing_by_repo_id = {r.repo_id: r for r in existing_repos}

                    seen_repo_ids: set[str] = set()
                    for r in repos_data:
                        repo_id = str(r.get("id"))
                        seen_repo_ids.add(repo_id)
                        repo_obj = existing_by_repo_id.get(repo_id)
                        if not repo_obj:
                            repo_obj = GitHubRepository(
                                installation_id=new_installation.id,
                                repo_id=repo_id,
                                repo_name=r.get("name") or "",
                                full_name=r.get("full_name") or "",
                                private=bool(r.get("private")),
                                description=r.get("description"),
                                default_branch=r.get("default_branch"),
                            )
                            db.add(repo_obj)
                        else:
                            repo_obj.repo_name = r.get("name") or repo_obj.repo_name
                            repo_obj.full_name = r.get("full_name") or repo_obj.full_name
                            repo_obj.private = bool(r.get("private"))
                            repo_obj.description = r.get("description")
                            repo_obj.default_branch = r.get("default_branch")

                    db.commit()
                else:
                    logger.warning(
                        "/github/callback - failed to fetch repositories for installation %s: %s",
                        installation_id,
                        repos_resp.text,
                    )
            else:
                logger.info("/github/callback - GitHub Integration not configured; skipping repo sync")
        except Exception:
            logger.exception("/github/callback - error while syncing repositories for installation %s", installation_id)
        
        # Optional: redirect to your frontend success page if CALLBACK_URL is set
        if CALLBACK_URL:
            # Avoid redirecting back to the same callback endpoint to prevent a second hit
            if CALLBACK_URL.rstrip("/").endswith("/github/callback"):
                return {
                    "message": "GitHub installation saved",
                    "org_id": str(org_uuid),
                    "installation_id": installation_id,
                    "status": "success",
                }
            # Append org_id and installation_id for UI to consume
            redirect_url = f"{CALLBACK_URL}?org_id={org_uuid}&installation_id={installation_id}&status=success"
            return RedirectResponse(url=str(redirect_url))

        return new_installation
        
    except IntegrityError:
        db.rollback()
        logger.exception("/github/callback - failed to save installation")
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
    
    if not git_integration:
        raise HTTPException(status_code=400, detail="GitHub Integration not configured")

    try:
        access_token = git_integration.get_access_token(int(installation.installation_id)).token
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/vnd.github.v3+json",
        }

        response = requests.get(f"{GITHUB_API_BASE}/installation/repositories", headers=headers)
        if response.status_code != 200:
            raise HTTPException(status_code=400, detail=f"Failed to get repositories: {response.text}")

        repos = response.json().get("repositories", [])

        existing_repos = db.query(GitHubRepository).filter(
            GitHubRepository.installation_id == installation.id
        ).all()
        existing_by_repo_id = {r.repo_id: r for r in existing_repos}

        seen_repo_ids: set[str] = set()
        created_count = 0
        updated_count = 0

        for r in repos:
            repo_id = str(r.get("id"))
            seen_repo_ids.add(repo_id)
            repo_obj = existing_by_repo_id.get(repo_id)
            if not repo_obj:
                repo_obj = GitHubRepository(
                    installation_id=installation.id,
                    repo_id=repo_id,
                    repo_name=r.get("name") or "",
                    full_name=r.get("full_name") or "",
                    private=bool(r.get("private")),
                    description=r.get("description"),
                    default_branch=r.get("default_branch"),
                )
                db.add(repo_obj)
                created_count += 1
            else:
                repo_obj.repo_name = r.get("name") or repo_obj.repo_name
                repo_obj.full_name = r.get("full_name") or repo_obj.full_name
                repo_obj.private = bool(r.get("private"))
                repo_obj.description = r.get("description")
                repo_obj.default_branch = r.get("default_branch")
                updated_count += 1

        db.commit()

        return {
            "message": "Repository sync completed",
            "installation_id": installation_id,
            "created": created_count,
            "updated": updated_count,
            "total": len(repos),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("/github/sync-repositories - error: %s", str(e))
        raise HTTPException(status_code=500, detail="Failed to sync repositories")


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
async def github_webhook(request: Request, db=Depends(get_db)):
    """Handle GitHub webhook events (PR events)"""
    # Get the raw body for signature verification
    body = await request.body()
    
    # Get GitHub signature header
    signature = request.headers.get("X-Hub-Signature-256")
    if not signature:
        logger.warning("/github/webhook - missing signature header")
        raise HTTPException(status_code=401, detail="Missing signature")
    
    # Verify webhook signature
    if not verify_webhook_signature(body, signature):
        logger.warning("/github/webhook - invalid signature")
        raise HTTPException(status_code=401, detail="Invalid signature")
    
    # Extract headers and parse payload early so we can log for any event
    event_type = request.headers.get("X-GitHub-Event")
    delivery_id = request.headers.get("X-GitHub-Delivery")

    # Parse the webhook payload
    try:
        payload = json.loads(body.decode())
    except json.JSONDecodeError:
        logger.warning("/github/webhook - invalid JSON payload")
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    # Log compact info for observability
    try:
        payload_preview = str(payload)[:2000]
        logger.info(
            "/github/webhook - delivery=%s event=%s payload_preview=%s",
            delivery_id,
            event_type,
            payload_preview,
        )
    except Exception:
        logger.debug("/github/webhook - failed to log payload preview")

    # If not interested in this event type, return early
    if event_type != "pull_request":
        logger.info("/github/webhook - ignoring event %s", event_type)
        return {"message": f"Ignoring {event_type} event"}
    
    # Extract PR information
    action = payload.get("action")
    if action not in ["opened", "reopened", "synchronize"]:
        logger.info("/github/webhook - ignoring PR action %s", action)
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

    # Validate installation id exists and is active in our DB
    if not installation_id:
        logger.info("/github/webhook - missing installation id in payload")
        return {"message": "Missing installation id"}

    installation = db.query(GitHubInstallation).filter(
        GitHubInstallation.installation_id == installation_id,
        GitHubInstallation.is_active == True
    ).first()

    if not installation:
        logger.info("/github/webhook - installation id %s not registered/active, ignoring", installation_id)
        return {"message": "Installation not recognized or inactive"}
    
    # Use GitHub App installation token and PyGithub to fetch files and post comment
    if not git_integration:
        logger.warning("/github/webhook - GitHub Integration not configured")
        return {"message": "GitHub Integration not configured"}

    try:
        access_token = git_integration.get_access_token(int(installation_id)).token
        gh = Github(login_or_token=access_token)
        repo = gh.get_repo(repo_full_name)
        pr = repo.get_pull(pr_number)

        # Collect relevant SQL file changes
        code_changes = []
        for file in pr.get_files():
            if getattr(file, "patch", None) and file.filename.lower().endswith(".sql"):
                code_changes.append(
                    {
                        "filename": file.filename,
                        "status": file.status,
                        "patch": file.patch,
                        "additions": file.additions,
                        "deletions": file.deletions,
                    }
                )

        if not code_changes:
            logger.info("/github/webhook - no relevant SQL changes found")
            return {"message": "No relevant SQL changes found"}

        # Analyze each SQL change
        results = []
        for c in code_changes:
            full_diff = (
                f"File: {c['filename']} ({c['status']}) [+{c['additions']}/-{c['deletions']}]\n{c['patch']}"
            )
            if c["filename"].endswith(".sql") and "models/" in c["filename"]:
                analysis_result = dbt_model_detection_rag(full_diff, c["filename"], str(installation.org_id))  # DBT model path
            else:
                analysis_result = schema_detection_rag(full_diff, str(installation.org_id))

            regression_queries = fetch_queries(analysis_result.get("affected_query_ids", []))

            results.append(
                {
                    "sql_change": full_diff,
                    "impact_analysis": analysis_result.get("impact_report", ""),
                    "affected_query_ids": analysis_result.get("affected_query_ids", []),
                    "regression_queries": regression_queries,
                    "source_metadata": analysis_result.get("source_metadata", []),
                }
            )

        # Compose comment
        file_summaries = []
        for idx, r in enumerate(results, start=1):
            file_info = code_changes[idx - 1]
            file_summaries.append(
                f"""
<details>
<summary>📂 **{file_info['filename']}** ({file_info['status']}) [+{file_info['additions']}/-{file_info['deletions']}]
</summary>

**Impact Report:**
{r['impact_analysis']}

**Affected Query IDs:** {', '.join(r['affected_query_ids']) if r['affected_query_ids'] else 'None'}

</details>
"""
            )

        comment_text = f"## 🤖 **Impact Analysis Summary**\n\nAnalyzed {len(results)} SQL file(s) for potential downstream impact.\n\n*Analysis generated at {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC*\n\n---\n\n{chr(10).join(file_summaries)}"

        # Store results
        analysis_id = store_analysis_result(pr_number, repo_full_name, {"files": results})

        issue = repo.get_issue(number=pr_number)
        issue.create_comment(comment_text)

        logger.info("/github/webhook - posted analysis comment to PR #%s", pr_number)

        return {
            "message": "PR webhook processed",
            "pr_number": pr_number,
            "repo": repo_full_name,
            "installation_id": installation_id,
            "action": action,
            "files_analyzed": len(results),
            "analysis_id": analysis_id,
        }

    except Exception as e:
        logger.exception("/github/webhook - analysis failed: %s", str(e))
        raise HTTPException(status_code=500, detail="Webhook processing failed")


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
