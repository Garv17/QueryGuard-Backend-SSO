import os
import json
import hmac
import hashlib
import time
import uuid
from typing import Optional

import jwt  # pyjwt
import requests
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session

from app.database import get_db
from app.utils.models import GitHubInstallation, GitHubRepository, Organization

router = APIRouter(prefix="/github", tags=["GitHub"])

# --- Config from environment ---
GITHUB_APP_ID = os.getenv("GITHUB_APP_ID")
GITHUB_PRIVATE_KEY = os.getenv("GITHUB_PRIVATE_KEY")  # load PEM as string
GITHUB_APP_URL = "https://github.com/apps/queryguardai-poc"
GITHUB_API_BASE = "https://api.github.com"
WEBHOOK_SECRET = os.getenv("GITHUB_WEBHOOK_SECRET", "your-webhook-secret")


# --- JWT + Token helpers ---
def generate_jwt() -> str:
    """Generate a JWT for GitHub App authentication"""
    now = int(time.time())
    payload = {
        "iat": now - 60,         # issued 60s ago
        "exp": now + (10 * 60),  # valid 10 minutes
        "iss": GITHUB_APP_ID,
    }
    return jwt.encode(payload, GITHUB_PRIVATE_KEY, algorithm="RS256")


def get_installation_token(installation_id: str) -> str:
    """Exchange JWT for an installation access token"""
    jwt_token = generate_jwt()
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Accept": "application/vnd.github.v3+json",
    }
    url = f"{GITHUB_API_BASE}/app/installations/{installation_id}/access_tokens"
    r = requests.post(url, headers=headers)
    if r.status_code != 201:
        raise HTTPException(status_code=400, detail=f"Failed to get token: {r.text}")
    return r.json()["token"]


def verify_webhook_signature(payload: bytes, signature: str) -> bool:
    """Verify webhook signature"""
    if not signature or not signature.startswith("sha256="):
        return False
    expected = signature.split("=")[1]
    actual = hmac.new(WEBHOOK_SECRET.encode(), payload, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, actual)


# --- Endpoints ---
@router.get("/install")
def github_install(org_id: str):
    """Redirect to GitHub App installation page with org_id in state"""
    try:
        uuid.UUID(org_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid org_id")
    url = f"{GITHUB_APP_URL}/installations/new?state={org_id}"
    return RedirectResponse(url=url)


@router.get("/callback")
def github_callback(
    installation_id: str,
    setup_action: str,
    state: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """Handle GitHub App installation callback"""
    if not state:
        return {"message": "Installation ignored - no state param"}

    # Validate state format
    try:
        org_uuid = uuid.UUID(state)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid state parameter")

    # 🔒 Validate org exists in DB
    org = db.query(Organization).filter(
        Organization.id == org_uuid,
        Organization.is_active == True
    ).first()
    if not org:
        raise HTTPException(status_code=403, detail="Organization not found or inactive")

    # Exchange JWT for installation token
    token = get_installation_token(installation_id)

    # Fetch installation details
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github.v3+json"}
    r = requests.get(f"{GITHUB_API_BASE}/app/installations/{installation_id}", headers=headers)
    if r.status_code != 200:
        raise HTTPException(status_code=400, detail=f"Failed to fetch installation: {r.text}")
    inst_data = r.json()

    account = inst_data["account"]
    repo_selection = inst_data.get("repository_selection", "all")

    # Save installation in DB
    installation = GitHubInstallation(
        installation_id=installation_id,
        org_id=org_uuid,
        account_type=account["type"],
        account_login=account["login"],
        repository_selection=repo_selection,
        permissions=json.dumps(inst_data.get("permissions", {})),
        events=json.dumps(inst_data.get("events", [])),
        is_active=True,
    )
    db.add(installation)
    db.commit()
    db.refresh(installation)

    return {
        "message": "Installation recorded",
        "installation_id": installation_id,
        "org": account["login"],
        "org_id": str(org_uuid)
    }


@router.post("/webhook")
async def github_webhook(request: Request, db: Session = Depends(get_db)):
    """Handle GitHub webhook events"""
    body = await request.body()
    sig = request.headers.get("X-Hub-Signature-256")
    if not verify_webhook_signature(body, sig):
        raise HTTPException(status_code=401, detail="Invalid signature")

    event = request.headers.get("X-GitHub-Event")
    payload = json.loads(body)

    if event == "installation" and payload["action"] == "created":
        installation_id = str(payload["installation"]["id"])
        account = payload["installation"]["account"]
        repos = payload.get("repositories", [])

        inst = db.query(GitHubInstallation).filter_by(installation_id=installation_id).first()

        # 🔒 Only update if linked to a valid SaaS org (from callback)
        if not inst:
            return {"message": "Ignored unlinked installation"}

        inst.account_login = account["login"]
        inst.account_type = account["type"]
        db.commit()

        # Save repos
        for repo in repos:
            repo_model = GitHubRepository(
                installation_id=inst.id,
                repo_id=str(repo["id"]),
                repo_name=repo["name"],
                full_name=repo["full_name"],
                private=repo["private"],
                description=repo.get("description"),
                default_branch=repo.get("default_branch"),
            )
            db.add(repo_model)
        db.commit()

    elif event == "pull_request":
        action = payload["action"]
        if action in ["opened", "reopened"]:
            pr = payload["pull_request"]
            repo = payload["repository"]
            return {"message": "PR event", "repo": repo["full_name"], "pr": pr["number"]}

    return {"message": "Webhook received", "event": event}
