import os
import msal
import jwt
from fastapi import HTTPException
 
# Azure AD Configuration
AZURE_TENANT_ID = os.getenv("AZURE_TENANT_ID")
AZURE_CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
AZURE_CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
REDIRECT_URI = os.getenv("REDIRECT_URI")
 
AUTHORITY = f"https://login.microsoftonline.com/{AZURE_TENANT_ID}"
 
# Microsoft OpenID configuration
OPENID_CONFIG = f"{AUTHORITY}/v2.0/.well-known/openid-configuration"
 
 
# Initialize MSAL
msal_app = msal.ConfidentialClientApplication(
    AZURE_CLIENT_ID,
    authority=AUTHORITY,
    client_credential=AZURE_CLIENT_SECRET
)
 
 
def get_azure_auth_url():
    """
    Step 1: Generate Azure login URL
    """
    return msal_app.get_authorization_request_url(
        scopes=["User.Read"],
        redirect_uri=REDIRECT_URI
    )
 
 
def validate_azure_token(code: str):
    """
    Step 2: Exchange authorization code for token
    """
    result = msal_app.acquire_token_by_authorization_code(
        code,
        scopes=["User.Read"],
        redirect_uri=REDIRECT_URI
    )
 
    if "error" in result:
        raise HTTPException(
            status_code=400,
            detail=result.get("error_description")
        )
 
    id_token = result.get("id_token")
 
    if not id_token:
        raise HTTPException(
            status_code=400,
            detail="Azure ID token missing"
        )
 
    return id_token
 
 
def verify_azure_token(id_token: str):
    """
    Step 3: Decode Azure token claims
    """
    try:
        decoded = jwt.decode(
            id_token,
            options={"verify_signature": False}
        )
 
        return {
            "email": decoded.get("preferred_username"),
            "name": decoded.get("name"),
            "groups": decoded.get("groups", []),
            "tid": decoded.get("tid"),
        }
 
    except Exception:
        raise HTTPException(
            status_code=401,
            detail="Invalid Azure Token"
        )
