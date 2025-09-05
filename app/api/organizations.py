# Organization management endpoints (Admin only)
# POST /organizations → create new organization
# GET /organizations → list all organizations
# GET /organizations/{org_id} → get organization details
# PUT /organizations/{org_id} → update organization
# DELETE /organizations/{org_id} → deactivate organization

from fastapi import APIRouter, HTTPException, Depends, status
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from typing import List
from pydantic import BaseModel
from app.database import get_db
from app.utils.models import Organization
import uuid
from uuid import UUID
from datetime import datetime

router = APIRouter(prefix="/organizations", tags=["Organizations"])


# --- Models ---
class OrganizationCreate(BaseModel):
    name: str

class OrganizationUpdate(BaseModel):
    name: str | None = None
    is_active: bool | None = None

class OrganizationResponse(BaseModel):
    id: UUID
    name: str
    is_active: bool
    created_at: datetime
    updated_at: datetime | None = None

    class Config:
        from_attributes = True


# --- Endpoints ---
@router.post("/", response_model=OrganizationResponse, status_code=status.HTTP_201_CREATED)
def create_organization(org: OrganizationCreate, db: Session = Depends(get_db)):
    """Create a new organization (Admin only)"""
    # Check if organization name already exists
    existing_org = db.query(Organization).filter(Organization.name == org.name).first()
    if existing_org:
        raise HTTPException(status_code=400, detail="Organization name already exists")

    new_org = Organization(name=org.name)
    
    try:
        db.add(new_org)
        db.commit()
        db.refresh(new_org)
        return new_org
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=400, detail="Failed to create organization")


@router.get("/", response_model=List[OrganizationResponse])
def list_organizations(db: Session = Depends(get_db)):
    """List all organizations (Admin only)"""
    organizations = db.query(Organization).all()
    return organizations


@router.get("/{org_id}", response_model=OrganizationResponse)
def get_organization(org_id: str, db: Session = Depends(get_db)):
    """Get organization details by ID (Admin only)"""
    try:
        org_uuid = uuid.UUID(org_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid organization ID format")

    organization = db.query(Organization).filter(Organization.id == org_uuid).first()
    if not organization:
        raise HTTPException(status_code=404, detail="Organization not found")
    
    return organization


@router.put("/{org_id}", response_model=OrganizationResponse)
def update_organization(org_id: str, org_update: OrganizationUpdate, db: Session = Depends(get_db)):
    """Update organization details (Admin only)"""
    try:
        org_uuid = uuid.UUID(org_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid organization ID format")

    organization = db.query(Organization).filter(Organization.id == org_uuid).first()
    if not organization:
        raise HTTPException(status_code=404, detail="Organization not found")

    # Update fields if provided
    if org_update.name is not None:
        # Check if new name already exists (excluding current org)
        existing_org = db.query(Organization).filter(
            Organization.name == org_update.name,
            Organization.id != org_uuid
        ).first()
        if existing_org:
            raise HTTPException(status_code=400, detail="Organization name already exists")
        organization.name = org_update.name

    if org_update.is_active is not None:
        organization.is_active = org_update.is_active

    try:
        db.commit()
        db.refresh(organization)
        return organization
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=400, detail="Failed to update organization")


@router.delete("/{org_id}")
def deactivate_organization(org_id: str, db: Session = Depends(get_db)):
    """Deactivate organization (Admin only) - Soft delete"""
    try:
        org_uuid = uuid.UUID(org_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid organization ID format")

    organization = db.query(Organization).filter(Organization.id == org_uuid).first()
    if not organization:
        raise HTTPException(status_code=404, detail="Organization not found")

    organization.is_active = False
    
    try:
        db.commit()
        return {"message": "Organization deactivated successfully"}
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=400, detail="Failed to deactivate organization")
