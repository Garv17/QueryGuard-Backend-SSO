"""
Data Catalog API endpoints
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import Optional
from uuid import UUID

from app.database import get_db
from app.utils.auth_deps import get_current_user
from app.utils.models import User
from app.data_catalog.service import (
    search_tables,
    get_table_detail,
    get_table_lineage,
    create_or_update_table_metadata,
    get_table_metadata,
    delete_table_metadata,
    build_table_id,
    parse_table_id
)
from app.data_catalog.models import (
    TableSearchResponse,
    TableDetailResponse,
    LineageGraphResponse,
    TableMetadataCreate
)

router = APIRouter(prefix="/data-catalog", tags=["Data Catalog"])


@router.get("/search", response_model=TableSearchResponse)
def search_tables_endpoint(
    q: Optional[str] = Query(None, description="Search query (searches table name, schema, database)"),
    limit: int = Query(50, ge=1, le=100, description="Maximum number of results"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Search for tables in the data catalog.
    Searches through all tables present in the lineage data.
    """
    try:
        results, total = search_tables(
            db=db,
            org_id=current_user.org_id,
            search_query=q,
            limit=limit,
            offset=offset
        )
        
        return TableSearchResponse(
            results=results,
            total=total
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error searching tables: {str(e)}")


# IMPORTANT: More specific routes must come BEFORE less specific ones
# Otherwise FastAPI will match /lineage as part of table_id

@router.get("/tables/{table_id:path}/lineage", response_model=LineageGraphResponse)
def get_table_lineage_endpoint(
    table_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Get lineage graph for a specific table.
    Returns upstream and downstream lineage data similar to the analysis endpoint format.
    
    Note: table_id can contain slashes (e.g., "database/schema/table_name")
    """
    try:
        # URL decode the table_id in case it was double-encoded
        from urllib.parse import unquote
        table_id = unquote(table_id)
        
        # First verify table exists
        table_detail = get_table_detail(
            db=db,
            org_id=current_user.org_id,
            table_id=table_id
        )
        
        if not table_detail:
            raise HTTPException(status_code=404, detail=f"Table not found: {table_id}")
        
        lineage = get_table_lineage(
            db=db,
            org_id=current_user.org_id,
            table_id=table_id
        )
        
        return lineage
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching lineage: {str(e)}")


@router.get("/tables/{table_id:path}", response_model=TableDetailResponse)
def get_table_detail_endpoint(
    table_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Get detailed information about a specific table.
    Includes metadata, columns, and basic information.
    
    Note: table_id can contain slashes (e.g., "database/schema/table_name")
    Use {table_id:path} to allow slashes in the path parameter.
    """
    try:
        # URL decode the table_id in case it was double-encoded
        from urllib.parse import unquote
        table_id = unquote(table_id)
        
        table_detail = get_table_detail(
            db=db,
            org_id=current_user.org_id,
            table_id=table_id
        )
        
        if not table_detail:
            raise HTTPException(status_code=404, detail=f"Table not found: {table_id}")
        
        return table_detail
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching table details: {str(e)}")


@router.put("/tables/{table_id:path}/metadata", response_model=TableDetailResponse)
def create_or_update_table_metadata_endpoint(
    table_id: str,
    metadata: TableMetadataCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Create or update table metadata.
    This endpoint allows users to add/edit table descriptions, column descriptions, owners, etc.
    """
    try:
        # URL decode the table_id in case it was double-encoded
        from urllib.parse import unquote
        table_id = unquote(table_id)
        
        # Verify table exists by trying to get its detail
        table_detail = get_table_detail(
            db=db,
            org_id=current_user.org_id,
            table_id=table_id
        )
        
        if not table_detail:
            raise HTTPException(status_code=404, detail=f"Table not found: {table_id}")
        
        # Parse table_id to get components
        database, schema, table_name = parse_table_id(table_id)
        
        # Create or update metadata
        metadata_record = create_or_update_table_metadata(
            db=db,
            org_id=current_user.org_id,
            table_id=table_id,
            database=database,
            schema=schema,
            table_name=table_name,
            description=metadata.description,
            owner=metadata.owner,
            column_descriptions=metadata.column_descriptions,
            user_id=current_user.id
        )
        
        # Return updated table detail
        updated_detail = get_table_detail(
            db=db,
            org_id=current_user.org_id,
            table_id=table_id
        )
        
        return updated_detail
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error saving metadata: {str(e)}")


@router.get("/tables/{table_id:path}/metadata")
def get_table_metadata_endpoint(
    table_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Get table metadata only (without full table details).
    """
    try:
        # URL decode the table_id in case it was double-encoded
        from urllib.parse import unquote
        table_id = unquote(table_id)
        
        metadata = get_table_metadata(
            db=db,
            org_id=current_user.org_id,
            table_id=table_id
        )
        
        if not metadata:
            raise HTTPException(status_code=404, detail=f"Metadata not found for table: {table_id}")
        
        return {
            "table_id": metadata.table_id,
            "description": metadata.description,
            "owner": metadata.owner,
            "tags": metadata.tags,
            "column_descriptions": metadata.column_descriptions,
            "created_at": metadata.created_at.isoformat() if metadata.created_at else None,
            "updated_at": metadata.updated_at.isoformat() if metadata.updated_at else None
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching metadata: {str(e)}")


@router.delete("/tables/{table_id:path}/metadata")
def delete_table_metadata_endpoint(
    table_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Delete table metadata.
    """
    try:
        # URL decode the table_id in case it was double-encoded
        from urllib.parse import unquote
        table_id = unquote(table_id)
        
        deleted = delete_table_metadata(
            db=db,
            org_id=current_user.org_id,
            table_id=table_id
        )
        
        if not deleted:
            raise HTTPException(status_code=404, detail=f"Metadata not found for table: {table_id}")
        
        return {"message": "Metadata deleted successfully", "table_id": table_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error deleting metadata: {str(e)}")

