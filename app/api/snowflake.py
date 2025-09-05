# POST /snowflake/test-connection → test connection
# POST /snowflake/save-connection → save connection (after successful test)
# GET /snowflake/fetch-databases → fetch all databases
# GET /snowflake/fetch-schemas/{database} → fetch schemas for selected DB
# POST /snowflake/save-schema-selection → save DB + schema selections

from fastapi import APIRouter, HTTPException, Depends, status
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from typing import List
from pydantic import BaseModel
import snowflake.connector
from app.database import get_db
from app.utils.models import SnowflakeConnection, SnowflakeDatabase, SnowflakeSchema, User
from app.api.auth import get_current_user
import uuid
from uuid import UUID
from datetime import datetime

router = APIRouter(prefix="/snowflake", tags=["Snowflake"])


# --- Helpers ---
def test_connection(account, username, password, warehouse=None, database=None, schema=None):
    try:
        conn = snowflake.connector.connect(
            user=username,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema
        )
        conn.close()
        return True
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Connection failed: {str(e)}")


# --- Models ---
class SnowflakeConn(BaseModel):
    connection_name: str
    account: str
    username: str
    password: str
    warehouse: str = None
    role: str = None
    cron_expression: str = None

class DatabaseSelection(BaseModel):
    database_names: List[str]

class SchemaSelection(BaseModel):
    database_name: str
    schema_names: List[str]

class ConnectionResponse(BaseModel):
    id: UUID
    connection_name: str
    account: str
    username: str
    warehouse: str | None
    cron_expression: str | None
    is_active: bool
    created_at: datetime

    class Config:
        from_attributes = True

class DatabaseResponse(BaseModel):
    id: UUID
    database_name: str
    is_selected: bool
    created_at: datetime

    class Config:
        from_attributes = True

class SchemaResponse(BaseModel):
    id: UUID
    schema_name: str
    is_selected: bool
    created_at: datetime

    class Config:
        from_attributes = True


# --- Endpoints ---
@router.post("/test-connection")
def snowflake_test_connection(conn: SnowflakeConn, current_user: User = Depends(get_current_user)):
    """Test Snowflake connection before saving"""
    success = test_connection(
        account=conn.account,
        username=conn.username,
        password=conn.password,
        warehouse=conn.warehouse
    )
    return {"message": "Connection successful"} if success else {"message": "Connection failed"}


@router.post("/save-connection", response_model=ConnectionResponse, status_code=status.HTTP_201_CREATED)
def save_connection(conn: SnowflakeConn, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Save Snowflake connection after successful test"""
    # Test connection before saving
    test_connection(conn.account, conn.username, conn.password, conn.warehouse)

    # Check if connection name already exists for this org
    existing_conn = db.query(SnowflakeConnection).filter(
        SnowflakeConnection.org_id == current_user.org_id,
        SnowflakeConnection.connection_name == conn.connection_name,
        SnowflakeConnection.is_active == True
    ).first()
    
    if existing_conn:
        raise HTTPException(status_code=400, detail="Connection name already exists for this organization")

    new_connection = SnowflakeConnection(
        org_id=current_user.org_id,
        connection_name=conn.connection_name,
        account=conn.account,
        username=conn.username,
        password=conn.password,
        warehouse=conn.warehouse,
        role=conn.role,
        cron_expression=conn.cron_expression
    )
    
    try:
        db.add(new_connection)
        db.commit()
        db.refresh(new_connection)
        return new_connection
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=400, detail="Failed to save connection")


@router.get("/connections", response_model=List[ConnectionResponse])
def list_connections(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    """List all Snowflake connections for the organization"""
    connections = db.query(SnowflakeConnection).filter(
        SnowflakeConnection.org_id == current_user.org_id,
        SnowflakeConnection.is_active == True
    ).all()
    return connections


@router.get("/fetch-databases/{connection_id}")
def fetch_databases(connection_id: str, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Fetch all databases from Snowflake connection"""
    try:
        conn_uuid = uuid.UUID(connection_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid connection ID format")

    # Get connection details
    connection = db.query(SnowflakeConnection).filter(
        SnowflakeConnection.id == conn_uuid,
        SnowflakeConnection.org_id == current_user.org_id,
        SnowflakeConnection.is_active == True
    ).first()
    
    if not connection:
        raise HTTPException(status_code=404, detail="Snowflake connection not found")

    try:
        # Connect to Snowflake and fetch databases
        snowflake_conn = snowflake.connector.connect(
            user=connection.username,
            password=connection.password,
            account=connection.account,
            warehouse=connection.warehouse,
            role=connection.role
        )
        cur = snowflake_conn.cursor()
        cur.execute("SHOW DATABASES")
        databases = [row[1] for row in cur.fetchall()]
        cur.close()
        snowflake_conn.close()
        
        # Store databases in our database
        for db_name in databases:
            existing_db = db.query(SnowflakeDatabase).filter(
                SnowflakeDatabase.connection_id == conn_uuid,
                SnowflakeDatabase.database_name == db_name
            ).first()
            
            if not existing_db:
                new_db = SnowflakeDatabase(
                    connection_id=conn_uuid,
                    database_name=db_name
                )
                db.add(new_db)
        
        db.commit()
        return {"databases": databases}
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/fetch-schemas/{connection_id}/{database_name}")
def fetch_schemas(connection_id: str, database_name: str, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Fetch schemas for a specific database"""
    try:
        conn_uuid = uuid.UUID(connection_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid connection ID format")

    # Get connection details
    connection = db.query(SnowflakeConnection).filter(
        SnowflakeConnection.id == conn_uuid,
        SnowflakeConnection.org_id == current_user.org_id,
        SnowflakeConnection.is_active == True
    ).first()
    
    if not connection:
        raise HTTPException(status_code=404, detail="Snowflake connection not found")

    try:
        # Connect to Snowflake and fetch schemas
        snowflake_conn = snowflake.connector.connect(
            user=connection.username,
            password=connection.password,
            account=connection.account,
            warehouse=connection.warehouse,
            database=database_name,
            role=connection.role
        )
        cur = snowflake_conn.cursor()
        cur.execute("SHOW SCHEMAS")
        schemas = [row[1] for row in cur.fetchall()]
        cur.close()
        snowflake_conn.close()
        
        # Get or create database record
        database = db.query(SnowflakeDatabase).filter(
            SnowflakeDatabase.connection_id == conn_uuid,
            SnowflakeDatabase.database_name == database_name
        ).first()
        
        if not database:
            database = SnowflakeDatabase(
                connection_id=conn_uuid,
                database_name=database_name
            )
            db.add(database)
            db.flush()  # Get the ID
        
        # Store schemas in our database
        for schema_name in schemas:
            existing_schema = db.query(SnowflakeSchema).filter(
                SnowflakeSchema.database_id == database.id,
                SnowflakeSchema.schema_name == schema_name
            ).first()
            
            if not existing_schema:
                new_schema = SnowflakeSchema(
                    database_id=database.id,
                    schema_name=schema_name
                )
                db.add(new_schema)
        
        db.commit()
        return {"schemas": schemas}
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/save-database-selection")
def save_database_selection(selection: DatabaseSelection, connection_id: str, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Save database selections for a connection"""
    try:
        conn_uuid = uuid.UUID(connection_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid connection ID format")

    # Verify connection belongs to user's org
    connection = db.query(SnowflakeConnection).filter(
        SnowflakeConnection.id == conn_uuid,
        SnowflakeConnection.org_id == current_user.org_id,
        SnowflakeConnection.is_active == True
    ).first()
    
    if not connection:
        raise HTTPException(status_code=404, detail="Snowflake connection not found")

    # Update database selections
    database_rows = db.query(SnowflakeDatabase).filter(
        SnowflakeDatabase.connection_id == conn_uuid
    ).all()
    
    for database_row in database_rows:
        database_row.is_selected = database_row.database_name in selection.database_names
    
    db.commit()
    return {"message": "Database selections saved"}


@router.post("/save-schema-selection")
def save_schema_selection(selection: SchemaSelection, connection_id: str, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Save schema selections for a specific database"""
    try:
        conn_uuid = uuid.UUID(connection_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid connection ID format")

    # Verify connection belongs to user's org
    connection = db.query(SnowflakeConnection).filter(
        SnowflakeConnection.id == conn_uuid,
        SnowflakeConnection.org_id == current_user.org_id,
        SnowflakeConnection.is_active == True
    ).first()
    
    if not connection:
        raise HTTPException(status_code=404, detail="Snowflake connection not found")

    # Get database
    database = db.query(SnowflakeDatabase).filter(
        SnowflakeDatabase.connection_id == conn_uuid,
        SnowflakeDatabase.database_name == selection.database_name
    ).first()
    
    if not database:
        raise HTTPException(status_code=404, detail="Database not found")

    # Update schema selections
    schemas = db.query(SnowflakeSchema).filter(
        SnowflakeSchema.database_id == database.id
    ).all()
    
    for schema in schemas:
        schema.is_selected = schema.schema_name in selection.schema_names
    
    db.commit()
    return {"message": "Schema selections saved"}


@router.get("/selected-databases/{connection_id}", response_model=List[DatabaseResponse])
def get_selected_databases(connection_id: str, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Get selected databases for a connection"""
    try:
        conn_uuid = uuid.UUID(connection_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid connection ID format")

    # Verify connection belongs to user's org
    connection = db.query(SnowflakeConnection).filter(
        SnowflakeConnection.id == conn_uuid,
        SnowflakeConnection.org_id == current_user.org_id,
        SnowflakeConnection.is_active == True
    ).first()
    
    if not connection:
        raise HTTPException(status_code=404, detail="Snowflake connection not found")

    selected_databases = db.query(SnowflakeDatabase).filter(
        SnowflakeDatabase.connection_id == conn_uuid,
        SnowflakeDatabase.is_selected == True
    ).all()
    
    return selected_databases


@router.get("/selected-schemas/{connection_id}/{database_name}", response_model=List[SchemaResponse])
def get_selected_schemas(connection_id: str, database_name: str, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Get selected schemas for a specific database"""
    try:
        conn_uuid = uuid.UUID(connection_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid connection ID format")

    # Verify connection belongs to user's org
    connection = db.query(SnowflakeConnection).filter(
        SnowflakeConnection.id == conn_uuid,
        SnowflakeConnection.org_id == current_user.org_id,
        SnowflakeConnection.is_active == True
    ).first()
    
    if not connection:
        raise HTTPException(status_code=404, detail="Snowflake connection not found")

    # Get database
    database = db.query(SnowflakeDatabase).filter(
        SnowflakeDatabase.connection_id == conn_uuid,
        SnowflakeDatabase.database_name == database_name
    ).first()
    
    if not database:
        raise HTTPException(status_code=404, detail="Database not found")

    selected_schemas = db.query(SnowflakeSchema).filter(
        SnowflakeSchema.database_id == database.id,
        SnowflakeSchema.is_selected == True
    ).all()
    
    return selected_schemas
