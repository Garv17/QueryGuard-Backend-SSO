# POST /snowflake/test-connection → test connection
# POST /snowflake/save-connection → save connection (after successful test)
# GET /snowflake/fetch-databases → fetch all databases
# GET /snowflake/fetch-schemas/{database} → fetch schemas for selected DB
# POST /snowflake/save-schema-selection → save DB + schema selections

from fastapi import APIRouter, HTTPException, Depends, status, Request
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from typing import List
from pydantic import BaseModel
import snowflake.connector
from app.database import get_db
from app.utils.models import SnowflakeConnection, SnowflakeDatabase, SnowflakeSchema, User, SnowflakeJob
from app.api.auth import get_current_user
import uuid
from uuid import UUID
from datetime import datetime
import logging

router = APIRouter(prefix="/snowflake", tags=["Snowflake"])
logger = logging.getLogger("snowflake")


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
        logger.info("/snowflake/test-connection - success for user=%s account=%s", username, account)
        return True
    except Exception as e:
        logger.warning("/snowflake/test-connection - failed: %s", str(e))
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
def snowflake_test_connection(conn: SnowflakeConn, current_user: User = Depends(get_current_user), request: Request = None):
    """Test Snowflake connection before saving"""
    success = test_connection(
        account=conn.account,
        username=conn.username,
        password=conn.password,
        warehouse=conn.warehouse
    )
    return {"message": "Connection successful"} if success else {"message": "Connection failed"}


@router.post("/save-connection", response_model=ConnectionResponse, status_code=status.HTTP_201_CREATED)
def save_connection(conn: SnowflakeConn, current_user: User = Depends(get_current_user), db: Session = Depends(get_db), request: Request = None):
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
        logger.warning("/snowflake/save-connection - name exists %s", conn.connection_name)
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
        # Ensure/create job row for this connection if cron is provided
        if conn.cron_expression:
            existing_job = db.query(SnowflakeJob).filter(SnowflakeJob.connection_id == new_connection.id).first()
            if existing_job:
                existing_job.cron_expression = conn.cron_expression
                existing_job.is_active = True
            else:
                db.add(SnowflakeJob(
                    connection_id=new_connection.id,
                    cron_expression=conn.cron_expression,
                    last_run_time=None,
                    is_active=True
                ))
            db.commit()
        logger.info("/snowflake/save-connection - saved id=%s", new_connection.id)
        return new_connection
    except IntegrityError:
        db.rollback()
        logger.exception("/snowflake/save-connection - failed to save connection")
        raise HTTPException(status_code=400, detail="Failed to save connection")


@router.get("/connections", response_model=List[ConnectionResponse])
def list_connections(current_user: User = Depends(get_current_user), db: Session = Depends(get_db), request: Request = None):
    """List all Snowflake connections for the organization"""
    connections = db.query(SnowflakeConnection).filter(
        SnowflakeConnection.org_id == current_user.org_id,
        SnowflakeConnection.is_active == True
    ).all()
    logger.debug("/snowflake/connections - list count=%d", len(connections))
    return connections


@router.get("/fetch-databases/{connection_id}")
def fetch_databases(connection_id: str, current_user: User = Depends(get_current_user), db: Session = Depends(get_db), request: Request = None):
    """Fetch all databases from Snowflake connection"""
    try:
        conn_uuid = uuid.UUID(connection_id)
    except ValueError:
        logger.warning("/snowflake/fetch-databases - invalid id %s", connection_id)
        raise HTTPException(status_code=400, detail="Invalid connection ID format")

    # Get connection details
    connection = db.query(SnowflakeConnection).filter(
        SnowflakeConnection.id == conn_uuid,
        SnowflakeConnection.org_id == current_user.org_id,
        SnowflakeConnection.is_active == True
    ).first()
    
    if not connection:
        logger.warning("/snowflake/fetch-databases - connection not found %s", conn_uuid)
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
        logger.info("/snowflake/fetch-databases - fetched %d databases", len(databases))
        return {"databases": databases}
        
    except Exception as e:
        logger.exception("/snowflake/fetch-databases - error: %s", str(e))
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/fetch-schemas/{connection_id}/{database_name}")
def fetch_schemas(connection_id: str, database_name: str, current_user: User = Depends(get_current_user), db: Session = Depends(get_db), request: Request = None):
    """Fetch schemas for a specific database"""
    try:
        conn_uuid = uuid.UUID(connection_id)
    except ValueError:
        logger.warning("/snowflake/fetch-schemas - invalid id %s", connection_id)
        raise HTTPException(status_code=400, detail="Invalid connection ID format")

    # Get connection details
    connection = db.query(SnowflakeConnection).filter(
        SnowflakeConnection.id == conn_uuid,
        SnowflakeConnection.org_id == current_user.org_id,
        SnowflakeConnection.is_active == True
    ).first()
    
    if not connection:
        logger.warning("/snowflake/fetch-schemas - connection not found %s", conn_uuid)
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
        logger.info("/snowflake/fetch-schemas - fetched %d schemas for %s", len(schemas), database_name)
        return {"schemas": schemas}
        
    except Exception as e:
        logger.exception("/snowflake/fetch-schemas - error: %s", str(e))
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/save-database-selection")
def save_database_selection(selection: DatabaseSelection, connection_id: str, current_user: User = Depends(get_current_user), db: Session = Depends(get_db), request: Request = None):
    """Save database selections for a connection"""
    try:
        conn_uuid = uuid.UUID(connection_id)
    except ValueError:
        logger.warning("/snowflake/save-database-selection - invalid id %s", connection_id)
        raise HTTPException(status_code=400, detail="Invalid connection ID format")

    # Verify connection belongs to user's org
    connection = db.query(SnowflakeConnection).filter(
        SnowflakeConnection.id == conn_uuid,
        SnowflakeConnection.org_id == current_user.org_id,
        SnowflakeConnection.is_active == True
    ).first()
    
    if not connection:
        logger.warning("/snowflake/save-database-selection - connection not found %s", conn_uuid)
        raise HTTPException(status_code=404, detail="Snowflake connection not found")

    # Update database selections
    database_rows = db.query(SnowflakeDatabase).filter(
        SnowflakeDatabase.connection_id == conn_uuid
    ).all()
    
    for database_row in database_rows:
        database_row.is_selected = database_row.database_name in selection.database_names
    
    db.commit()
    logger.info("/snowflake/save-database-selection - saved selections count=%d", len(selection.database_names))
    return {"message": "Database selections saved"}


@router.post("/save-schema-selection")
def save_schema_selection(selection: SchemaSelection, connection_id: str, current_user: User = Depends(get_current_user), db: Session = Depends(get_db), request: Request = None):
    """Save schema selections for a specific database"""
    try:
        conn_uuid = uuid.UUID(connection_id)
    except ValueError:
        logger.warning("/snowflake/save-schema-selection - invalid id %s", connection_id)
        raise HTTPException(status_code=400, detail="Invalid connection ID format")

    # Verify connection belongs to user's org
    connection = db.query(SnowflakeConnection).filter(
        SnowflakeConnection.id == conn_uuid,
        SnowflakeConnection.org_id == current_user.org_id,
        SnowflakeConnection.is_active == True
    ).first()
    
    if not connection:
        logger.warning("/snowflake/save-schema-selection - connection not found %s", conn_uuid)
        raise HTTPException(status_code=404, detail="Snowflake connection not found")

    # Get database
    database = db.query(SnowflakeDatabase).filter(
        SnowflakeDatabase.connection_id == conn_uuid,
        SnowflakeDatabase.database_name == selection.database_name
    ).first()
    
    if not database:
        logger.warning("/snowflake/save-schema-selection - database not found %s", selection.database_name)
        raise HTTPException(status_code=404, detail="Database not found")

    # Update schema selections
    schemas = db.query(SnowflakeSchema).filter(
        SnowflakeSchema.database_id == database.id
    ).all()
    
    for schema in schemas:
        schema.is_selected = schema.schema_name in selection.schema_names
    
    db.commit()
    logger.info("/snowflake/save-schema-selection - saved %d schemas for %s", len(selection.schema_names), selection.database_name)
    return {"message": "Schema selections saved"}


@router.get("/selected-databases/{connection_id}", response_model=List[DatabaseResponse])
def get_selected_databases(connection_id: str, current_user: User = Depends(get_current_user), db: Session = Depends(get_db), request: Request = None):
    """Get selected databases for a connection"""
    try:
        conn_uuid = uuid.UUID(connection_id)
    except ValueError:
        logger.warning("/snowflake/selected-databases - invalid id %s", connection_id)
        raise HTTPException(status_code=400, detail="Invalid connection ID format")

    # Verify connection belongs to user's org
    connection = db.query(SnowflakeConnection).filter(
        SnowflakeConnection.id == conn_uuid,
        SnowflakeConnection.org_id == current_user.org_id,
        SnowflakeConnection.is_active == True
    ).first()
    
    if not connection:
        logger.warning("/snowflake/selected-databases - connection not found %s", conn_uuid)
        raise HTTPException(status_code=404, detail="Snowflake connection not found")

    selected_databases = db.query(SnowflakeDatabase).filter(
        SnowflakeDatabase.connection_id == conn_uuid,
        SnowflakeDatabase.is_selected == True
    ).all()
    
    logger.debug("/snowflake/selected-databases - count=%d", len(selected_databases))
    return selected_databases


@router.get("/selected-schemas/{connection_id}/{database_name}", response_model=List[SchemaResponse])
def get_selected_schemas(connection_id: str, database_name: str, current_user: User = Depends(get_current_user), db: Session = Depends(get_db), request: Request = None):
    """Get selected schemas for a specific database"""
    try:
        conn_uuid = uuid.UUID(connection_id)
    except ValueError:
        logger.warning("/snowflake/selected-schemas - invalid id %s", connection_id)
        raise HTTPException(status_code=400, detail="Invalid connection ID format")

    # Verify connection belongs to user's org
    connection = db.query(SnowflakeConnection).filter(
        SnowflakeConnection.id == conn_uuid,
        SnowflakeConnection.org_id == current_user.org_id,
        SnowflakeConnection.is_active == True
    ).first()
    
    if not connection:
        logger.warning("/snowflake/selected-schemas - connection not found %s", conn_uuid)
        raise HTTPException(status_code=404, detail="Snowflake connection not found")

    # Get database
    database = db.query(SnowflakeDatabase).filter(
        SnowflakeDatabase.connection_id == conn_uuid,
        SnowflakeDatabase.database_name == database_name
    ).first()
    
    if not database:
        logger.warning("/snowflake/selected-schemas - database not found %s", database_name)
        raise HTTPException(status_code=404, detail="Database not found")

    selected_schemas = db.query(SnowflakeSchema).filter(
        SnowflakeSchema.database_id == database.id,
        SnowflakeSchema.is_selected == True
    ).all()
    
    logger.debug("/snowflake/selected-schemas - count=%d", len(selected_schemas))
    return selected_schemas
