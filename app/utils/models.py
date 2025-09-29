from sqlalchemy import Column, String, DateTime, Text, Boolean, ForeignKey, Integer, BigInteger
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from app.database import Base
import uuid


class Organization(Base):
    __tablename__ = "organizations"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(100), nullable=False, index=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationship
    users = relationship("User", back_populates="organization")


class User(Base):
    __tablename__ = "users"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    username = Column(String(50), unique=True, nullable=False, index=True)
    email = Column(String(100), unique=True, nullable=False, index=True)
    password_hash = Column(String(255), nullable=False)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False, index=True)
    password_reset_otp = Column(String(6), nullable=True)
    reset_otp_expires = Column(DateTime, nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationship
    organization = relationship("Organization", back_populates="users")


class UserToken(Base):
    __tablename__ = "user_tokens"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    token = Column(Text, nullable=False, unique=True, index=True)
    expires_at = Column(DateTime, nullable=False)
    is_revoked = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class SnowflakeConnection(Base):
    __tablename__ = "snowflake_connections"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False, index=True)
    connection_name = Column(String(100), nullable=False)
    account = Column(String(100), nullable=False)
    username = Column(String(100), nullable=False)
    password = Column(String(255), nullable=False)
    warehouse = Column(String(100), nullable=True)
    role = Column(String(100), nullable=True)
    cron_expression = Column(String(100), nullable=True)  # Miner config
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationships
    organization = relationship("Organization", backref="snowflake_connections")
    databases = relationship("SnowflakeDatabase", back_populates="connection", cascade="all, delete-orphan")
    job = relationship("SnowflakeJob", back_populates="connection", uselist=False, cascade="all, delete-orphan")


class SnowflakeDatabase(Base):
    __tablename__ = "snowflake_databases"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    connection_id = Column(UUID(as_uuid=True), ForeignKey("snowflake_connections.id"), nullable=False, index=True)
    database_name = Column(String(100), nullable=False)
    is_selected = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    connection = relationship("SnowflakeConnection", back_populates="databases")
    schemas = relationship("SnowflakeSchema", back_populates="database", cascade="all, delete-orphan")


class SnowflakeSchema(Base):
    __tablename__ = "snowflake_schemas"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    database_id = Column(UUID(as_uuid=True), ForeignKey("snowflake_databases.id"), nullable=False, index=True)
    schema_name = Column(String(100), nullable=False)
    is_selected = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    database = relationship("SnowflakeDatabase", back_populates="schemas")


class GitHubInstallation(Base):
    __tablename__ = "github_installations"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    installation_id = Column(String(50), unique=True, nullable=False, index=True)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False, index=True)
    account_type = Column(String(20), nullable=False)  # 'User' or 'Organization'
    account_login = Column(String(100), nullable=False)
    repository_selection = Column(String(20), nullable=False)  # 'all' or 'selected'
    permissions = Column(Text, nullable=True)  # JSON string of permissions
    events = Column(Text, nullable=True)  # JSON string of events
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationships
    organization = relationship("Organization", backref="github_installations")
    repositories = relationship("GitHubRepository", back_populates="installation", cascade="all, delete-orphan")


class GitHubRepository(Base):
    __tablename__ = "github_repositories"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    installation_id = Column(UUID(as_uuid=True), ForeignKey("github_installations.id"), nullable=False, index=True)
    repo_id = Column(String(50), nullable=False, index=True)
    repo_name = Column(String(200), nullable=False)
    full_name = Column(String(200), nullable=False)
    private = Column(Boolean, default=False)
    description = Column(Text, nullable=True)
    default_branch = Column(String(100), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationships
    installation = relationship("GitHubInstallation", back_populates="repositories")


# New code

class JiraConnection(Base):
    __tablename__ = "jira_connections"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False, index=True)
    connection_name = Column(String(100), nullable=False)
    server_url = Column(String(255), nullable=False)  # e.g., https://company.atlassian.net
    username = Column(String(100), nullable=False)  # Email for Atlassian Cloud
    api_token = Column(String(255), nullable=False)  # API token or password
    project_key = Column(String(20), nullable=False)  # Default project key for tickets
    issue_type = Column(String(50), default="Task")  # Default issue type
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationships
    organization = relationship("Organization", backref="jira_connections")


class JiraTicket(Base):
    __tablename__ = "jira_tickets"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    connection_id = Column(UUID(as_uuid=True), ForeignKey("jira_connections.id"), nullable=False, index=True)
    ticket_key = Column(String(50), nullable=False, index=True)  # e.g., PROJ-123
    ticket_url = Column(String(500), nullable=False)
    summary = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    issue_type = Column(String(50), nullable=False)
    status = Column(String(50), nullable=False)
    priority = Column(String(50), nullable=True)
    assignee = Column(String(100), nullable=True)
    pr_url = Column(String(500), nullable=True)  # Related PR URL
    analysis_report_url = Column(String(500), nullable=True)  # Analysis report URL
    created_by = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationships
    connection = relationship("JiraConnection", backref="tickets")
    creator = relationship("User", backref="created_jira_tickets")


# Snowflake crawler job and audit models

class SnowflakeJob(Base):
    __tablename__ = "snowflake_jobs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    connection_id = Column(UUID(as_uuid=True), ForeignKey("snowflake_connections.id"), unique=True, nullable=False, index=True)
    cron_expression = Column(String(100), nullable=False)
    last_run_time = Column(DateTime(timezone=True), nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    connection = relationship("SnowflakeConnection", back_populates="job")


class SnowflakeCrawlAudit(Base):
    __tablename__ = "snowflake_crawl_audit"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False, index=True)
    batch_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    connection_id = Column(UUID(as_uuid=True), ForeignKey("snowflake_connections.id"), nullable=False, index=True)
    scheduled_at = Column(DateTime(timezone=True), nullable=False)
    started_at = Column(DateTime(timezone=True), server_default=func.now())
    finished_at = Column(DateTime(timezone=True), nullable=True)
    status = Column(String(20), nullable=False, default="running")  # running|success|failed
    query_history_rows_fetched = Column(Integer, nullable=False, default=0)
    information_schema_columns_rows_fetched = Column(Integer, nullable=False, default=0)
    error_message = Column(Text, nullable=True)

    organization = relationship("Organization", foreign_keys=[org_id], backref="snowflake_crawl_audits_org_id")
    connection = relationship("SnowflakeConnection", foreign_keys=[connection_id], backref="snowflake_crawl_audits_conn_id")


class SnowflakeQueryRecord(Base):
    __tablename__ = "snowflake_query_history"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False, index=True)
    batch_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    connection_id = Column(UUID(as_uuid=True), ForeignKey("snowflake_connections.id"), nullable=False, index=True)
    query_id = Column(String(100), nullable=False, index=True)
    query_text = Column(Text, nullable=True)
    database_name = Column(String(200), nullable=True)
    database_id = Column(Integer, nullable=True)
    schema_name = Column(String(200), nullable=True)
    schema_id = Column(Integer, nullable=True)
    start_time = Column(DateTime(timezone=True), nullable=False)
    end_time = Column(DateTime(timezone=True), nullable=True)
    session_id = Column(BigInteger, nullable=True)
    base_objects_accessed = Column(JSONB, nullable=True)
    objects_modified = Column(JSONB, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    organization = relationship("Organization", foreign_keys=[org_id], backref="snowflake_query_record_ord_id")
    connection = relationship("SnowflakeConnection", foreign_keys=[connection_id], backref="snowflake_query_record_conn_id")


class InformationSchemacolumns(Base):
    __tablename__ = "information_schema_columns"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False, index=True)
    batch_id = Column(UUID(as_uuid=True), ForeignKey("snowflake_query_history.batch_id"), nullable=False, index=True)
    connection_id = Column(UUID(as_uuid=True), ForeignKey("snowflake_connections.id"), nullable=False, index=True)
    table_catalog = Column(String(200), nullable=True)
    table_schema = Column(String(200), nullable=True)
    table_name = Column(String(200), nullable=True)
    column_name = Column(String(200), nullable=True)
    data_type = Column(String(100), nullable=True)
    ordinal_position = Column(Integer, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    organization = relationship("Organization", foreign_keys=[org_id], backref="information_schema_columns_org_id")
    batch = relationship("SnowflakeQueryRecord", foreign_keys=[batch_id], viewonly=True)
    connection = relationship("SnowflakeConnection", foreign_keys=[connection_id], backref="information_schema_columns_conn_id")

class ColumnLevelLineage(Base):
    __tablename__ = "column_level_lineage"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False, index=True)
    batch_id = Column(UUID(as_uuid=True), ForeignKey("snowflake_query_history.batch_id"), nullable=False, index=True)
    connection_id = Column(UUID(as_uuid=True), ForeignKey("snowflake_connections.id"), nullable=False, index=True)
    source_database = Column(String(200), nullable=True)
    source_schema = Column(String(200), nullable=True)
    source_table = Column(String(200), nullable=True)
    source_column = Column(String(200), nullable=True)
    target_database = Column(String(200), nullable=True)
    target_schema = Column(String(200), nullable=True)
    target_table = Column(String(200), nullable=True)
    target_column = Column(String(200), nullable=True)
    query_id = Column(JSONB, nullable=True)  # store list of query IDs
    query_type = Column(String(50), nullable=True)
    session_id = Column(BigInteger, nullable=True)
    dependency_score = Column(Integer, nullable=True)
    dbt_model_file_path = Column(String(200), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    is_active = Column(Integer, nullable=False, server_default="1")
    
    # Relationships
    organization = relationship("Organization", foreign_keys=[org_id], backref="column_level_lineage_org_id")
    batch = relationship("SnowflakeQueryRecord", foreign_keys=[batch_id], viewonly=True)
    connection = relationship("SnowflakeConnection", foreign_keys=[connection_id], backref="column_level_lineage_conn_id")

class FilterClauseColumnLineage(Base):
    __tablename__ = "filter_clause_column_lineage"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False, index=True)
    batch_id = Column(UUID(as_uuid=True), ForeignKey("snowflake_query_history.batch_id"), nullable=False, index=True)
    connection_id = Column(UUID(as_uuid=True), ForeignKey("snowflake_connections.id"), nullable=False, index=True)
    source_database = Column(String(200), nullable=True)
    source_schema = Column(String(200), nullable=True)
    source_table = Column(String(200), nullable=True)
    source_column = Column(String(200), nullable=True)
    target_database = Column(String(200), nullable=True)
    target_schema = Column(String(200), nullable=True)
    target_table = Column(String(200), nullable=True)
    target_column = Column(String(200), nullable=True)
    query_id = Column(JSONB, nullable=True)  # store list of query IDs
    query_type = Column(String(50), nullable=True)
    session_id = Column(BigInteger, nullable=True)
    dependency_score = Column(Integer, nullable=True)
    dbt_model_file_path = Column(String(200), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    is_active = Column(Integer, nullable=False, server_default="1")
    
    # Relationships
    organization = relationship("Organization", foreign_keys=[org_id], backref="filter_clause_column_org_id")
    batch = relationship("SnowflakeQueryRecord", foreign_keys=[batch_id], viewonly=True)
    connection = relationship("SnowflakeConnection", foreign_keys=[connection_id], backref="filter_clause_column_conn_id")


class LineageLoadWatermark(Base):
    __tablename__ = "lineage_load_watermarks"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False, index=True)
    batch_id = Column(UUID(as_uuid=True), ForeignKey("snowflake_query_history.batch_id"), nullable=False, index=True)
    connection_id = Column(UUID(as_uuid=True), ForeignKey("snowflake_connections.id"), nullable=False, index=True)
    last_processed_at = Column(DateTime(timezone=True), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    # Relationships
    organization = relationship("Organization", foreign_keys=[org_id], backref="lineage_load_watermark_org_id")
    batch = relationship("SnowflakeQueryRecord", foreign_keys=[batch_id], viewonly=True)
    connection = relationship("SnowflakeConnection", foreign_keys=[connection_id], backref="lineage_watermarks_conn_id")
      

