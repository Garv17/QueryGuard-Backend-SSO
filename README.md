# QueryGuardAI Backend

A FastAPI-based backend for QueryGuardAI - a data lineage and impact analysis tool for Snowflake queries.

## Features

- User authentication with JWT tokens
- Database integration with PostgreSQL
- RESTful API endpoints for auth operations
- Secure password hashing and token management

## Setup

### Prerequisites

- Python 3.8+
- PostgreSQL database
- pip

### Installation

1. Clone the repository and navigate to the main directory:
```bash
cd main
```

2. Create a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up environment variables:
```bash
export DATABASE_URL="postgresql+psycopg2://username:password@localhost:5432/queryguard"
export SECRET_KEY="your-secret-key-here"
```

5. Create the database:
```bash
createdb queryguard
```

6. Run the application:
```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

## API Endpoints

### Authentication

- `POST /auth/signup` - Register a new user (requires valid org_id)
- `POST /auth/login` - Login and get JWT token
- `POST /auth/forgot-password` - Generate password reset token
- `POST /auth/reset-password` - Reset password with token
- `POST /auth/logout` - Logout and revoke JWT
- `GET /auth/me` - Get current user information

### Organizations (Admin Only)

- `POST /organizations` - Create a new organization
- `GET /organizations` - List all organizations
- `GET /organizations/{org_id}` - Get organization details
- `PUT /organizations/{org_id}` - Update organization
- `DELETE /organizations/{org_id}` - Deactivate organization

### Snowflake Management

- `POST /snowflake/test-connection` - Test Snowflake connection
- `POST /snowflake/save-connection` - Save Snowflake connection (includes cron expression)
- `GET /snowflake/connections` - List all connections for organization
- `GET /snowflake/fetch-databases/{connection_id}` - Fetch databases from Snowflake
- `GET /snowflake/fetch-schemas/{connection_id}/{database_name}` - Fetch schemas for database
- `POST /snowflake/save-database-selection` - Save database selections
- `POST /snowflake/save-schema-selection` - Save schema selections
- `GET /snowflake/selected-databases/{connection_id}` - Get selected databases
- `GET /snowflake/selected-schemas/{connection_id}/{database_name}` - Get selected schemas

### GitHub App Management

- `GET /github/install?org_id={org_id}` - Redirect to GitHub App installation
- `GET /github/callback` - Handle GitHub App installation callback
- `GET /github/installations` - List all installations for organization
- `GET /github/repositories/{installation_id}` - List repositories for installation
- `POST /github/sync-repositories/{installation_id}` - Sync repositories (manual trigger)
- `DELETE /github/installations/{installation_id}` - Deactivate installation
- `POST /github/webhook` - Handle GitHub webhook events (PR events)
- `POST /github/process-pr` - Process PR changes and add comment

### Health Check

- `GET /` - API information
- `GET /health` - Health check endpoint

## Database Schema

### Organizations Table
- `id` (UUID) - Primary key
- `name` (String) - Organization name
- `is_active` (Boolean) - Organization status
- `created_at` (DateTime) - Creation timestamp
- `updated_at` (DateTime) - Last update timestamp

### Users Table
- `id` (UUID) - Primary key
- `username` (String) - Unique username
- `email` (String) - Unique email
- `password_hash` (String) - Hashed password
- `org_id` (UUID) - Foreign key to organizations
- `password_reset_otp` (String) - Password reset OTP (6 digits)
- `reset_otp_expires` (DateTime) - OTP expiry
- `is_active` (Boolean) - User status
- `created_at` (DateTime) - Creation timestamp
- `updated_at` (DateTime) - Last update timestamp

### User Tokens Table
- `id` (UUID) - Primary key
- `user_id` (UUID) - Foreign key to users
- `token` (Text) - JWT token
- `expires_at` (DateTime) - Token expiry
- `is_revoked` (Boolean) - Token status
- `created_at` (DateTime) - Creation timestamp

### Snowflake Connections Table
- `id` (UUID) - Primary key
- `org_id` (UUID) - Foreign key to organizations
- `connection_name` (String) - Connection name
- `account` (String) - Snowflake account
- `username` (String) - Snowflake username
- `password` (String) - Snowflake password
- `warehouse` (String) - Snowflake warehouse
- `cron_expression` (String) - Mining schedule
- `is_active` (Boolean) - Connection status
- `created_at` (DateTime) - Creation timestamp
- `updated_at` (DateTime) - Last update timestamp

### Snowflake Databases Table
- `id` (UUID) - Primary key
- `connection_id` (UUID) - Foreign key to snowflake_connections
- `database_name` (String) - Database name
- `is_selected` (Boolean) - Selection status
- `created_at` (DateTime) - Creation timestamp

### Snowflake Schemas Table
- `id` (UUID) - Primary key
- `database_id` (UUID) - Foreign key to snowflake_databases
- `schema_name` (String) - Schema name
- `is_selected` (Boolean) - Selection status
- `created_at` (DateTime) - Creation timestamp

### GitHub Installations Table
- `id` (UUID) - Primary key
- `installation_id` (String) - GitHub installation ID
- `org_id` (UUID) - Foreign key to organizations
- `account_type` (String) - User or Organization
- `account_login` (String) - GitHub username/org name
- `repository_selection` (String) - all or selected
- `permissions` (Text) - JSON string of permissions
- `events` (Text) - JSON string of events
- `is_active` (Boolean) - Installation status
- `created_at` (DateTime) - Creation timestamp
- `updated_at` (DateTime) - Last update timestamp

### GitHub Repositories Table
- `id` (UUID) - Primary key
- `installation_id` (UUID) - Foreign key to github_installations
- `repo_id` (String) - GitHub repository ID
- `repo_name` (String) - Repository name
- `full_name` (String) - Full repository name (owner/repo)
- `private` (Boolean) - Repository visibility
- `description` (Text) - Repository description
- `default_branch` (String) - Default branch name
- `created_at` (DateTime) - Creation timestamp
- `updated_at` (DateTime) - Last update timestamp

## Development

The application uses SQLAlchemy for database operations and JWT for authentication. The database models are defined in `app/utils/models.py` and the authentication logic is in `app/api/auth.py`.

## GitHub App Setup

### Required Permissions
- `contents: read` - Read repository contents for PR file changes
- `pull_requests: read` - Read pull request events and data
- `metadata: read` - Read repository metadata

### Required Events
- `pull_request` - Receive events when PRs are opened, closed, reopened, etc.

### Configuration
Update the following values in `app/api/github.py`:
- `GITHUB_APP_URL` - Your GitHub App's installation URL
- `CALLBACK_URL` - Your backend's callback URL for installations
- `WEBHOOK_SECRET` - Your GitHub App's webhook secret for signature verification

### Installation Flow
1. Customer signs into your SaaS (gets org_id)
2. Frontend redirects to: `GET /github/install?org_id={org_id}`
3. Backend redirects to GitHub App installation with state parameter
4. Customer installs app on their repository/organization
5. GitHub redirects back to callback with installation details
6. Backend validates state (org_id) and stores installation data

### Security Notes
- Only installations with valid state parameter (org_id) are processed
- Installations without state are ignored (prevents unauthorized installations)
- All operations are scoped to the user's organization
- Webhook signatures are verified to ensure requests come from GitHub

### Webhook Events
The webhook endpoint processes the following events:
- `pull_request` events with actions: `opened`, `reopened`
- Automatically extracts PR information for downstream processing
- Verifies webhook signature for security

### PR Processing
The PR processing endpoint:
- Validates installation ownership
- Adds comment "Changes Processed By Query Guard AI" to PR
- Requires proper GitHub App access token (needs JWT implementation)

## Security Notes

- Passwords are hashed using SHA-256
- JWT tokens are stored in the database for revocation capability
- Password reset OTPs expire after 60 minutes
- All sensitive operations require authentication
