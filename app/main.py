from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from app.database import init_db
from app.api import auth, organizations, snowflake, github, jira, impact
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("app")

app = FastAPI(
    title="QueryGuardAI Backend",
    description="Backend API for QueryGuardAI - Data Lineage and Impact Analysis Tool",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure this properly for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(auth.router)
app.include_router(organizations.router)
app.include_router(snowflake.router)
app.include_router(github.router)
app.include_router(jira.router)
app.include_router(impact.router)

@app.on_event("startup")
async def startup_event():
    """Initialize database on startup"""
    logger.info("Application startup: initializing database")
    init_db()
    logger.info("Database initialized")

@app.get("/")
async def root(request: Request):
    logger.info("GET / - Root endpoint called from %s", request.client.host if request.client else "unknown")
    return {"message": "QueryGuardAI Backend API", "version": "1.0.0"}

@app.get("/health")
async def health_check(request: Request):
    logger.debug("GET /health - Health check from %s", request.client.host if request.client else "unknown")
    return {"status": "healthy"}
