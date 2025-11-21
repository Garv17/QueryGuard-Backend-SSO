from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel
from typing import List
from app.utils.auth_deps import get_current_user
from app.utils.models import User
import logging

router = APIRouter(prefix="/overview-dashboard", tags=["Overview Dashboard"])
logger = logging.getLogger("overview_dashboard")

# --- Static Data ---
STATIC_DASHBOARD_DATA = {
    "summary": {
        "total_prs": 15,
        "impacted_queries": 53
    },
    "pull_requests": [
        {
            "pr_id": "PR-1024",
            "title": "Remove deprecated column dimhomestatusoccupancywid",
            "description": "Cleaning up unused database column to optimize schema and improve query performance",
            "branch_name": "feature/remove-deprecated-column",
            "repository_name": "data-warehouse",
            "author_name": "john.doe",
            "submitted_at": "2025-11-21T18:30:00Z",
            "total_impacted_queries": 5
        },
        {
            "pr_id": "PR-1023",
            "title": "Add new customer segmentation table",
            "description": "New table for customer analytics and segmentation",
            "branch_name": "feature/customer-segments",
            "repository_name": "analytics-dbt",
            "author_name": "jane.smith",
            "submitted_at": "2025-11-20T14:10:00Z",
            "total_impacted_queries": 12
        },
        {
            "pr_id": "PR-1022",
            "title": "Optimize revenue forecast materialized view",
            "description": "Improved query and index strategy for forecast models",
            "branch_name": "feature/optimize-forecast-mv",
            "repository_name": "finance-models",
            "author_name": "alex.wilson",
            "submitted_at": "2025-11-19T10:45:00Z",
            "total_impacted_queries": 4
        },
        {
            "pr_id": "PR-1021",
            "title": "Fix data quality rule failures for lease_pipeline",
            "description": "Resolved incorrect null-handling and updated validation logic",
            "branch_name": "fix/lease-pipeline-dq",
            "repository_name": "data-quality",
            "author_name": "sara.kim",
            "submitted_at": "2025-11-18T09:20:00Z",
            "total_impacted_queries": 3
        },
        {
            "pr_id": "PR-1020",
            "title": "Refactor ETL job for nightly refresh",
            "description": "Separated transformation stages and improved error handling",
            "branch_name": "refactor/nightly-etl",
            "repository_name": "etl-jobs",
            "author_name": "mike.anderson",
            "submitted_at": "2025-11-17T16:05:00Z",
            "total_impacted_queries": 7
        },
        {
            "pr_id": "PR-1019",
            "title": "Add new KPI metrics for portfolio dashboard",
            "description": "Introduced occupancy efficiency and revenue-per-unit metrics",
            "branch_name": "feature/new-kpi-metrics",
            "repository_name": "analytics-dbt",
            "author_name": "emma.brown",
            "submitted_at": "2025-11-17T11:30:00Z",
            "total_impacted_queries": 6
        },
        {
            "pr_id": "PR-1018",
            "title": "Remove unused staging models",
            "description": "Cleaned up legacy staging models no longer referenced",
            "branch_name": "cleanup/remove-staging-models",
            "repository_name": "data-warehouse",
            "author_name": "li.chen",
            "submitted_at": "2025-11-16T13:40:00Z",
            "total_impacted_queries": 2
        },
        {
            "pr_id": "PR-1017",
            "title": "Improve SLA tracking logic",
            "description": "Updated SLA start/end timestamps and fixed aggregation bug",
            "branch_name": "feature/improve-sla-logic",
            "repository_name": "service-analytics",
            "author_name": "daniel.ramirez",
            "submitted_at": "2025-11-15T15:55:00Z",
            "total_impacted_queries": 5
        },
        {
            "pr_id": "PR-1016",
            "title": "Add index and partitioning to events table",
            "description": "Query performance improved by optimizing column distribution",
            "branch_name": "feature/events-indexing",
            "repository_name": "event-pipeline",
            "author_name": "chloe.martin",
            "submitted_at": "2025-11-14T08:25:00Z",
            "total_impacted_queries": 4
        },
        {
            "pr_id": "PR-1015",
            "title": "Introduce audit logging for ETL failures",
            "description": "Added structured logs and enhanced monitoring alerts",
            "branch_name": "feature/etl-audit-logging",
            "repository_name": "etl-jobs",
            "author_name": "robert.hughes",
            "submitted_at": "2025-11-13T19:10:00Z",
            "total_impacted_queries": 5
        },
        {
            "pr_id": "PR-1014",
            "title": "Fix incorrect date truncation in monthly snapshot",
            "description": "Corrected time zone and rounding issues",
            "branch_name": "fix/monthly-snapshot-date",
            "repository_name": "analytics-core",
            "author_name": "olivia.scott",
            "submitted_at": "2025-11-12T17:00:00Z",
            "total_impacted_queries": 3
        }
    ]
}


# --- Response Models ---
class PullRequestSummary(BaseModel):
    pr_id: str
    title: str
    description: str
    branch_name: str
    repository_name: str
    author_name: str
    submitted_at: str
    total_impacted_queries: int


class DashboardSummary(BaseModel):
    total_prs: int
    impacted_queries: int


class DashboardOverviewResponse(BaseModel):
    summary: DashboardSummary
    pull_requests: List[PullRequestSummary]


# --- Endpoints ---
@router.get("/", response_model=DashboardOverviewResponse)
def get_dashboard_overview(
    current_user: User = Depends(get_current_user),
    request: Request = None
):
    """
    Get dashboard overview data with KPIs and pull request information.
    Accessible by any authenticated user (any role).
    """
    logger.info("/overview-dashboard - request by user_id=%s username=%s", 
                current_user.id, current_user.username)
    
    # Return static data embedded in the file
    data = STATIC_DASHBOARD_DATA
    
    # Convert to response model
    response = DashboardOverviewResponse(
        summary=DashboardSummary(
            total_prs=data["summary"]["total_prs"],
            impacted_queries=data["summary"]["impacted_queries"]
        ),
        pull_requests=[
            PullRequestSummary(
                pr_id=pr["pr_id"],
                title=pr["title"],
                description=pr["description"],
                branch_name=pr["branch_name"],
                repository_name=pr["repository_name"],
                author_name=pr["author_name"],
                submitted_at=pr["submitted_at"],
                total_impacted_queries=pr["total_impacted_queries"]
            )
            for pr in data["pull_requests"]
        ]
    )
    
    logger.info("/overview-dashboard - returning data with %d PRs", 
               len(response.pull_requests))
    return response

