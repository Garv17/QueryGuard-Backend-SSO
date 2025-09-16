from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from typing import Optional
from app.services.impact_analysis import schema_detection_rag, dbt_model_detection_rag, fetch_queries


router = APIRouter(prefix="/simulate", tags=["Impact Simulation"])


class ImpactRequest(BaseModel):
    sql_change: str
    file_path: Optional[str] = None
    max_iters: Optional[int] = 5


@router.post("/schema_change_impact")
async def simulate_schema_impact(req: ImpactRequest):
    try:
        res = schema_detection_rag(req.sql_change)

        impact_report = res.get("impact_report", "")
        affected_query_ids = res.get("affected_query_ids", [])
        source_metadata = res.get("source_metadata", [])

        regression_queries = fetch_queries(affected_query_ids)

        return {
            "sql_change": req.sql_change,
            "impact_analysis": impact_report,
            "affected_query_ids": affected_query_ids,
            "regression_queries": regression_queries,
            "source_metadata": source_metadata,
        }

    except Exception:
        raise HTTPException(status_code=500, detail="Impact analysis failed")


@router.post("/dbt_model_change_impact")
async def simulate_dbt_model_impact(req: ImpactRequest):
    try:
        if not req.file_path:
            raise HTTPException(status_code=400, detail="file_path is required for DBT model analysis")

        res = dbt_model_detection_rag(req.sql_change, req.file_path)

        impact_report = res.get("impact_report", "")
        affected_query_ids = res.get("affected_query_ids", [])
        source_metadata = res.get("source_metadata", [])

        regression_queries = fetch_queries(affected_query_ids)

        return {
            "sql_change": req.sql_change,
            "impact_analysis": impact_report,
            "affected_query_ids": affected_query_ids,
            "regression_queries": regression_queries,
            "source_metadata": source_metadata,
        }

    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=500, detail="Impact analysis failed")


