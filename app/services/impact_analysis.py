from typing import List, Dict, Any, Optional, Set
from pydantic import BaseModel
from langchain_community.document_loaders.csv_loader import CSVLoader
from langchain_community.vectorstores import Chroma
from langchain_google_genai import GoogleGenerativeAIEmbeddings, ChatGoogleGenerativeAI
from langchain.schema import Document
from langchain.chains import RetrievalQA
from datetime import datetime
import psycopg2
import psycopg2.extras
from psycopg2.extras import Json
import os
import json
import logging
import re


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


VECTOR_STORE_DIR = os.getenv("VECTOR_STORE_DIR", "chroma_collection_setup")
LINEAGE_CSV_PATH = os.getenv("LINEAGE_CSV_PATH", "temp_lineage_data/lineage_output_deep.csv")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")
 
# SQLAlchemy models for storing PR analysis in first-class tables
from sqlalchemy.orm import Session
from app.utils.models import GitHubPullRequestAnalysis, GitHubRepository, GitHubInstallation, DbtManifestNode


embedding = GoogleGenerativeAIEmbeddings(model="models/gemini-embedding-001", google_api_key=GOOGLE_API_KEY)
LLM = ChatGoogleGenerativeAI(model="gemini-2.0-flash", google_api_key=GOOGLE_API_KEY, temperature=0.2)


def get_model_metadata(db: Session, file_path: str, org_id: str) -> Optional[Dict[str, Any]]:
    """
    Retrieve dbt model metadata from the dbt_manifest_node table by file path.
    
    Args:
        db: SQLAlchemy database session
        file_path: The original file path to search for
        org_id: Organization ID to filter by
        
    Returns:
        Dictionary containing the model metadata if found, None otherwise
    """
    try:
        # Validate input parameters
        if not file_path or not org_id:
            logger.warning(f"Invalid parameters: file_path={file_path}, org_id={org_id}")
            return None
        
        # Query the dbt_manifest_node table for the specific file path and org
        node = db.query(DbtManifestNode).filter(
            DbtManifestNode.org_id == org_id,
            DbtManifestNode.original_file_path == file_path
        ).first()
        
        if node:
            # Convert the SQLAlchemy model to a dictionary
            return {
                "id": str(node.id),
                "org_id": str(node.org_id),
                "connection_id": str(node.connection_id),
                "run_id": node.run_id,
                "unique_id": node.unique_id,
                "database": node.database,
                "schema": node.schema,
                "name": node.name,
                "package_name": node.package_name,
                "path": node.path,
                "original_file_path": node.original_file_path,
                "resource_type": node.resource_type,
                "raw_code": node.raw_code,
                "compiled_code": node.compiled_code,
                "downstream_models": node.downstream_models,
                "last_successful_run_at": node.last_successful_run_at.isoformat() if node.last_successful_run_at else None,
                "synced_at": node.synced_at.isoformat() if node.synced_at else None
            }
        return None
    except Exception as e:
        logger.error(f"Error retrieving model metadata for file_path {file_path}: {str(e)}")
        return None


# # We are assuming that we have created a vector sotre 
# def init_vector_store() -> Chroma:
#     if os.path.exists(VECTOR_STORE_DIR):
#         db = Chroma(persist_directory=VECTOR_STORE_DIR, embedding_function=embedding)
#         logger.info("Loaded existing Chroma vector store")
#     else:
#         loader = CSVLoader(LINEAGE_CSV_PATH)
#         docs = loader.load()
#         db = Chroma.from_documents(docs, embedding, persist_directory=VECTOR_STORE_DIR)
#         db.persist()
#         logger.info("Created and persisted new Chroma vector store")
#     return db




def get_org_vector_store(org_id: str) -> Chroma:
    """
    Returns a Chroma vector store bound to a specific org collection.
    """
    collection_name = f"org_{org_id}"
    db = Chroma(
        collection_name=collection_name,
        persist_directory=VECTOR_STORE_DIR,
        embedding_function=embedding,
    )
    return db


def init_org_vector_store(org_id: str, csv_path: str = None) -> Chroma:
    """
    Initialize or update an org-specific collection.
    Optionally bootstrap with CSV data.
    """
    db = get_org_vector_store(org_id)

    if csv_path:  # bootstrap docs into the org collection
        loader = CSVLoader(csv_path)
        docs = loader.load()
        db.add_documents(docs)
        db.persist()
        print(f"Loaded {len(docs)} docs into collection for org {org_id}")

    return db


def get_retriever(org_id: str, k: int = 8):
    db = get_org_vector_store(org_id)
    return db.as_retriever(search_kwargs={"k": k})


def get_qa_chain(org_id: str, k: int = 5):
    retriever = get_retriever(org_id, k=k)
    qa_chain = RetrievalQA.from_chain_type(
        llm=LLM,
        retriever=retriever,
        return_source_documents=True,
    )
    return qa_chain


# Removed global, hardcoded retriever/qa_chain initialization.
# Use get_retriever(org_id) and get_qa_chain(org_id) dynamically per request.


def get_db_connection():
    return psycopg2.connect(DATABASE_URL)


def fetch_queries(query_ids: List[str]) -> List[Dict]:
    if not query_ids:
        return []
    with get_db_connection() as conn:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        placeholders = ",".join(["%s"] * len(query_ids))
        cursor.execute(
            f"""
            SELECT query_id, query_text FROM "snowflake_query_history"
            WHERE query_id IN ({placeholders})
        """,
            query_ids,
        )
        return cursor.fetchall()


def store_pr_analysis(
    db: Session,
    *,
    org_id: str,
    installation_id_str: str,
    repo_full_name: str,
    pr_number: int,
    pr_title: Optional[str],
    pr_description: Optional[str] = None,
    branch_name: Optional[str] = None,
    author_name: Optional[str] = None,
    pr_url: Optional[str] = None,
    total_impacted_queries: Optional[int] = None,
    analysis_data: Dict,
) -> str:
    """
    Persist PR analysis using SQLAlchemy model `GitHubPullRequestAnalysis` and link to
    related GitHub entities.

    Returns the created analysis UUID as string.
    """
    # Resolve installation row (by external installation id string)
    installation = (
        db.query(GitHubInstallation)
        .filter(GitHubInstallation.installation_id == installation_id_str)
        .first()
    )
    if not installation:
        raise ValueError("Installation not found for storing PR analysis")

    # Try to resolve repository row by full_name under this installation
    repository = (
        db.query(GitHubRepository)
        .filter(
            GitHubRepository.installation_id == installation.id,
            GitHubRepository.full_name == repo_full_name,
        )
        .first()
    )

    # If total_impacted_queries not provided, calculate from analysis_data
    if total_impacted_queries is None:
        all_query_ids = set()
        files = analysis_data.get("files", [])
        for file_data in files:
            affected_ids = file_data.get("affected_query_ids", [])
            all_query_ids.update(affected_ids)
        total_impacted_queries = len(all_query_ids)

    analysis = GitHubPullRequestAnalysis(
        org_id=installation.org_id,
        installation_id=installation.id,
        repository_id=repository.id if repository else None,
        repo_full_name=repo_full_name,
        pr_number=pr_number,
        pr_title=pr_title,
        pr_description=pr_description,
        branch_name=branch_name,
        author_name=author_name,
        pr_url=pr_url,
        total_impacted_queries=total_impacted_queries,
        analysis_data=analysis_data,
    )
    db.add(analysis)
    db.commit()
    db.refresh(analysis)
    return str(analysis.id)


# -----------------------------
# Prompts for Schema changes
# -----------------------------
PLAN_PROMPT = """
You are a metadata-aware lineage analyst. You will receive a SQL/schema change and a set of context snippets from a lineage knowledge base (CSV rows embedded in a vector store). 

Your task:
1) Infer the impacted entities (tables/columns) based on the change and snippets.
2) Suggest next queries to expand downstream dependencies (multi-hop), if any.
3) Return a STRICT JSON as text with the following structure:
       "found_entities": ["schema.table.column", ...], 
       "next_queries": ["..."], 
       "notes": "..."

- If no further queries are useful, return an empty list for "next_queries".
- Prefer exact entity strings from the snippets for next queries (e.g., "edw_staging.allocationrule.departmentlist").

SQL/Schema Change:
{change}

Context Snippets (may be empty):
{snippets}

Respond only with JSON text, do not include any extra explanation.
"""

FINAL_REPORT_PROMPT = """
You are a metadata-aware assistant tasked with generating a **complete multi-hop downstream impact analysis report**.

You are given:
1. The SQL/schema change
2. ALL retrieved lineage context (multi-hop) as raw snippets

Your task:
- Traverse the lineage graph recursively (level by level) using the snippets.
- Include ALL downstream assets until no further dependencies remain (not just direct neighbors).
- Group impacts by **depth level** (1-hop, 2-hop, 3-hop, etc.).
- Collect impacted query IDs and metadata.

---

Your output must be valid JSON with these keys:
{{
  "impact_report": "<the full Markdown report, format attached below>",
  "affected_query_ids": ["q1", "q2", ...],
  "source_metadata": [
    {{
      "target_database": "...",
      "target_schema": "...",
      "target_table": "...",
      "target_column": "..."
    }}
  ]
}}

---

### 📑 Impact Report Markdown format

📌 **Change Summary**

Change summary description: _(Explain why this change may have an impact downstream based on source-to-target dependencies.)_

| Field                  | Description |
|------------------------|-------------|
| Change Type            | e.g., Add/Drop/Alter Column |
| Affected Table         | <database.schema.table> |
| Affected Column(s)     | <column name(s)> |
| Requested Change       | <exact change or best-effort> |
| Reason for Change      | <reason if known, else N/A> |

---

### **Downstream Impact Analysis**

_List ALL impacted downstream targets grouped by depth._

#### Depth 1 (direct dependencies):
1. **Target Database:** ...
   **Target Schema:** ...
   **Target Table:** ...
   **Target Column:** ...

#### Depth 2:
1. ...

#### Depth 3:
1. ...

(Continue until no more dependencies)

---

**Explanation:**
- Describe clearly how the change propagates through ALL levels (up to the deepest retrieved). 
- Mention necessary updates (views, ETL, dashboards, schema enforcement, SELECT * risks, etc.).

---

Details:
- `affected_query_ids`: Collect all query IDs seen in snippets for ALL impacted assets.
- `source_metadata`: Extract structured metadata for each impacted asset at any depth.
- Ensure the JSON is strictly valid (no trailing commas, no comments, no markdown around it).
- Do not hallucinate. If a depth has no entries, omit it.
- STOP ONLY when all retrieved snippets have been exhausted.

---

SQL/Schema Change:
{change}

ALL Retrieved Snippets:
{snippets}
"""


# -----------------------------
# Prompts for DBT model changes
# -----------------------------
dbt_PLAN_PROMPT = """
You are a metadata-aware lineage analyst. You will receive a json data, which explains change in dbt model logic and their impacted columns in target. 
sample/example json data :
{{'impact': [{{'change': "Modified the allocationruledesc column to append '_UPDATED' when allocationrulekey is 1, otherwise keep the original value.",
   'explanation': 'This change introduces a conditional update to the allocationruledesc column based on the allocationrulekey. This is important because it alters the data content of this column based on a specific condition, which might affect downstream processes or reports that rely on the original value or a specific pattern in this column.',
   'impacted_columns': ['PROD_TZ.EDW_STAGING_EDW.DIMSHAREDSERVICESALLOCATIONRULE.allocationruledesc']}},
  {{'change': "Modified the payrollbasis column to be 'NEW_PAYROLL_BASIS' when allocationrulekey is 1, otherwise keep the original value.",
   'explanation': 'This change introduces a conditional update to the payrollbasis column based on the allocationrulekey. This means payrollbasis will have a new value for allocationrulekey = 1 and this change might impact any process that depends on the original value of payrollbasis',
   'impacted_columns': ['PROD_TZ.EDW_STAGING_EDW.DIMSHAREDSERVICESALLOCATIONRULE.payrollbasis']}}]}}

Your task:
1) Go through impacted_columns fields in json, Infer the impacted entities (tables/columns).
2) Suggest next queries to expand downstream dependencies (multi-hop), if any.
3) Return a STRICT JSON as text with the following structure:
       "found_entities": ["schema.table.column", ...], 
       "next_queries": ["..."], 
       "notes": "..."

- If no further queries are useful, return an empty list for "next_queries".
- Prefer exact entity strings from the snippets for next queries (e.g., "edw_staging.allocationrule.departmentlist").

json data:
{safe_json_text}


Respond only with JSON text, do not include any extra explanation.
"""


dbt_FINAL_REPORT_PROMPT = """
You are a metadata-aware assistant tasked with generating a **complete multi-hop downstream impact analysis report**.


You are given:
1. A json data, which explains change in dbt model logic and their impacted columns in target.
2. Model metadata : which gives the information about which table is materilized and any downstream models are there.
2. ALL retrieved lineage context (multi-hop) as raw snippets based on impacted target columns received in json data.

--
json data:
{safe_json_text}

ALL Retrieved Snippets:
{snippets}

--

Your task:
- Traverse the lineage graph recursively (level by level) using the snippets.
- Include ALL downstream assets until no further dependencies remain (not just direct neighbors).
- Group impacts by **depth level** (1-hop, 2-hop, 3-hop, etc.).
- Collect impacted query IDs and metadata.
- For each change, explicitly trace: **code change → affected column → downstream propagation → explanation**.

---

Your output must be valid JSON with these keys:
{{
  "impact_report": "<the full Markdown report, format attached below>",
  "affected_query_ids": ["q1", "q2", ...],
  "source_metadata": [
    {{
      "target_database": "...",
      "target_schema": "...",
      "target_table": "...",
      "target_column": "..."
    }}
  ]
}}

---

### 📑 Impact Report Markdown format

📌 **Change Summary**

Change summary description: _(Explain why this transformation may impact downstream based on source-to-target dependencies.)_

| Field                  | Description |
|------------------------|-------------|
| Change Type            | e.g., Transformation Logic Update |
| Affected Table         | <database.schema.table> |
| Affected Column(s)     | <column name(s)> |
| Requested Change       | <exact code modification> |
| Reason for Change      | <reason if known, else N/A> |

---

### **Change-to-Impact Mapping**

For **each detected change**, show the chain of impact:

#### 🔄 Change 1
```sql
Old: 1 - ratio  
New: cast(0.95 * (ratio) as decimal(7,6))
```
Impacted Column: corporateservices (in STAGING.legalallocation)
Impact Chain for target table:

allocationtype.column_name → allocationrule_upsert.column_name → target.column_name

target to downstream chain: 
target.col_name → downstream_table.column_name

Impacted Downstream DBT Model : 

Explanation:


#### 🔄 Change 2
```sql
Old: 1 - ratio  
New: cast(0.95 * (ratio) as decimal(7,6))
```
Impacted Column: corporateservices (in STAGING.legalallocation)
Impact Chain for target table:

allocationtype.column_name → allocationrule_upsert.column_name → target.column_name

target to downstream chain: 
target.col_name → downstream_table.column_name

Explanation:

### **Downstream Impact Analysis (Grouped by Depth)** ###
Depth 1 (direct dependencies):

Target Table: STAGING.legalallocation
Impacted Columns: corporateservices, allocationtype, payrollbasis
explanation: 

Depth 2:

Target Table: STAGING.allocationrule_upsert
Impacted Columns: corporateservices, allocationtype, payrollbasis

Depth 3:

Target Table: STAGING.finalsource
Impacted Columns: corporateservices, allocationtype, payrollbasis
"""



def _docs_to_snippets(docs: List[Document]) -> str:
    return "\n".join(d.page_content.strip() for d in docs)


def _safe_json_parse(txt: str) -> Dict[str, Any]:
    try:
        return json.loads(txt)
    except Exception:
        start = txt.find("{")
        end = txt.rfind("}")
        if start != -1 and end != -1 and end > start:
            try:
                return json.loads(txt[start : end + 1])
            except Exception:
                pass
        return {}


class IterativeConfig(BaseModel):
    max_iters: int = 5
    k_per_query: int = 8
    dedupe: bool = True


def schema_detection_rag(change_text: str, org_id: str, cfg: Optional[IterativeConfig] = None):
    cfg = cfg or IterativeConfig()
    used_queries: List[str] = []
    collected_docs: List[Document] = []
    seen_texts: Set[str] = set()
    frontier: List[str] = [change_text]
    retriever = get_retriever(org_id, k=cfg.k_per_query)

    for _ in range(cfg.max_iters):
        new_docs: List[Document] = []
        for q in frontier:
            used_queries.append(q)
            docs = retriever.get_relevant_documents(q)
            try:
                logger.info(
                    "lineage.retrieval - org=%s query_len=%d docs=%d",
                    org_id,
                    len(q or ""),
                    len(docs or []),
                )
                for idx, d in enumerate(docs[:3]):
                    preview = (d.page_content or "").strip().replace("\n", " ")[:300]
                    logger.info("lineage.retrieval.preview[%d]: %s", idx, preview)
            except Exception:
                logger.debug("lineage.retrieval - logging failed for query preview")
            for d in docs:
                if not cfg.dedupe or d.page_content not in seen_texts:
                    seen_texts.add(d.page_content)
                    new_docs.append(d)
                    collected_docs.append(d)
        if not new_docs:
            break
        snippets_text = _docs_to_snippets(new_docs)
        plan = LLM.invoke(PLAN_PROMPT.format(change=change_text, snippets=snippets_text))
        plan_text = plan.content if hasattr(plan, "content") else str(plan)
        plan_json = _safe_json_parse(plan_text)
        next_queries: List[str] = plan_json.get("next_queries", []) or []
        frontier = [q for q in next_queries if q and q not in used_queries]
        if not frontier:
            break

    all_snippets_text = _docs_to_snippets(collected_docs) if collected_docs else "(no snippets retrieved)"
    final = LLM.invoke(FINAL_REPORT_PROMPT.format(change=change_text, snippets=all_snippets_text))
    final_text = final.content if hasattr(final, "content") else str(final)
    final_json_response = _safe_json_parse(final_text)
    return final_json_response


def dbt_model_detection_rag(code_changes: str, file_path: str, org_id: str, db: Session, cfg: Optional[IterativeConfig] = None):
    def get_dbt_impact(code_changes_inner: str, file_path_inner: str):
        model_metadata = get_model_metadata(db, file_path_inner, org_id)
        query = f"""You are DBT model specialised anlayser. You are provided with these things:
        1) Change in dbt model, basically diff text like this (below is just example of how we get the diff text)
        Example :
        File: models/EDW/DIMSHAREDSERVICESALLOCATIONRULE.sql (modified) [+10/-2]
        @@ -8,7 +8,11 @@ with source_allocationrule as (
        ...
        2) Lineage information (source columns and its target columns with DBT file path information, query id informaiton) is provided through vector store embedded
        3) compiled sql code through dbt manifest.json file, which gives you broader context about complete model.
        4) Dbt Model metadata like which database, schema and table it materilize.
        Your tasks :
        1. so based on above three points which is additional context to you, you need to analyse which are columns that will be impacted in target table mapping change that we are reciving.
        2. once analysed you need to give output in striclty in stuctured json format like this. no other text, only json.
        {{
        "impact": [
            {{
            "change": "describe the specific change in logic from the diff",
            "explanation": "explain what this change does and why it matters",
            "impacted_columns": [
                "database.schema.table.column2"
            ]
            }}
        ]
        }}
        --------------------------- information you recieved -------------------
        dbt model change {code_changes_inner}
        dbt model metadata {model_metadata}
        """
        qa_chain_local = get_qa_chain(org_id, k=cfg.k_per_query) if cfg else get_qa_chain(org_id)
        result = qa_chain_local.invoke({"query": query})
        safe_json = _safe_json_parse(result["result"]) if isinstance(result, dict) and "result" in result else {}
        impacted_columns = [col for impact in safe_json.get("impact", []) for col in impact.get("impacted_columns", [])]
        return {"safe_json": safe_json, "impacted_columns": impacted_columns}

    intermediate_resultset = get_dbt_impact(code_changes, file_path)
    impact_json_data = intermediate_resultset["safe_json"]
    impact_columns = intermediate_resultset["impacted_columns"]

    cfg = cfg or IterativeConfig()
    used_queries: List[str] = []
    collected_docs: List[Document] = []
    seen_texts: Set[str] = set()
    frontier: List[str] = impact_columns
    retriever = get_retriever(org_id, k=cfg.k_per_query)

    for _ in range(cfg.max_iters):
        new_docs: List[Document] = []
        for q in frontier:
            used_queries.append(q)
            docs = retriever.get_relevant_documents(q)
            try:
                logger.info(
                    "dbt.retrieval - org=%s query_len=%d docs=%d",
                    org_id,
                    len(q or ""),
                    len(docs or []),
                )
                for idx, d in enumerate(docs[:3]):
                    preview = (d.page_content or "").strip().replace("\n", " ")[:300]
                    logger.info("dbt.retrieval.preview[%d]: %s", idx, preview)
            except Exception:
                logger.debug("dbt.retrieval - logging failed for query preview")
            for d in docs:
                if not cfg.dedupe or d.page_content not in seen_texts:
                    seen_texts.add(d.page_content)
                    new_docs.append(d)
                    collected_docs.append(d)
        if not new_docs:
            break
        snippets_text = _docs_to_snippets(new_docs)
        plan = LLM.invoke(dbt_PLAN_PROMPT.format(safe_json_text=impact_json_data, snippets=snippets_text))
        plan_text = plan.content if hasattr(plan, "content") else str(plan)
        plan_json = _safe_json_parse(plan_text)
        next_queries: List[str] = plan_json.get("next_queries", []) or []
        frontier = [q for q in next_queries if q and q not in used_queries]
        if not frontier:
            break

    all_snippets_text = _docs_to_snippets(collected_docs) if collected_docs else "(no snippets retrieved)"
    final = LLM.invoke(dbt_FINAL_REPORT_PROMPT.format(safe_json_text=impact_json_data, snippets=all_snippets_text))
    final_text = final.content if hasattr(final, "content") else str(final)
    final_json_response = _safe_json_parse(final_text)
    return final_json_response


