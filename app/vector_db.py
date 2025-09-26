from langchain_google_genai import GoogleGenerativeAIEmbeddings, ChatGoogleGenerativeAI
import os
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional
import json
import psycopg2
import psycopg2.extras
from langchain.schema import Document

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
VECTOR_STORE_DIR = os.getenv("VECTOR_STORE_DIR", "chroma_collection_setup")
LINEAGE_CSV_PATH = os.getenv("LINEAGE_CSV_PATH", "temp_lineage_data/lineage_output_deep.csv")
DATABASE_URL = os.getenv("DATABASE_URL")

embedding = GoogleGenerativeAIEmbeddings(model="models/gemini-embedding-001", google_api_key=GOOGLE_API_KEY)
LLM = ChatGoogleGenerativeAI(model="gemini-2.0-flash", google_api_key=GOOGLE_API_KEY, temperature=0.2)


from langchain_community.vectorstores import Chroma
from langchain_community.document_loaders import CSVLoader
from langchain.chains import RetrievalQA


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


def get_db_connection():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL environment variable is not set")
    return psycopg2.connect(DATABASE_URL)


def _fetch_table_rows(
    table_name: str,
    columns: Optional[List[str]] = None,
    where_clause: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    selected_cols = ", ".join(columns) if columns else "*"
    query_parts: List[str] = [f"SELECT {selected_cols} FROM {table_name}"]
    if where_clause:
        query_parts.append(f"WHERE {where_clause}")
    if limit is not None and limit > 0:
        query_parts.append(f"LIMIT {limit}")
    query = " ".join(query_parts)

    with get_db_connection() as conn:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(query)
        rows = cursor.fetchall()
        return [dict(r) for r in rows]


def _row_to_document(row: Dict[str, Any], metadata_fields: Optional[List[str]] = None) -> Document:
    metadata_fields = metadata_fields or []
    # Prefer a structured, readable page content; fallback to JSON
    if any(k in row for k in [
        "source_database", "source_schema", "source_table", "source_column",
        "target_database", "target_schema", "target_table", "target_column",
    ]):
        parts: List[str] = []
        for key in [
            "source_database", "source_schema", "source_table", "source_column",
            "target_database", "target_schema", "target_table", "target_column",
            "query_id", "query_type", "dbt_model_file_path", "dependency_score",
        ]:
            if key in row and row[key] is not None:
                parts.append(f"{key}: {row[key]}")
        page_content = "\n".join(parts) if parts else json.dumps(row, default=str)
    else:
        page_content = json.dumps(row, default=str)

    metadata: Dict[str, Any] = {}
    for mkey in metadata_fields:
        if mkey in row:
            metadata[mkey] = row[mkey]
    return Document(page_content=page_content, metadata=metadata)


def init_org_vector_store_from_table(
    org_id: str,
    table_name: str,
    columns: Optional[List[str]] = None,
    where_clause: Optional[str] = None,
    limit: Optional[int] = None,
    id_field: Optional[str] = None,
    metadata_fields: Optional[List[str]] = None,
) -> Chroma:
    """
    Initialize or update an org-specific collection using rows from a database table.
    If the collection exists, rows will be added (duplicates depend on Chroma ID handling).
    """
    db = get_org_vector_store(org_id)
    rows = _fetch_table_rows(table_name=table_name, columns=columns, where_clause=where_clause, limit=limit)
    if not rows:
        return db

    docs: List[Document] = [_row_to_document(r, metadata_fields=metadata_fields) for r in rows]
    ids: Optional[List[str]] = None
    if id_field:
        tmp_ids: List[str] = []
        for r in rows:
            val = r.get(id_field)
            if val is not None:
                tmp_ids.append(str(val))
            else:
                tmp_ids.append(None)  # type: ignore
        # Only set ids if all are present
        if all(x is not None for x in tmp_ids):
            ids = [str(x) for x in tmp_ids]  # type: ignore

    if ids:
        db.add_documents(docs, ids=ids)
    else:
        db.add_documents(docs)
    db.persist()
    return db


def upsert_org_vector_store_from_table(
    org_id: str,
    table_name: str,
    id_field: str,
    columns: Optional[List[str]] = None,
    where_clause: Optional[str] = None,
    limit: Optional[int] = None,
    metadata_fields: Optional[List[str]] = None,
) -> Chroma:
    """
    Upsert semantics using a stable ID field. Existing IDs will be replaced.
    """
    db = get_org_vector_store(org_id)
    rows = _fetch_table_rows(table_name=table_name, columns=columns, where_clause=where_clause, limit=limit)
    if not rows:
        return db

    docs: List[Document] = [_row_to_document(r, metadata_fields=metadata_fields) for r in rows]
    ids: List[str] = [str(r[id_field]) for r in rows if id_field in r and r[id_field] is not None]
    if not ids or len(ids) != len(docs):
        # Fallback to simple add if IDs are not fully present
        db.add_documents(docs)
    else:
        # Chroma supports upsert via add with the same IDs; duplicates get replaced in 0.4+
        db.add_documents(docs, ids=ids)
    db.persist()
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
# # Initialize (bootstrap) a vector store for org_123 with CSV data
# DB = init_org_vector_store("123_org_1", LINEAGE_CSV_PATH)
