from langchain_google_genai import GoogleGenerativeAIEmbeddings, ChatGoogleGenerativeAI
import os
from dotenv import load_dotenv
load_dotenv()
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
VECTOR_STORE_DIR = os.getenv("VECTOR_STORE_DIR", "chroma_collection_setup")
LINEAGE_CSV_PATH = os.getenv("LINEAGE_CSV_PATH", "temp_lineage_data/lineage_output_deep.csv")

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
