from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from app.vector_db import get_qa_chain
from app.api.auth import get_current_user
from app.utils.models import User
from app.tools import build_org_lineage_tool, build_org_query_history_tool, LLM
from app.services.impact_analysis import fetch_queries
import logging
from langchain.agents import initialize_agent, AgentType

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/chat", tags=["chat"])

class ChatMessage(BaseModel):
    role: str  # "user" or "assistant"
    content: str
    timestamp: Optional[str] = None

class ChatRequest(BaseModel):
    message: str
    k: Optional[int] = 5  # Number of documents to retrieve for context
    conversation_history: Optional[List[ChatMessage]] = []

class ChatResponse(BaseModel):
    response: str
    sources: List[Dict[str, Any]]
    conversation_id: Optional[str] = None
    impacted_query_ids: Optional[List[str]] = []
    impacted_queries: Optional[List[Dict[str, Any]]] = []

class ChatConversation(BaseModel):
    conversation_id: str
    org_id: str
    messages: List[ChatMessage]
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

@router.post("/query", response_model=ChatResponse)
async def chat_with_llm(request: ChatRequest, current_user: User = Depends(get_current_user)):
    """
    Chat with LLM using vector database context for a specific organization.
    
    Args:
        request: ChatRequest containing org_id, message, and optional parameters
        
    Returns:
        ChatResponse with LLM response and source documents
    """
    try:
        # Resolve organization strictly from authenticated user
        resolved_org_id = str(current_user.org_id)

        logger.info(f"Chat request for org_id: {resolved_org_id}, message: {request.message[:100]}...")

        # Prepare the query with conversation context if provided
        query = request.message
        if request.conversation_history:
            context_messages = [f"{msg.role}: {msg.content}" for msg in request.conversation_history[-5:]]
            if context_messages:
                context = "\n".join(context_messages)
                query = f"Previous conversation context:\n{context}\n\nCurrent question: {request.message}"

        # LLM classification: decide whether to use tools (lineage/impact) or respond conversationally (other)
        classify_prompt = (
            "You are a classifier. Decide if the user's message requires using specialized tools for data lineage (extract_lineage) "
            "or query impact analysis (query_history_search).\n"
            "Respond with exactly one word: lineage, impact, or other.\n\n"
            f"Message: {request.message}"
        )
        classification = LLM.invoke(classify_prompt)
        classification_label = (getattr(classification, "content", str(classification)) or "other").strip().lower()

        if classification_label not in {"lineage", "impact"}:
            # Conversational reply without tools
            persona_prompt = (
                "SYSTEM: You are Zane AI, a helpful assistant for data lineage and change-impact analysis.\n"
                "- Be concise.\n"
                "- Do NOT invent lineage or impacts without analysis.\n"
                "- If the user hasn't asked for lineage/impact, introduce capabilities briefly and ask a clarifying question.\n\n"
                f"USER: {request.message}\n"
                "ASSISTANT:"
            )
            llm_reply = LLM.invoke(persona_prompt)
            reply_text = getattr(llm_reply, "content", str(llm_reply))
            return ChatResponse(
                response=reply_text,
                sources=[],
                conversation_id=None,
                impacted_query_ids=[],
                impacted_queries=[],
            )

        # Build org-aware tools and delegate tool selection to the LLM agent
        lineage_tool = build_org_lineage_tool(org_id=resolved_org_id, k=request.k or 5)
        query_history_tool = build_org_query_history_tool(org_id=resolved_org_id, max_iters=5)

        agent = initialize_agent(
            tools=[lineage_tool, query_history_tool],
            llm=LLM,
            agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
            verbose=True,
        )

        # Strong guidance to the agent on tool selection and output format
        guidance = (
            "SYSTEM ROLE: You are Zane AI, an assistant that helps analyze data lineage and change impacts.\n"
            "BEHAVIOR:\n"
            "- Be concise and helpful.\n"
            "- If the user greets you (e.g., 'hi', 'hello'), respond with a short intro of who you are and how you can help (lineage Q&A and query impact analysis).\n"
            "- If the question is about schema/column changes or 'impacted queries', you MUST use the query_history_search tool.\n"
            "- When reporting impacted queries, return a concise, numbered list with a short SQL preview for each query, not just IDs.\n"
            "- If it's a pure lineage question, use the extract_lineage tool.\n"
        )
        # Nudge the agent to preferred tool if classification is specific
        preferred_hint = (
            "\nPREFERRED_TOOL: query_history_search\n" if classification_label == "impact" else (
                "\nPREFERRED_TOOL: extract_lineage\n" if classification_label == "lineage" else ""
            )
        )
        agent_query = f"{guidance}{preferred_hint}\nUser question: {query}"

        agent_result = agent.invoke(agent_query)
        # LangChain agents often return dicts with `output`; fallback to str
        if isinstance(agent_result, dict) and "output" in agent_result:
            response_text = agent_result.get("output", "")
        else:
            response_text = str(agent_result)

        # Best-effort: extract query IDs from the response text and fetch full queries
        impacted_query_ids: List[str] = []
        impacted_queries: List[Dict[str, Any]] = []
        try:
            import re as _re
            # Match UUID-like ids commonly used in results
            impacted_query_ids = list(dict.fromkeys(_re.findall(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b", response_text, flags=_re.I)))
            if impacted_query_ids:
                impacted_queries = fetch_queries(impacted_query_ids) or []
        except Exception:
            impacted_query_ids = []
            impacted_queries = []

        return ChatResponse(
            response=response_text,
            sources=[],  # Tool outputs include their own context; no structured source docs here
            conversation_id=None,
            impacted_query_ids=impacted_query_ids,
            impacted_queries=impacted_queries,
        )
        
    except Exception as e:
        logger.error(f"Error in chat endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing chat request: {str(e)}")

@router.post("/conversation", response_model=ChatConversation)
async def create_conversation(org_id: str):
    """
    Create a new conversation for an organization.
    
    Args:
        org_id: Organization ID
        
    Returns:
        ChatConversation object
    """
    try:
        if not org_id:
            raise HTTPException(status_code=400, detail="org_id is required")
        
        # Generate a simple conversation ID (in production, use proper UUID)
        import uuid
        conversation_id = str(uuid.uuid4())
        
        conversation = ChatConversation(
            conversation_id=conversation_id,
            org_id=org_id,
            messages=[],
            created_at=None,  # Could add timestamp
            updated_at=None
        )
        
        logger.info(f"Created conversation {conversation_id} for org {org_id}")
        return conversation
        
    except Exception as e:
        logger.error(f"Error creating conversation: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error creating conversation: {str(e)}")

@router.get("/health")
async def chat_health():
    """
    Health check for chat service.
    
    Returns:
        Status of chat service
    """
    return {"status": "healthy", "service": "chat"}

@router.get("/orgs/{org_id}/test")
async def test_org_vector_store(org_id: str):
    """
    Test if vector store is available for an organization.
    
    Args:
        org_id: Organization ID
        
    Returns:
        Test result
    """
    try:
        from app.vector_db import get_org_vector_store
        
        # Try to get the vector store
        vector_store = get_org_vector_store(org_id)
        
        # Try a simple query to test
        test_query = "test query"
        results = vector_store.similarity_search(test_query, k=1)
        
        return {
            "org_id": org_id,
            "status": "available",
            "test_results_count": len(results),
            "message": "Vector store is accessible"
        }
        
    except Exception as e:
        logger.error(f"Error testing vector store for org {org_id}: {str(e)}")
        return {
            "org_id": org_id,
            "status": "error",
            "message": f"Vector store error: {str(e)}"
        }
