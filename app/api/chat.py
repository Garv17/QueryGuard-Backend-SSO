from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from app.vector_db import get_qa_chain
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/chat", tags=["chat"])

class ChatMessage(BaseModel):
    role: str  # "user" or "assistant"
    content: str
    timestamp: Optional[str] = None

class ChatRequest(BaseModel):
    org_id: str
    message: str
    k: Optional[int] = 5  # Number of documents to retrieve for context
    conversation_history: Optional[List[ChatMessage]] = []

class ChatResponse(BaseModel):
    response: str
    sources: List[Dict[str, Any]]
    conversation_id: Optional[str] = None

class ChatConversation(BaseModel):
    conversation_id: str
    org_id: str
    messages: List[ChatMessage]
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

@router.post("/query", response_model=ChatResponse)
async def chat_with_llm(request: ChatRequest):
    """
    Chat with LLM using vector database context for a specific organization.
    
    Args:
        request: ChatRequest containing org_id, message, and optional parameters
        
    Returns:
        ChatResponse with LLM response and source documents
    """
    try:
        logger.info(f"Chat request for org_id: {request.org_id}, message: {request.message[:100]}...")
        
        # Validate org_id
        if not request.org_id:
            raise HTTPException(status_code=400, detail="org_id is required")
        
        # Get QA chain for the organization
        qa_chain = get_qa_chain(request.org_id, k=request.k)
        
        # Prepare the query with conversation context if provided
        query = request.message
        if request.conversation_history:
            # Add conversation context to the query
            context_messages = []
            for msg in request.conversation_history[-5:]:  # Keep last 5 messages for context
                context_messages.append(f"{msg.role}: {msg.content}")
            
            if context_messages:
                context = "\n".join(context_messages)
                query = f"Previous conversation context:\n{context}\n\nCurrent question: {request.message}"
        
        # Invoke the QA chain
        result = qa_chain.invoke({"query": query})
        
        # Extract response and sources
        response_text = result.get("result", "")
        source_documents = result.get("source_documents", [])
        
        # Format source documents
        sources = []
        for doc in source_documents:
            source_info = {
                "content": doc.page_content,
                "metadata": doc.metadata
            }
            sources.append(source_info)
        
        logger.info(f"Chat response generated with {len(sources)} sources")
        
        return ChatResponse(
            response=response_text,
            sources=sources,
            conversation_id=None  # Could be implemented for conversation persistence
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
