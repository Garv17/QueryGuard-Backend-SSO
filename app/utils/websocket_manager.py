import json
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Any
from fastapi import WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from app.utils.websocket_models import MessageType, UserStatus
import logging

logger = logging.getLogger(__name__)

class WebSocketMessage(BaseModel):
    type: str  # "chat_message", "system_message", "typing", "join", "leave"
    data: Dict[str, Any]
    timestamp: str = None
    sender_id: str = None
    room_id: str = None
    message_id: str = None

    def __init__(self, **data):
        if 'timestamp' not in data or data['timestamp'] is None:
            data['timestamp'] = datetime.utcnow().isoformat()
        if 'message_id' not in data or data['message_id'] is None:
            data['message_id'] = str(uuid.uuid4())
        super().__init__(**data)

class ChatSession(BaseModel):
    session_id: str
    org_id: str
    user_id: str
    websocket: WebSocket = None
    created_at: str
    last_activity: str
    
    class Config:
        arbitrary_types_allowed = True

    def __init__(self, **data):
        if 'session_id' not in data:
            data['session_id'] = str(uuid.uuid4())
        if 'created_at' not in data:
            data['created_at'] = datetime.utcnow().isoformat()
        if 'last_activity' not in data:
            data['last_activity'] = datetime.utcnow().isoformat()
        super().__init__(**data)

class WebSocketManager:
    def __init__(self):
        # Store active WebSocket connections
        self.active_connections: Dict[str, WebSocket] = {}
        # Store chat sessions by session_id
        self.chat_sessions: Dict[str, ChatSession] = {}
        # Store sessions by org_id for broadcasting
        self.org_sessions: Dict[str, List[str]] = {}
        # Store user sessions
        self.user_sessions: Dict[str, List[str]] = {}

    async def connect(self, websocket: WebSocket, session_id: str, org_id: str, user_id: str):
        """Accept a WebSocket connection and register the session"""
        try:
            await websocket.accept()
            
            # Create chat session
            session = ChatSession(
                session_id=session_id,
                org_id=org_id,
                user_id=user_id,
                websocket=websocket
            )
            
            # Store the connection
            self.active_connections[session_id] = websocket
            self.chat_sessions[session_id] = session
            
            # Add to org sessions
            if org_id not in self.org_sessions:
                self.org_sessions[org_id] = []
            if session_id not in self.org_sessions[org_id]:
                self.org_sessions[org_id].append(session_id)
            
            # Add to user sessions
            if user_id not in self.user_sessions:
                self.user_sessions[user_id] = []
            if session_id not in self.user_sessions[user_id]:
                self.user_sessions[user_id].append(session_id)
            
            logger.info(f"WebSocket connection established for session {session_id}, org {org_id}, user {user_id}")
            
            # Send connection confirmation
            await self.send_message(session_id, WebSocketMessage(
                type="system_message",
                data={
                    "message": "Connected to chat",
                    "session_id": session_id,
                    "status": "connected"
                },
                sender_id="system"
            ))
            
            return session
            
        except Exception as e:
            logger.error(f"Error connecting WebSocket: {str(e)}")
            raise

    async def disconnect(self, session_id: str):
        """Remove a WebSocket connection"""
        try:
            if session_id in self.chat_sessions:
                session = self.chat_sessions[session_id]
                org_id = session.org_id
                user_id = session.user_id
                
                # Remove from active connections
                if session_id in self.active_connections:
                    del self.active_connections[session_id]
                
                # Remove from chat sessions
                del self.chat_sessions[session_id]
                
                # Remove from org sessions
                if org_id in self.org_sessions and session_id in self.org_sessions[org_id]:
                    self.org_sessions[org_id].remove(session_id)
                    if not self.org_sessions[org_id]:
                        del self.org_sessions[org_id]
                
                # Remove from user sessions
                if user_id in self.user_sessions and session_id in self.user_sessions[user_id]:
                    self.user_sessions[user_id].remove(session_id)
                    if not self.user_sessions[user_id]:
                        del self.user_sessions[user_id]
                
                logger.info(f"WebSocket disconnected for session {session_id}")
                
        except Exception as e:
            logger.error(f"Error disconnecting WebSocket: {str(e)}")

    async def send_message(self, session_id: str, message: WebSocketMessage):
        """Send a message to a specific WebSocket connection"""
        try:
            if session_id in self.active_connections:
                websocket = self.active_connections[session_id]
                message_dict = message.dict()
                await websocket.send_text(json.dumps(message_dict))
                
                # Update last activity
                if session_id in self.chat_sessions:
                    self.chat_sessions[session_id].last_activity = datetime.utcnow().isoformat()
                    
                return True
            else:
                logger.warning(f"Session {session_id} not found in active connections")
                return False
                
        except WebSocketDisconnect:
            logger.info(f"WebSocket disconnected for session {session_id}")
            await self.disconnect(session_id)
            return False
        except Exception as e:
            logger.error(f"Error sending message to session {session_id}: {str(e)}")
            return False

    async def broadcast_to_org(self, org_id: str, message: WebSocketMessage, exclude_session: Optional[str] = None):
        """Broadcast a message to all connections in an organization"""
        try:
            if org_id in self.org_sessions:
                session_ids = self.org_sessions[org_id].copy()
                failed_sessions = []
                
                for session_id in session_ids:
                    if exclude_session and session_id == exclude_session:
                        continue
                        
                    success = await self.send_message(session_id, message)
                    if not success:
                        failed_sessions.append(session_id)
                
                # Clean up failed sessions
                for failed_session in failed_sessions:
                    await self.disconnect(failed_session)
                    
                logger.info(f"Broadcasted message to {len(session_ids) - len(failed_sessions)} sessions in org {org_id}")
                
        except Exception as e:
            logger.error(f"Error broadcasting to org {org_id}: {str(e)}")

    async def broadcast_to_user(self, user_id: str, message: WebSocketMessage, exclude_session: Optional[str] = None):
        """Broadcast a message to all connections for a user"""
        try:
            if user_id in self.user_sessions:
                session_ids = self.user_sessions[user_id].copy()
                failed_sessions = []
                
                for session_id in session_ids:
                    if exclude_session and session_id == exclude_session:
                        continue
                        
                    success = await self.send_message(session_id, message)
                    if not success:
                        failed_sessions.append(session_id)
                
                # Clean up failed sessions
                for failed_session in failed_sessions:
                    await self.disconnect(failed_session)
                    
                logger.info(f"Broadcasted message to {len(session_ids) - len(failed_sessions)} sessions for user {user_id}")
                
        except Exception as e:
            logger.error(f"Error broadcasting to user {user_id}: {str(e)}")

    async def get_session_info(self, session_id: str) -> Optional[ChatSession]:
        """Get information about a chat session"""
        return self.chat_sessions.get(session_id)

    async def get_active_sessions(self, org_id: str = None, user_id: str = None) -> List[ChatSession]:
        """Get list of active sessions, optionally filtered by org_id or user_id"""
        try:
            sessions = []
            
            if org_id:
                if org_id in self.org_sessions:
                    for session_id in self.org_sessions[org_id]:
                        if session_id in self.chat_sessions:
                            sessions.append(self.chat_sessions[session_id])
            elif user_id:
                if user_id in self.user_sessions:
                    for session_id in self.user_sessions[user_id]:
                        if session_id in self.chat_sessions:
                            sessions.append(self.chat_sessions[session_id])
            else:
                sessions = list(self.chat_sessions.values())
            
            return sessions
            
        except Exception as e:
            logger.error(f"Error getting active sessions: {str(e)}")
            return []

    async def cleanup_inactive_sessions(self, timeout_minutes: int = 30):
        """Remove sessions that have been inactive for too long"""
        try:
            from datetime import timedelta
            current_time = datetime.utcnow()
            timeout_delta = timedelta(minutes=timeout_minutes)
            
            inactive_sessions = []
            
            for session_id, session in self.chat_sessions.items():
                last_activity = datetime.fromisoformat(session.last_activity)
                if current_time - last_activity > timeout_delta:
                    inactive_sessions.append(session_id)
            
            for session_id in inactive_sessions:
                await self.disconnect(session_id)
                logger.info(f"Cleaned up inactive session: {session_id}")
            
            if inactive_sessions:
                logger.info(f"Cleaned up {len(inactive_sessions)} inactive sessions")
                
        except Exception as e:
            logger.error(f"Error cleaning up inactive sessions: {str(e)}")

    def get_stats(self) -> Dict[str, Any]:
        """Get WebSocket connection statistics"""
        return {
            "active_connections": len(self.active_connections),
            "active_sessions": len(self.chat_sessions),
            "organizations_with_sessions": len(self.org_sessions),
            "users_with_sessions": len(self.user_sessions),
            "total_org_sessions": sum(len(sessions) for sessions in self.org_sessions.values()),
            "total_user_sessions": sum(len(sessions) for sessions in self.user_sessions.values())
        }

# Global WebSocket manager instance
websocket_manager = WebSocketManager()