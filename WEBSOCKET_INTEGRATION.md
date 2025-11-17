# WebSocket Chat Integration for QueryGuard

This document explains how to use the WebSocket functionality added to QueryGuard for real-time chat features.

## Overview

The WebSocket integration provides real-time bidirectional communication between clients and the QueryGuard AI assistant. Users can send messages and receive immediate responses with typing indicators, user status updates, and more.

## Features

- **Real-time messaging**: Send and receive messages instantly
- **AI Integration**: Chat messages are automatically processed by the QueryGuard AI assistant
- **Typing indicators**: See when users or AI are typing
- **User status tracking**: Track when users join/leave chat
- **Session management**: Automatic cleanup of inactive sessions
- **Error handling**: Graceful error handling with user-friendly messages
- **Multi-user support**: Multiple users can chat simultaneously in organization rooms

## API Endpoints

### WebSocket Connection

**Endpoint**: `ws://localhost:8000/chat/ws/{org_id}/{user_id}`

**Query Parameters**:
- `session_id` (optional): Custom session ID, auto-generated if not provided
- `user_name` (optional): Display name for the user

**Example**:
```
ws://localhost:8000/chat/ws/76d33fb3-6062-456b-a211-4aec9971f8be/user123?session_id=my-session&user_name=John%20Doe
```

### REST API Endpoints

1. **Get Active Sessions**
   - `GET /chat/sessions/{org_id}`
   - Returns list of active chat sessions for an organization

2. **Get Chat Statistics**
   - `GET /chat/stats`
   - Returns WebSocket connection statistics

3. **Cleanup Inactive Sessions**
   - `POST /chat/cleanup?timeout_minutes=30`
   - Manually trigger cleanup of inactive sessions

4. **Test Page**
   - `GET /chat/test-page`
   - Serves a simple HTML test page for WebSocket functionality

## WebSocket Message Types

### Outgoing Messages (Client → Server)

#### 1. Chat Message
```json
{
  "type": "chat_message",
  "content": "Your message here",
  "conversation_id": "optional-conversation-id"
}
```

#### 2. Typing Indicator
```json
{
  "type": "typing",
  "data": {
    "is_typing": true
  }
}
```

#### 3. Ping (Keep-alive)
```json
{
  "type": "ping"
}
```

### Incoming Messages (Server → Client)

#### 1. System Message
```json
{
  "type": "system_message",
  "data": {
    "message": "Welcome to QueryGuard chat!",
    "session_id": "session-123",
    "status": "connected"
  },
  "sender_id": "system",
  "timestamp": "2023-11-07T10:30:00Z",
  "message_id": "msg-123"
}
```

#### 2. User Chat Message
```json
{
  "type": "chat_message",
  "data": {
    "content": "Hello everyone!",
    "sender_id": "user123",
    "sender_name": "John Doe",
    "message_type": "user"
  },
  "sender_id": "user123",
  "timestamp": "2023-11-07T10:30:00Z",
  "message_id": "msg-124"
}
```

#### 3. AI Response
```json
{
  "type": "ai_response",
  "data": {
    "response": "Based on your query, here's what I found...",
    "sources": [
      {
        "content": "source document content",
        "metadata": {"table": "users", "schema": "public"}
      }
    ],
    "processing_time": 2.5,
    "sender_id": "ai_assistant",
    "sender_name": "QueryGuard AI",
    "message_type": "assistant"
  },
  "sender_id": "ai_assistant",
  "timestamp": "2023-11-07T10:30:02Z",
  "message_id": "msg-125"
}
```

#### 4. Typing Indicator
```json
{
  "type": "typing",
  "data": {
    "is_typing": true,
    "sender_id": "user123",
    "sender_name": "John Doe"
  },
  "sender_id": "user123",
  "timestamp": "2023-11-07T10:30:00Z"
}
```

#### 5. User Status
```json
{
  "type": "user_status",
  "data": {
    "message": "User John Doe joined the chat",
    "user_id": "user123",
    "user_name": "John Doe",
    "status": "online",
    "action": "joined"
  },
  "sender_id": "user123",
  "timestamp": "2023-11-07T10:30:00Z"
}
```

#### 6. Error Message
```json
{
  "type": "error",
  "data": {
    "error_code": "PROCESSING_ERROR",
    "error_message": "Error processing message"
  },
  "sender_id": "system",
  "timestamp": "2023-11-07T10:30:00Z"
}
```

#### 7. Pong Response
```json
{
  "type": "pong",
  "data": {
    "message": "pong"
  },
  "sender_id": "system",
  "timestamp": "2023-11-07T10:30:00Z"
}
```

## JavaScript Client Example

```javascript
class QueryGuardWebSocketClient {
  constructor(baseUrl = 'ws://localhost:8000') {
    this.baseUrl = baseUrl;
    this.ws = null;
  }

  connect(orgId, userId, userName) {
    const url = `${this.baseUrl}/chat/ws/${orgId}/${userId}?user_name=${encodeURIComponent(userName)}`;
    this.ws = new WebSocket(url);

    this.ws.onopen = (event) => {
      console.log('Connected to WebSocket');
    };

    this.ws.onmessage = (event) => {
      const data = JSON.parse(event.data);
      this.handleMessage(data);
    };

    this.ws.onclose = (event) => {
      console.log('WebSocket connection closed');
    };

    this.ws.onerror = (error) => {
      console.error('WebSocket error:', error);
    };
  }

  sendMessage(content) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify({
        type: 'chat_message',
        content: content
      }));
    }
  }

  sendTyping(isTyping) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify({
        type: 'typing',
        data: { is_typing: isTyping }
      }));
    }
  }

  handleMessage(data) {
    switch(data.type) {
      case 'chat_message':
        console.log(`${data.data.sender_name}: ${data.data.content}`);
        break;
      case 'ai_response':
        console.log(`AI: ${data.data.response}`);
        break;
      case 'system_message':
        console.log(`System: ${data.data.message}`);
        break;
      // Handle other message types...
    }
  }

  disconnect() {
    if (this.ws) {
      this.ws.close();
    }
  }
}

// Usage
const client = new QueryGuardWebSocketClient();
client.connect('org-id', 'user-id', 'User Name');
```

## Python Client Example

See `websocket_client_example.py` for a complete Python client implementation.

**Basic usage**:
```python
from websocket_client_example import QueryGuardChatClient

async def main():
    client = QueryGuardChatClient()
    
    # Connect
    await client.connect(
        org_id="76d33fb3-6062-456b-a211-4aec9971f8be",
        user_id="user123",
        user_name="John Doe"
    )
    
    # Send a message
    await client.send_message("What is data lineage?")
    
    # Send typing indicator
    await client.send_typing(True)
    
    # Keep connection alive
    await asyncio.sleep(30)
    
    # Disconnect
    await client.disconnect()
```

## Testing the WebSocket Implementation

### 1. Using the Test Page

Navigate to `http://localhost:8000/chat/test-page` to access the built-in test interface.

### 2. Using the Python Client

```bash
# Interactive mode
python websocket_client_example.py

# Automated test mode
python websocket_client_example.py test
```

### 3. Using cURL for REST API

```bash
# Get chat statistics
curl -X GET "http://localhost:8000/chat/stats"

# Get active sessions
curl -X GET "http://localhost:8000/chat/sessions/76d33fb3-6062-456b-a211-4aec9971f8be"

# Cleanup inactive sessions
curl -X POST "http://localhost:8000/chat/cleanup?timeout_minutes=30"
```

## Architecture

### WebSocket Manager
- Manages active WebSocket connections
- Handles session lifecycle (connect/disconnect)
- Provides broadcasting capabilities
- Automatic cleanup of inactive sessions

### Message Flow
1. User sends message via WebSocket
2. Message is validated and echoed to all org users
3. AI assistant processes the message
4. Response is generated using vector DB context
5. AI response is broadcast to all org users

### Session Management
- Each connection gets a unique session ID
- Sessions are grouped by organization
- Inactive sessions are automatically cleaned up
- Connection state is tracked and monitored

## Configuration

### Environment Variables
Add to your environment:
```bash
# WebSocket settings (optional, has defaults)
WEBSOCKET_PING_INTERVAL=30
WEBSOCKET_PING_TIMEOUT=10
WEBSOCKET_CLOSE_TIMEOUT=10
```

### CORS Configuration
The WebSocket endpoint inherits CORS settings from the main FastAPI application. Update `main.py` CORS configuration as needed for production.

## Error Handling

The WebSocket implementation includes comprehensive error handling:

- **Connection errors**: Graceful connection cleanup
- **Message parsing errors**: JSON validation with error responses
- **AI processing errors**: Fallback error messages to users
- **Session timeouts**: Automatic cleanup of inactive sessions
- **Rate limiting**: Can be added if needed

## Security Considerations

1. **Authentication**: Add authentication token validation to WebSocket connections
2. **Rate limiting**: Implement message rate limiting per user/session
3. **Input validation**: Validate all incoming message content
4. **CORS**: Configure appropriate CORS settings for production
5. **SSL/TLS**: Use WSS (WebSocket Secure) in production

## Production Deployment

### Using Docker

The existing Docker setup should work with WebSocket. Ensure:

1. WebSocket port (8000) is exposed
2. Load balancer supports WebSocket (if using one)
3. Session persistence is handled correctly

### Using nginx

If using nginx as reverse proxy, add WebSocket support:

```nginx
location /chat/ws {
    proxy_pass http://backend;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
}
```

## Monitoring

Monitor WebSocket connections using:

1. **Connection statistics**: `/chat/stats` endpoint
2. **Active sessions**: `/chat/sessions/{org_id}` endpoint  
3. **Application logs**: WebSocket events are logged
4. **Health checks**: Include WebSocket stats in `/worker-status`

## Troubleshooting

### Common Issues

1. **Connection refused**: Check if server is running and port is accessible
2. **CORS errors**: Verify CORS configuration in main.py
3. **JSON parsing errors**: Ensure client sends valid JSON
4. **Session cleanup**: Use `/chat/cleanup` endpoint to manually clean sessions

### Debug Mode

Enable debug logging:
```python
logging.getLogger("app.utils.websocket_manager").setLevel(logging.DEBUG)
logging.getLogger("app.api.chat").setLevel(logging.DEBUG)
```

## Next Steps

Potential enhancements:
1. Add authentication and authorization
2. Implement message persistence/history
3. Add file sharing capabilities
4. Implement typing timeouts
5. Add user presence indicators
6. Implement private messaging
7. Add message reactions/emojis
8. Rate limiting and abuse prevention