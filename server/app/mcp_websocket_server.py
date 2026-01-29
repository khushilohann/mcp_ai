"""
MCP WebSocket Server for concurrent connections
Supports multiple AI agents connecting simultaneously via WebSocket.
"""
import json
import asyncio
import logging
from typing import Dict, Set
import websockets
from websockets.server import WebSocketServerProtocol

from app.mcp_server import MCPServer, JSONRPCResponse
from app.core.logging import get_logger

logger = get_logger("mcp.websocket")


class MCPWebSocketServer:
    """MCP Server over WebSocket for concurrent connections"""
    
    def __init__(self, host: str = "localhost", port: int = 8765):
        self.host = host
        self.port = port
        self.server = None
        self.connections: Set[WebSocketServerProtocol] = set()
        self.mcp_server = MCPServer()
        logger.info(f"MCP WebSocket Server initialized on {host}:{port}")
    
    async def handle_client(self, websocket: WebSocketServerProtocol, path: str):
        """Handle a WebSocket client connection"""
        self.connections.add(websocket)
        client_addr = f"{websocket.remote_address[0]}:{websocket.remote_address[1]}"
        logger.info(f"Client connected: {client_addr} (total connections: {len(self.connections)})")
        
        try:
            async for message in websocket:
                try:
                    # Handle JSON-RPC request
                    response = await self.mcp_server.handle_request(message)
                    
                    if response:
                        await websocket.send(response)
                
                except json.JSONDecodeError as e:
                    logger.error(f"JSON decode error from {client_addr}: {e}")
                    error_response = json.dumps({
                        "jsonrpc": "2.0",
                        "id": None,
                        "error": {
                            "code": -32700,
                            "message": "Parse error",
                            "data": str(e)
                        }
                    }) + "\n"
                    await websocket.send(error_response)
                
                except Exception as e:
                    logger.exception(f"Error handling message from {client_addr}: {e}")
                    error_response = json.dumps({
                        "jsonrpc": "2.0",
                        "id": None,
                        "error": {
                            "code": -32603,
                            "message": "Internal error",
                            "data": str(e)
                        }
                    }) + "\n"
                    await websocket.send(error_response)
        
        except websockets.exceptions.ConnectionClosed:
            logger.info(f"Client disconnected: {client_addr}")
        except Exception as e:
            logger.exception(f"Unexpected error with client {client_addr}: {e}")
        finally:
            self.connections.discard(websocket)
            logger.info(f"Client removed: {client_addr} (remaining connections: {len(self.connections)})")
    
    async def start(self):
        """Start the WebSocket server"""
        logger.info(f"Starting MCP WebSocket Server on ws://{self.host}:{self.port}")
        self.server = await websockets.serve(
            self.handle_client,
            self.host,
            self.port,
            ping_interval=20,
            ping_timeout=10
        )
        logger.info(f"MCP WebSocket Server started. Listening on ws://{self.host}:{self.port}")
        logger.info(f"Supports concurrent connections from multiple AI agents")
    
    async def stop(self):
        """Stop the WebSocket server"""
        if self.server:
            logger.info("Stopping MCP WebSocket Server...")
            self.server.close()
            await self.server.wait_closed()
            logger.info("MCP WebSocket Server stopped")
    
    async def run_forever(self):
        """Run server forever"""
        await self.start()
        try:
            await asyncio.Future()  # Run forever
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        finally:
            await self.stop()


async def main():
    """Main entry point for WebSocket server"""
    import sys
    
    host = sys.argv[1] if len(sys.argv) > 1 else "localhost"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 8765
    
    server = MCPWebSocketServer(host=host, port=port)
    await server.run_forever()


if __name__ == "__main__":
    asyncio.run(main())
