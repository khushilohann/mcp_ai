#!/usr/bin/env python3
"""
MCP Server Startup Script (WebSocket mode)
Run this script to start the MCP server over WebSocket for concurrent connections.
"""
import asyncio
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.mcp_websocket_server import main

if __name__ == "__main__":
    asyncio.run(main())
