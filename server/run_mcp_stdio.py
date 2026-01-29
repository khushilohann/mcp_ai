#!/usr/bin/env python3
"""
MCP Server Startup Script (stdio mode)
Run this script to start the MCP server over stdio for CLI clients.
"""
import asyncio
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.mcp_server import run_stdio_server

if __name__ == "__main__":
    print("Starting MCP Server (stdio mode)...", file=sys.stderr)
    print("Protocol: JSON-RPC 2.0 over stdio", file=sys.stderr)
    print("Ready to accept connections", file=sys.stderr)
    asyncio.run(run_stdio_server())
