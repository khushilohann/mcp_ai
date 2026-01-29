#!/usr/bin/env python3
"""
Simple test script for MCP server
Tests JSON-RPC 2.0 protocol implementation
"""
import json
import asyncio
from app.mcp_server import MCPServer, JSONRPCRequest


async def test_mcp_server():
    """Test MCP server functionality"""
    server = MCPServer()
    
    print("Testing MCP Server...")
    print("=" * 50)
    
    # Test 1: Initialize
    print("\n1. Testing initialize...")
    init_request = json.dumps({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {}
    })
    response = await server.handle_request(init_request)
    print(f"Response: {response}")
    
    # Test 2: List tools
    print("\n2. Testing tools/list...")
    tools_request = json.dumps({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/list"
    })
    response = await server.handle_request(tools_request)
    result = json.loads(response)
    print(f"Found {len(result.get('result', {}).get('tools', []))} tools")
    
    # Test 3: Call list_sources tool
    print("\n3. Testing tools/call (list_sources)...")
    call_request = json.dumps({
        "jsonrpc": "2.0",
        "id": 3,
        "method": "tools/call",
        "params": {
            "name": "list_sources",
            "arguments": {}
        }
    })
    response = await server.handle_request(call_request)
    result = json.loads(response)
    print(f"Sources: {json.dumps(result.get('result', {}), indent=2)}")
    
    # Test 4: Call query_data tool
    print("\n4. Testing tools/call (query_data)...")
    query_request = json.dumps({
        "jsonrpc": "2.0",
        "id": 4,
        "method": "tools/call",
        "params": {
            "name": "query_data",
            "arguments": {
                "question": "show all users",
                "sources": ["sql"]
            }
        }
    })
    response = await server.handle_request(query_request)
    result = json.loads(response)
    if result.get("error"):
        print(f"Error: {result['error']}")
    else:
        exec_result = result.get("result", {}).get("execution", {})
        rows = exec_result.get("rows", [])
        print(f"Query returned {len(rows)} rows")
    
    print("\n" + "=" * 50)
    print("All tests completed!")


if __name__ == "__main__":
    asyncio.run(test_mcp_server())
