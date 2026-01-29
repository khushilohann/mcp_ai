"""
MCP (Model Context Protocol) Server Implementation
Implements JSON-RPC 2.0 over stdio and WebSocket transports.
Supports concurrent connections from multiple AI agents.
"""
import json
import sys
import asyncio
import logging
from typing import Any, Dict, List, Optional, Callable, Awaitable
from dataclasses import dataclass
from enum import Enum
import traceback

from app.core.logging import setup_logging, get_logger
from app.tools.query_data import query_data, QueryPayload
from app.tools.query_api import query_api, QueryAPIRequest
from app.tools.list_sources import list_sources
from app.tools.transform_data import transform_data, TransformRequest, TransformSpec
from app.tools.export_data import export_data, ExportRequest
from app.tools.integrate import integrate_data, IntegrateRequest
from app.tools.analyze_schema import analyze_schema, suggest_queries
from app.tools.data_quality import check_data_quality, DataQualityRequest
from app.tools.file_connector import list_files, parse_file
from app.services.unified_search import search_everywhere_users

setup_logging()
logger = get_logger("mcp.server")


class JSONRPCErrorCode(Enum):
    """JSON-RPC 2.0 error codes"""
    PARSE_ERROR = -32700
    INVALID_REQUEST = -32600
    METHOD_NOT_FOUND = -32601
    INVALID_PARAMS = -32602
    INTERNAL_ERROR = -32603
    SERVER_ERROR = -32000


@dataclass
class JSONRPCRequest:
    """JSON-RPC 2.0 request"""
    jsonrpc: str = "2.0"
    id: Optional[Any] = None
    method: str = ""
    params: Optional[Dict[str, Any]] = None


@dataclass
class JSONRPCResponse:
    """JSON-RPC 2.0 response"""
    jsonrpc: str = "2.0"
    id: Optional[Any] = None
    result: Optional[Any] = None
    error: Optional[Dict[str, Any]] = None


@dataclass
class JSONRPCError:
    """JSON-RPC 2.0 error"""
    code: int
    message: str
    data: Optional[Any] = None


class MCPServer:
    """MCP Server implementing JSON-RPC 2.0 protocol"""
    
    def __init__(self):
        self.tools: Dict[str, Callable] = {}
        self.resources: Dict[str, Callable] = {}
        self.prompts: Dict[str, Callable] = {}
        self._register_tools()
        self._register_resources()
        self._register_prompts()
        logger.info("MCP Server initialized")
    
    def _register_tools(self):
        """Register all MCP tools"""
        self.tools = {
            "list_sources": self._handle_list_sources,
            "query_data": self._handle_query_data,
            "query_api": self._handle_query_api,
            "transform_data": self._handle_transform_data,
            "integrate_data": self._handle_integrate_data,
            "export_data": self._handle_export_data,
            "analyze_schema": self._handle_analyze_schema,
            "suggest_queries": self._handle_suggest_queries,
            "check_data_quality": self._handle_check_data_quality,
            "list_files": self._handle_list_files,
            "parse_file": self._handle_parse_file,
            "search_users": self._handle_search_users,
        }
        logger.info(f"Registered {len(self.tools)} MCP tools")
    
    def _register_resources(self):
        """Register MCP resources"""
        self.resources = {
            "sources": self._get_sources_resource,
            "tables": self._get_tables_resource,
        }
    
    def _register_prompts(self):
        """Register MCP prompts"""
        self.prompts = {
            "query_help": self._get_query_help_prompt,
        }
    
    async def handle_request(self, request_data: str) -> Optional[str]:
        """Handle a JSON-RPC request"""
        try:
            request = self._parse_request(request_data)
            if request is None:
                return None
            
            # Handle notification (no id)
            if request.id is None:
                await self._handle_notification(request)
                return None
            
            # Handle method call
            response = await self._handle_method(request)
            return self._serialize_response(response)
        
        except json.JSONDecodeError as e:
            logger.error(f"JSON parse error: {e}")
            error_response = JSONRPCResponse(
                id=None,
                error={
                    "code": JSONRPCErrorCode.PARSE_ERROR.value,
                    "message": "Parse error",
                    "data": str(e)
                }
            )
            return self._serialize_response(error_response)
        except Exception as e:
            logger.exception(f"Unexpected error handling request: {e}")
            error_response = JSONRPCResponse(
                id=None,
                error={
                    "code": JSONRPCErrorCode.INTERNAL_ERROR.value,
                    "message": "Internal error",
                    "data": str(e)
                }
            )
            return self._serialize_response(error_response)
    
    def _parse_request(self, data: str) -> Optional[JSONRPCRequest]:
        """Parse JSON-RPC request"""
        try:
            obj = json.loads(data)
            if not isinstance(obj, dict):
                return None
            
            return JSONRPCRequest(
                jsonrpc=obj.get("jsonrpc", "2.0"),
                id=obj.get("id"),
                method=obj.get("method", ""),
                params=obj.get("params")
            )
        except json.JSONDecodeError:
            raise
    
    async def _handle_method(self, request: JSONRPCRequest) -> JSONRPCResponse:
        """Handle a method call"""
        method = request.method
        
        # Handle MCP protocol methods
        if method == "initialize":
            return await self._handle_initialize(request)
        elif method == "tools/list":
            return await self._handle_tools_list(request)
        elif method == "tools/call":
            return await self._handle_tools_call(request)
        elif method == "resources/list":
            return await self._handle_resources_list(request)
        elif method == "resources/read":
            return await self._handle_resources_read(request)
        elif method == "prompts/list":
            return await self._handle_prompts_list(request)
        elif method == "prompts/get":
            return await self._handle_prompts_get(request)
        else:
            # Try custom tool
            if method in self.tools:
                return await self._call_tool(method, request.params or {})
            else:
                return JSONRPCResponse(
                    id=request.id,
                    error={
                        "code": JSONRPCErrorCode.METHOD_NOT_FOUND.value,
                        "message": f"Method not found: {method}"
                    }
                )
    
    async def _handle_notification(self, request: JSONRPCRequest):
        """Handle notification (no response)"""
        if request.method == "notifications/initialized":
            logger.info("Client initialized")
        elif request.method == "notifications/progress":
            logger.debug(f"Progress notification: {request.params}")
    
    async def _handle_initialize(self, request: JSONRPCRequest) -> JSONRPCResponse:
        """Handle initialize request"""
        return JSONRPCResponse(
            id=request.id,
            result={
                "protocolVersion": "2024-11-05",
                "capabilities": {
                    "tools": {},
                    "resources": {},
                    "prompts": {}
                },
                "serverInfo": {
                    "name": "null-pointers-mcp-server",
                    "version": "1.0.0"
                }
            }
        )
    
    async def _handle_tools_list(self, request: JSONRPCRequest) -> JSONRPCResponse:
        """List all available tools"""
        tools = []
        for tool_name, handler in self.tools.items():
            tool_def = await self._get_tool_definition(tool_name)
            if tool_def:
                tools.append(tool_def)
        
        return JSONRPCResponse(
            id=request.id,
            result={"tools": tools}
        )
    
    async def _handle_tools_call(self, request: JSONRPCRequest) -> JSONRPCResponse:
        """Call a tool"""
        params = request.params or {}
        tool_name = params.get("name")
        arguments = params.get("arguments", {})
        
        if tool_name not in self.tools:
            return JSONRPCResponse(
                id=request.id,
                error={
                    "code": JSONRPCErrorCode.METHOD_NOT_FOUND.value,
                    "message": f"Tool not found: {tool_name}"
                }
            )
        
        try:
            result = await self._call_tool(tool_name, arguments)
            result.id = request.id
            return result
        except Exception as e:
            logger.exception(f"Error calling tool {tool_name}: {e}")
            return JSONRPCResponse(
                id=request.id,
                error={
                    "code": JSONRPCErrorCode.INTERNAL_ERROR.value,
                    "message": f"Tool execution failed: {str(e)}",
                    "data": traceback.format_exc()
                }
            )
    
    async def _call_tool(self, tool_name: str, arguments: Dict[str, Any]) -> JSONRPCResponse:
        """Call a tool handler"""
        handler = self.tools[tool_name]
        try:
            result = await handler(arguments)
            return JSONRPCResponse(
                id=None,
                result=result
            )
        except Exception as e:
            logger.exception(f"Tool {tool_name} error: {e}")
            raise
    
    async def _get_tool_definition(self, tool_name: str) -> Optional[Dict[str, Any]]:
        """Get tool definition for MCP"""
        definitions = {
            "list_sources": {
                "name": "list_sources",
                "description": "List all configured data sources with metadata",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            },
            "query_data": {
                "name": "query_data",
                "description": "Execute queries using natural language or SQL. Converts natural language to SQL using LLM. Supports joins across different sources.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "question": {
                            "type": "string",
                            "description": "Natural language question or SQL query"
                        },
                        "sources": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Data sources to query: sql, api, file"
                        }
                    },
                    "required": ["question"]
                }
            },
            "query_api": {
                "name": "query_api",
                "description": "Execute REST API calls (GET, POST, PUT, DELETE) with authentication support",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "method": {"type": "string", "enum": ["GET", "POST", "PUT", "DELETE"]},
                        "path": {"type": "string"},
                        "params": {"type": "object"},
                        "json": {"type": "object"},
                        "base_url": {"type": "string"},
                        "api_key": {"type": "string"},
                        "oauth2_token": {"type": "string"},
                        "use_cache": {"type": "boolean"}
                    },
                    "required": ["method", "path"]
                }
            },
            "transform_data": {
                "name": "transform_data",
                "description": "Apply transformations to query results: filter, sort, aggregate, data type conversions, column mapping",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "sql": {"type": "string"},
                        "rows": {"type": "array"},
                        "transform_spec": {
                            "type": "object",
                            "properties": {
                                "filter": {"type": "string"},
                                "sort": {"type": "array", "items": {"type": "string"}},
                                "select": {"type": "array", "items": {"type": "string"}},
                                "rename": {"type": "object"},
                                "groupby": {"type": "array", "items": {"type": "string"}},
                                "aggregations": {"type": "object"},
                                "limit": {"type": "integer"},
                                "offset": {"type": "integer"}
                            }
                        }
                    }
                }
            },
            "integrate_data": {
                "name": "integrate_data",
                "description": "Combine data from multiple sources with automatic schema alignment, conflict resolution, and deduplication",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "sources": {
                            "type": "array",
                            "items": {
                                "type": "array",
                                "items": {"type": "object"}
                            }
                        },
                        "join_on": {"type": "array", "items": {"type": "string"}},
                        "deduplicate_on": {"type": "array", "items": {"type": "string"}},
                        "conflict_strategy": {"type": "string", "enum": ["prefer_first", "prefer_last", "merge"]}
                    },
                    "required": ["sources"]
                }
            },
            "export_data": {
                "name": "export_data",
                "description": "Export results to various formats (JSON, CSV, Excel) with summary reports and visualizations",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "sql": {"type": "string"},
                        "rows": {"type": "array"},
                        "format": {"type": "string", "enum": ["json", "csv", "xlsx", "report"]},
                        "filename": {"type": "string"},
                        "include_summary": {"type": "boolean"},
                        "include_visualization": {"type": "boolean"},
                        "transform_spec": {"type": "object"}
                    }
                }
            },
            "analyze_schema": {
                "name": "analyze_schema",
                "description": "AI-powered schema analysis and query optimization suggestions",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "table_name": {"type": "string"},
                        "question": {"type": "string"}
                    }
                }
            },
            "suggest_queries": {
                "name": "suggest_queries",
                "description": "Get query suggestions based on schema analysis",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "use_case": {"type": "string"}
                    }
                }
            },
            "check_data_quality": {
                "name": "check_data_quality",
                "description": "Detect anomalies, missing values, inconsistencies in data",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "sql": {"type": "string"},
                        "table_name": {"type": "string"},
                        "rows": {"type": "array"}
                    }
                }
            },
            "list_files": {
                "name": "list_files",
                "description": "List files in a directory",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "directory": {"type": "string"}
                    }
                }
            },
            "parse_file": {
                "name": "parse_file",
                "description": "Parse CSV, JSON, XML, Excel files",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "file_path": {"type": "string"},
                        "file_content": {"type": "string"}
                    },
                    "required": ["file_path"]
                }
            },
            "search_users": {
                "name": "search_users",
                "description": "Unified search across SQL, REST API, and files for user data with AND/OR filtering",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Search query supporting id, name, email, region, signup_date with AND/OR operators"
                        }
                    },
                    "required": ["query"]
                }
            }
        }
        return definitions.get(tool_name)
    
    async def _handle_resources_list(self, request: JSONRPCRequest) -> JSONRPCResponse:
        """List available resources"""
        resources = [
            {
                "uri": "sources://all",
                "name": "All Data Sources",
                "description": "List of all configured data sources",
                "mimeType": "application/json"
            },
            {
                "uri": "tables://all",
                "name": "All Database Tables",
                "description": "List of all database tables",
                "mimeType": "application/json"
            }
        ]
        return JSONRPCResponse(id=request.id, result={"resources": resources})
    
    async def _handle_resources_read(self, request: JSONRPCRequest) -> JSONRPCResponse:
        """Read a resource"""
        params = request.params or {}
        uri = params.get("uri", "")
        
        if uri.startswith("sources://"):
            result = await self._get_sources_resource({})
        elif uri.startswith("tables://"):
            result = await self._get_tables_resource({})
        else:
            return JSONRPCResponse(
                id=request.id,
                error={
                    "code": JSONRPCErrorCode.INVALID_PARAMS.value,
                    "message": f"Unknown resource URI: {uri}"
                }
            )
        
        return JSONRPCResponse(
            id=request.id,
            result={
                "contents": [
                    {
                        "uri": uri,
                        "mimeType": "application/json",
                        "text": json.dumps(result, indent=2)
                    }
                ]
            }
        )
    
    async def _handle_prompts_list(self, request: JSONRPCRequest) -> JSONRPCResponse:
        """List available prompts"""
        prompts = [
            {
                "name": "query_help",
                "description": "Get help with query syntax and examples"
            }
        ]
        return JSONRPCResponse(id=request.id, result={"prompts": prompts})
    
    async def _handle_prompts_get(self, request: JSONRPCRequest) -> JSONRPCResponse:
        """Get a prompt"""
        params = request.params or {}
        prompt_name = params.get("name")
        
        if prompt_name == "query_help":
            result = await self._get_query_help_prompt({})
        else:
            return JSONRPCResponse(
                id=request.id,
                error={
                    "code": JSONRPCErrorCode.INVALID_PARAMS.value,
                    "message": f"Unknown prompt: {prompt_name}"
                }
            )
        
        return JSONRPCResponse(id=request.id, result=result)
    
    def _serialize_response(self, response: JSONRPCResponse) -> str:
        """Serialize JSON-RPC response"""
        data = {
            "jsonrpc": response.jsonrpc,
            "id": response.id
        }
        if response.error:
            data["error"] = response.error
        else:
            data["result"] = response.result
        
        return json.dumps(data) + "\n"
    
    # Tool handlers - wrap existing FastAPI tool functions
    async def _handle_list_sources(self, args: Dict[str, Any]) -> Dict[str, Any]:
        result = await list_sources()
        return result
    
    async def _handle_query_data(self, args: Dict[str, Any]) -> Dict[str, Any]:
        payload = QueryPayload(**args)
        result = await query_data(payload)
        return result
    
    async def _handle_query_api(self, args: Dict[str, Any]) -> Dict[str, Any]:
        req = QueryAPIRequest(**args)
        result = await query_api(req)
        return result
    
    async def _handle_transform_data(self, args: Dict[str, Any]) -> Dict[str, Any]:
        transform_spec = None
        if "transform_spec" in args:
            transform_spec = TransformSpec(**args["transform_spec"])
        
        req = TransformRequest(
            sql=args.get("sql"),
            rows=args.get("rows"),
            transform_spec=transform_spec
        )
        result = await transform_data(req)
        return result
    
    async def _handle_integrate_data(self, args: Dict[str, Any]) -> Dict[str, Any]:
        req = IntegrateRequest(**args)
        result = await integrate_data(req)
        return result
    
    async def _handle_export_data(self, args: Dict[str, Any]) -> Dict[str, Any]:
        transform_spec = None
        if "transform_spec" in args:
            transform_spec = TransformSpec(**args["transform_spec"])
        
        req = ExportRequest(
            sql=args.get("sql"),
            rows=args.get("rows"),
            format=args.get("format", "csv"),
            filename=args.get("filename"),
            include_summary=args.get("include_summary", False),
            include_visualization=args.get("include_visualization", False),
            transform_spec=transform_spec
        )
        result = await export_data(req)
        # For MCP, return JSON representation
        if hasattr(result, 'body'):
            # StreamingResponse - convert to JSON
            return {"success": True, "message": "Export generated", "format": req.format}
        return result
    
    async def _handle_analyze_schema(self, args: Dict[str, Any]) -> Dict[str, Any]:
        result = await analyze_schema(
            table_name=args.get("table_name"),
            question=args.get("question")
        )
        return result
    
    async def _handle_suggest_queries(self, args: Dict[str, Any]) -> Dict[str, Any]:
        result = await suggest_queries(use_case=args.get("use_case"))
        return result
    
    async def _handle_check_data_quality(self, args: Dict[str, Any]) -> Dict[str, Any]:
        req = DataQualityRequest(**args)
        result = await check_data_quality(req)
        return result
    
    async def _handle_list_files(self, args: Dict[str, Any]) -> Dict[str, Any]:
        directory = args.get("directory", "./")
        result = list_files(directory=directory)
        return result
    
    async def _handle_parse_file(self, args: Dict[str, Any]) -> Dict[str, Any]:
        # For MCP, we'll need file path or content
        file_path = args.get("file_path")
        file_content = args.get("file_content")
        
        if not file_path and not file_content:
            raise ValueError("Either file_path or file_content is required")
        
        # Read file if path provided
        if file_path and not file_content:
            import aiofiles
            async with aiofiles.open(file_path, 'rb') as f:
                content = await f.read()
        elif file_content:
            content = file_content.encode() if isinstance(file_content, str) else file_content
        else:
            raise ValueError("No file content available")
        
        # Parse file using file_connector logic
        import os
        import csv
        import json as json_lib
        from io import BytesIO
        import pandas as pd
        
        ext = os.path.splitext(file_path if file_path else "file")[1].lower()
        
        if ext == ".csv":
            decoded = content.decode()
            reader = csv.DictReader(decoded.splitlines())
            return {"rows": list(reader)}
        elif ext == ".json":
            decoded = content.decode()
            return json_lib.loads(decoded)
        elif ext in (".xls", ".xlsx"):
            df = pd.read_excel(BytesIO(content))
            return {"rows": df.to_dict(orient="records")}
        elif ext == ".xml":
            import xml.etree.ElementTree as ET
            root = ET.fromstring(content)
            def xml_to_dict(elem):
                return {elem.tag: {**elem.attrib, **{c.tag: xml_to_dict(c) for c in elem}} or elem.text}
            return xml_to_dict(root)
        else:
            raise ValueError(f"Unsupported file type: {ext}")
    
    async def _handle_search_users(self, args: Dict[str, Any]) -> Dict[str, Any]:
        query = args.get("query", "")
        if not query:
            raise ValueError("query is required")
        
        rows = await search_everywhere_users(query)
        return {
            "success": True,
            "rows": rows,
            "count": len(rows)
        }
    
    # Resource handlers
    async def _get_sources_resource(self, args: Dict[str, Any]) -> Dict[str, Any]:
        return await list_sources()
    
    async def _get_tables_resource(self, args: Dict[str, Any]) -> Dict[str, Any]:
        from app.db.explorer import list_tables
        tables = await list_tables()
        return {"tables": tables}
    
    # Prompt handlers
    async def _get_query_help_prompt(self, args: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "messages": [
                {
                    "role": "user",
                    "content": {
                        "type": "text",
                        "text": """# Query Help

## Supported Query Types

1. **User Queries**: Search users by id, name, email, region, signup_date
   - Example: "show user with name user21"
   - Example: "email user21@example.com and region EU"

2. **SQL Queries**: Natural language to SQL conversion
   - Example: "show all users from EU region"

3. **API Queries**: Query REST APIs
   - Example: "get data from api path /users"

4. **File Queries**: Parse CSV, JSON, Excel files
   - Example: "parse users.csv"

## Operators
- AND: Combine conditions (e.g., "region EU and signup_date 2025-01-22")
- OR: Alternative conditions (e.g., "region EU or region NA")

## Data Sources
- SQL Database (SQLite)
- REST API (with authentication)
- Local Files (CSV, JSON, Excel, XML)
"""
                    }
                }
            ]
        }


async def run_stdio_server():
    """Run MCP server over stdio (for CLI clients)"""
    server = MCPServer()
    logger.info("MCP Server starting (stdio mode)")
    
    # Read from stdin, write to stdout
    loop = asyncio.get_event_loop()
    
    async def process_input():
        while True:
            try:
                line = await loop.run_in_executor(None, sys.stdin.readline)
                if not line:
                    break
                
                line = line.strip()
                if not line:
                    continue
                
                response = await server.handle_request(line)
                if response:
                    sys.stdout.write(response)
                    sys.stdout.flush()
            
            except EOFError:
                break
            except Exception as e:
                logger.exception(f"Error processing input: {e}")
                error_response = json.dumps({
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {
                        "code": JSONRPCErrorCode.INTERNAL_ERROR.value,
                        "message": str(e)
                    }
                }) + "\n"
                sys.stdout.write(error_response)
                sys.stdout.flush()
    
    try:
        await process_input()
    except KeyboardInterrupt:
        logger.info("MCP Server shutting down")
    finally:
        logger.info("MCP Server stopped")


if __name__ == "__main__":
    asyncio.run(run_stdio_server())
