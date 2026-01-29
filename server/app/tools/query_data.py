from typing import Optional, List, Dict, Any
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from app.core.ollama_client import ask_ollama
from app.services.sql_engine import execute_sql
from app.db.explorer import explain_query, list_tables, table_info
from app.tools.query_api import query_api, QueryAPIRequest
from app.tools.file_connector import parse_file
import os

router = APIRouter()


class QueryPayload(BaseModel):
    question: str
    sources: Optional[List[str]] = None  # e.g., ["sql", "api", "file"] for cross-source queries


async def _get_schema_info():
    """Get schema information from SQL database."""
    tables = await list_tables()
    schema_info = {}
    for table in tables:
        info = await table_info(table)
        schema_info[table] = [{"name": row[1], "type": row[2], "notnull": row[3], "pk": row[5]} for row in info]
    return schema_info


async def _execute_cross_source_query(question: str, sources: List[str]):
    """Execute queries across multiple data sources and join results."""
    results = {}
    
    # Get schema info for SQL
    schema_info = await _get_schema_info()
    
    # Build a prompt that considers multiple sources
    schema_prompt = ""
    if "sql" in sources:
        schema_prompt += f"\nSQL Schema: {schema_info}"
    if "api" in sources:
        schema_prompt += "\nREST API available at /users, /orders, etc."
    if "file" in sources:
        schema_prompt += "\nFile sources: CSV, JSON, Excel files available"
    
    # Use LLM to determine what to query from each source
    prompt = f"""
    Analyze this question and determine which data sources to query: {question}
    
    Available sources: {', '.join(sources)}
    {schema_prompt}
    
    Return a JSON object with:
    - "sql_query": SQL query if SQL source is needed, or null
    - "api_calls": List of API paths to call, or []
    - "file_paths": List of file paths to read, or []
    - "join_strategy": How to combine results (e.g., "merge", "join_on_id", "concatenate")
    - "join_keys": Keys to join on if applicable
    
    Return ONLY valid JSON, no other text.
    """
    
    try:
        llm_response = ask_ollama(prompt)
        # Try to parse JSON from response (might need cleaning)
        import json
        import re
        json_match = re.search(r'\{.*\}', llm_response, re.DOTALL)
        if json_match:
            plan = json.loads(json_match.group())
        else:
            # Fallback: simple extraction
            plan = {"sql_query": None, "api_calls": [], "file_paths": [], "join_strategy": "concatenate", "join_keys": []}
    except Exception as e:
        # Fallback plan
        plan = {"sql_query": None, "api_calls": [], "file_paths": [], "join_strategy": "concatenate", "join_keys": []}
    
    # Execute SQL query if needed
    if plan.get("sql_query") and "sql" in sources:
        sql_result = await execute_sql(plan["sql_query"])
        if sql_result.get("success"):
            results["sql"] = sql_result.get("rows", [])
    
    # Execute API calls if needed
    if plan.get("api_calls") and "api" in sources:
        api_results = []
        for api_path in plan["api_calls"]:
            try:
                api_req = QueryAPIRequest(method="GET", path=api_path)
                api_resp = await query_api(api_req)
                if api_resp.get("success"):
                    data = api_resp.get("data", [])
                    if isinstance(data, list):
                        api_results.extend(data)
                    elif isinstance(data, dict):
                        api_results.append(data)
            except Exception:
                pass
        if api_results:
            results["api"] = api_results
    
    # Read files if needed
    if plan.get("file_paths") and "file" in sources:
        file_results = []
        for file_path in plan["file_paths"]:
            try:
                # This would need file upload or path handling
                # For now, skip file reading in cross-source queries
                pass
            except Exception:
                pass
        if file_results:
            results["file"] = file_results
    
    # Join results based on strategy
    all_rows = []
    for source_data in results.values():
        if isinstance(source_data, list):
            all_rows.extend(source_data)
    
    # Simple join logic - can be enhanced
    if plan.get("join_strategy") == "join_on_id" and plan.get("join_keys"):
        # Basic join implementation
        joined = {}
        for row in all_rows:
            key = tuple(row.get(k) for k in plan["join_keys"] if k in row)
            if key not in joined:
                joined[key] = {}
            joined[key].update(row)
        all_rows = list(joined.values())
    
    return {
        "success": True,
        "rows": all_rows,
        "columns": list(set(k for row in all_rows for k in row.keys())) if all_rows else [],
        "sources_used": list(results.keys()),
        "join_strategy": plan.get("join_strategy", "concatenate")
    }


@router.post("/query_data")
async def query_data(payload: QueryPayload):
    question = payload.question
    if not question or not question.strip():
        raise HTTPException(status_code=400, detail="`question` is required")
    
    sources = payload.sources or ["sql"]  # Default to SQL only
    
    # If multiple sources specified, use cross-source query
    if len(sources) > 1:
        cross_result = await _execute_cross_source_query(question, sources)
        return {
            "success": cross_result.get("success", False),
            "question": question,
            "sources": sources,
            "execution": cross_result,
        }
    
    # Single source query (original logic)
    prompt = f"""
Convert this question into SQL. IMPORTANT: Do NOT add LIMIT clause unless explicitly requested.
Question: {question}
Return only SQL.
"""

    try:
        sql = ask_ollama(prompt)
    except Exception as e:
        return {"success": False, "error": {"message": "LLM generation failed", "details": str(e)}}

    if not sql or not sql.strip():
        return {"success": False, "error": {"message": "LLM returned empty SQL"}}

    # Execute SQL against local SQLite for now (only SELECT allowed)
    exec_result = await execute_sql(sql)

    # Get explanation / query plan (best-effort)
    try:
        plan = await explain_query(sql)
    except Exception:
        plan = None

    response = {
        "success": exec_result.get("success", False),
        "question": question,
        "generated_sql": sql,
        "execution": exec_result,
        "explain": plan,
    }
    return response
