"""
Smart Schema Understanding Tool
Uses AI to analyze database schemas and suggest optimal queries.
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from app.core.ollama_client import ask_ollama
from app.db.explorer import list_tables, table_info

router = APIRouter()


class SchemaAnalysisRequest(BaseModel):
    table_name: Optional[str] = None  # If None, analyze all tables
    question: Optional[str] = None  # Optional: analyze schema in context of a question


@router.get("/analyze_schema")
async def analyze_schema(table_name: Optional[str] = None, question: Optional[str] = None):
    """Analyze database schema and provide insights and query suggestions."""
    try:
        tables = await list_tables()
        
        if table_name and table_name not in tables:
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
        
        tables_to_analyze = [table_name] if table_name else tables
        
        schema_info = {}
        for table in tables_to_analyze:
            info = await table_info(table)
            schema_info[table] = [
                {
                    "name": row[1],
                    "type": row[2],
                    "notnull": bool(row[3]),
                    "primary_key": bool(row[5]),
                    "default": row[4]
                }
                for row in info
            ]
        
        # Build schema description for LLM
        schema_description = "\n".join([
            f"Table: {table}\n" + "\n".join([
                f"  - {col['name']} ({col['type']})" + 
                (" PRIMARY KEY" if col['primary_key'] else "") +
                (" NOT NULL" if col['notnull'] else "")
                for col in cols
            ])
            for table, cols in schema_info.items()
        ])
        
        # Use LLM to analyze schema and suggest queries
        if question:
            prompt = f"""
            Given this database schema:
            {schema_description}
            
            And this question: {question}
            
            Provide:
            1. Analysis of which tables and columns are relevant
            2. Suggested optimal SQL query
            3. Explanation of why this query is optimal
            4. Alternative query approaches if applicable
            
            Format as JSON with keys: "relevant_tables", "suggested_query", "explanation", "alternatives"
            """
        else:
            prompt = f"""
            Analyze this database schema and provide insights:
            {schema_description}
            
            Provide:
            1. Overview of the schema structure
            2. Key relationships between tables (if multiple tables)
            3. Recommended queries for common use cases
            4. Performance optimization suggestions
            5. Data quality considerations
            
            Format as JSON with keys: "overview", "relationships", "recommended_queries", "optimization_tips", "data_quality_notes"
            """
        
        try:
            llm_response = ask_ollama(prompt)
            # Try to extract JSON from response
            import json
            import re
            json_match = re.search(r'\{.*\}', llm_response, re.DOTALL)
            if json_match:
                analysis = json.loads(json_match.group())
            else:
                # Fallback: return raw response
                analysis = {"raw_analysis": llm_response}
        except Exception as e:
            analysis = {"error": f"Failed to parse LLM response: {str(e)}", "raw_response": llm_response}
        
        return {
            "success": True,
            "schema": schema_info,
            "analysis": analysis,
            "tables_analyzed": tables_to_analyze
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": {"message": str(e)}
        }


@router.get("/suggest_queries")
async def suggest_queries(use_case: Optional[str] = None):
    """Get query suggestions based on schema analysis."""
    try:
        tables = await list_tables()
        schema_info = {}
        for table in tables:
            info = await table_info(table)
            schema_info[table] = [
                {"name": row[1], "type": row[2], "primary_key": bool(row[5])}
                for row in info
            ]
        
        schema_description = "\n".join([
            f"Table: {table}\nColumns: {', '.join([c['name'] for c in cols])}"
            for table, cols in schema_info.items()
        ])
        
        prompt = f"""
        Based on this database schema:
        {schema_description}
        
        {"Use case: " + use_case if use_case else "Provide common use cases and queries:"}
        
        Suggest 5-10 useful SQL queries that would be valuable for this schema.
        For each query, provide:
        - The SQL query
        - What it does
        - When to use it
        
        Format as JSON array with objects containing: "query", "description", "use_case"
        """
        
        try:
            llm_response = ask_ollama(prompt)
            import json
            import re
            json_match = re.search(r'\[.*\]', llm_response, re.DOTALL)
            if json_match:
                suggestions = json.loads(json_match.group())
            else:
                suggestions = [{"query": "SELECT * FROM " + tables[0] + " LIMIT 10", "description": "Basic query", "use_case": "View data"}]
        except Exception:
            suggestions = []
        
        return {
            "success": True,
            "suggestions": suggestions,
            "schema": schema_info
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": {"message": str(e)}
        }
