from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from app.core.ollama_client import ask_ollama
from app.services.sql_engine import execute_sql
from app.db.explorer import explain_query

router = APIRouter()


class QueryPayload(BaseModel):
    question: str


@router.post("/query_data")
async def query_data(payload: QueryPayload):
    question = payload.question
    if not question or not question.strip():
        raise HTTPException(status_code=400, detail="`question` is required")

    prompt = f"""
Convert this question into SQL:
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
