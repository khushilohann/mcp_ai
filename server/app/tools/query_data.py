from fastapi import APIRouter
from app.core.ollama_client import ask_ollama

router = APIRouter()

@router.post("/query_data")
async def query_data(payload: dict):
    question = payload.get("question")

    prompt = f"""
Convert this question into SQL:
Question: {question}
Return only SQL.
"""

    sql = ask_ollama(prompt)

    return {
        "question": question,
        "generated_sql": sql
    }
