import aiosqlite
from app.db.session import DB_PATH

async def execute_sql(sql: str, params: tuple = None, limit: int = 1000):
    """Execute a SELECT SQL statement safely and return structured results.

    Only SELECT is allowed in this demo to avoid modifying data.
    """
    if not sql:
        return {"success": False, "error": "Empty SQL"}

    # Basic safety: allow only SELECT statements
    s = sql.strip().lower()
    if not s.startswith("select"):
        return {"success": False, "error": "Only SELECT queries are allowed in this demo."}

    # Ensure no runaway results by appending a LIMIT if not present
    sql_clean = sql.strip().rstrip(";")
    if "limit" not in s:
        sql_clean = f"{sql_clean} LIMIT {limit}"

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(sql_clean, params or ())
        rows = await cur.fetchall()

    results = [dict(r) for r in rows]
    columns = list(rows[0].keys()) if rows else []

    return {"success": True, "columns": columns, "rows": results}
