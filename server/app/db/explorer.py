import aiosqlite
from .session import DB_PATH

async def list_tables():
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT name FROM sqlite_master WHERE type='table'")
        rows = await cur.fetchall()
    return [r[0] for r in rows]


async def table_info(table_name: str):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(f"PRAGMA table_info({table_name})")
        rows = await cur.fetchall()
    # Return list of (cid, name, type, notnull, dflt_value, pk)
    return rows


async def explain_query(sql: str):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(f"EXPLAIN QUERY PLAN {sql}")
        rows = await cur.fetchall()
    return rows
