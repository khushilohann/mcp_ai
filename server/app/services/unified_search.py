import os
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd

from app.services.sql_engine import execute_sql
from app.tools.query_api import QueryAPIRequest, query_api


SERVER_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DEFAULT_FILES = [
    os.path.join(SERVER_ROOT, "users.csv"),
    os.path.join(SERVER_ROOT, "users.xlsx"),
]

REGIONS = {"na", "eu", "apac", "latam"}


@dataclass(frozen=True)
class Cond:
    field: str  # id|name|email|region|signup_date|any|signup_date_range
    op: str  # eq|like|range
    value: Any


DNF = List[List[Cond]]  # OR of ANDs


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip().lower())


def _parse_last_month_range(today: Optional[date] = None) -> Tuple[str, str]:
    """Return [start, end) for last month as ISO dates."""
    d = today or date.today()
    first_this_month = d.replace(day=1)
    # go to last day of previous month then back to first of that month
    last_prev_month = first_this_month - pd.Timedelta(days=1)
    first_prev_month = last_prev_month.replace(day=1)
    return first_prev_month.strftime("%Y-%m-%d"), first_this_month.strftime("%Y-%m-%d")


def parse_query_to_dnf(query: str) -> DNF:
    """
    Very small, deterministic parser for queries like:
      - "email user21@example.com"
      - "region EU and signup_date 2025-01-22"
      - "name user21 or name user22"
      - "signed up last month and region NA"
    """
    q = _norm(query)
    # normalize punctuation but keep @ . - _
    q = re.sub(r"[^a-z0-9@\.\-_\s]", " ", q)
    q = re.sub(r"\s+", " ", q).strip()

    or_parts = [p.strip() for p in re.split(r"\s+or\s+", q) if p.strip()]
    dnf: DNF = []

    for part in or_parts:
        and_parts = [p.strip() for p in re.split(r"\s+and\s+", part) if p.strip()]
        clause: List[Cond] = []
        for token in and_parts:
            # last month shortcut
            if "last month" in token or "previous month" in token:
                start, end = _parse_last_month_range()
                clause.append(Cond(field="signup_date", op="range", value=(start, end)))
                continue

            # email
            m = re.search(r"([a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,})", token)
            if m:
                clause.append(Cond(field="email", op="eq", value=m.group(1)))
                continue

            # explicit id
            m = re.search(r"\b(?:user\s+)?id\s*(?:=|is)?\s*(\d+)\b", token)
            if m:
                clause.append(Cond(field="id", op="eq", value=int(m.group(1))))
                continue

            # explicit signup_date
            m = re.search(r"\b(?:signup_date|signup|signed up|date)\s*(?:=|is|on)?\s*(\d{4}-\d{2}-\d{2})\b", token)
            if m:
                clause.append(Cond(field="signup_date", op="eq", value=m.group(1)))
                continue

            # region
            m = re.search(r"\b(?:region\s*)?(na|eu|apac|latam)\b", token)
            if m and (("region" in token) or (m.group(1) in REGIONS)):
                clause.append(Cond(field="region", op="eq", value=m.group(1).upper()))
                continue

            # explicit name
            m = re.search(r"\b(?:name\s*(?:=|is)?\s*|user\s*(?:with\s+name\s+)?)([a-z0-9_]+)\b", token)
            if m:
                clause.append(Cond(field="name", op="eq", value=m.group(1)))
                continue

            # fallback "any" search
            val = token.strip()
            if val:
                clause.append(Cond(field="any", op="like", value=val))

        if clause:
            dnf.append(clause)

    return dnf or [[Cond(field="any", op="like", value=q)]]


def row_matches_dnf(row: Dict[str, Any], dnf: DNF) -> bool:
    r = {str(k).lower(): row.get(k) for k in row.keys()}

    def match_cond(c: Cond) -> bool:
        if c.op == "range" and c.field == "signup_date":
            v = r.get("signup_date")
            if not v:
                return False
            start, end = c.value
            return str(v) >= start and str(v) < end

        if c.field == "any" and c.op == "like":
            needle = str(c.value).lower()
            for key in ("id", "name", "email", "region", "signup_date"):
                val = r.get(key)
                if val is None:
                    continue
                if needle in str(val).lower():
                    return True
            return False

        val = r.get(c.field)
        if val is None:
            return False
        if c.op == "eq":
            if c.field == "id":
                try:
                    return int(val) == int(c.value)
                except Exception:
                    return False
            return str(val).lower() == str(c.value).lower()
        if c.op == "like":
            return str(c.value).lower() in str(val).lower()
        return False

    # OR of ANDs
    for clause in dnf:
        if all(match_cond(c) for c in clause):
            return True
    return False


def _build_sql_where_from_dnf(dnf: DNF) -> Tuple[str, Tuple[Any, ...]]:
    """Convert DNF to a parameterized SQLite WHERE clause."""
    or_sql: List[str] = []
    params: List[Any] = []

    for clause in dnf:
        and_sql: List[str] = []
        for c in clause:
            if c.op == "range" and c.field == "signup_date":
                and_sql.append("(signup_date >= ? AND signup_date < ?)")
                start, end = c.value
                params.extend([start, end])
                continue

            if c.field == "any" and c.op == "like":
                and_sql.append(
                    "("
                    "CAST(id AS TEXT) LIKE ? OR "
                    "lower(name) LIKE lower(?) OR "
                    "lower(email) LIKE lower(?) OR "
                    "lower(region) LIKE lower(?) OR "
                    "signup_date LIKE ?"
                    ")"
                )
                like = f"%{c.value}%"
                params.extend([like, like, like, like, like])
                continue

            if c.op == "eq":
                if c.field == "id":
                    and_sql.append("id = ?")
                    params.append(int(c.value))
                else:
                    and_sql.append(f"lower({c.field}) = lower(?)")
                    params.append(str(c.value))
                continue

            if c.op == "like":
                and_sql.append(f"lower({c.field}) LIKE lower(?)")
                params.append(f"%{c.value}%")
                continue

        or_sql.append("(" + " AND ".join(and_sql) + ")")

    where = " OR ".join(or_sql) if or_sql else "1=1"
    return where, tuple(params)


async def search_sql_users(query: str, dnf: Optional[DNF] = None, limit: int = 200) -> List[Dict[str, Any]]:
    dnf = dnf or parse_query_to_dnf(query)
    where, params = _build_sql_where_from_dnf(dnf)
    sql = f"SELECT id, name, email, region, signup_date FROM users WHERE {where}"
    res = await execute_sql(sql, params=params, limit=limit)
    if not res.get("success"):
        return []
    rows = res.get("rows", [])
    for r in rows:
        r["source"] = "sql"
    return rows


def _read_users_file(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        return []
    ext = os.path.splitext(path)[1].lower()
    try:
        if ext == ".csv":
            df = pd.read_csv(path)
        elif ext in (".xlsx", ".xls"):
            df = pd.read_excel(path)
        elif ext == ".json":
            df = pd.read_json(path)
        else:
            return []
    except Exception:
        return []

    if df is None or df.empty:
        return []
    # normalize columns
    cols = {c.lower(): c for c in df.columns}
    want = ["id", "name", "email", "region", "signup_date"]
    rows: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        out = {}
        for w in want:
            if w in cols:
                out[w] = row[cols[w]]
        # coerce id to int if possible
        if "id" in out:
            try:
                out["id"] = int(out["id"])
            except Exception:
                pass
        rows.append(out)
    return rows


async def search_file_users(query: str, dnf: Optional[DNF] = None, files: Optional[Sequence[str]] = None) -> List[Dict[str, Any]]:
    dnf = dnf or parse_query_to_dnf(query)
    files = list(files or DEFAULT_FILES)
    out: List[Dict[str, Any]] = []
    for fpath in files:
        rows = _read_users_file(fpath)
        rows = [r for r in rows if row_matches_dnf(r, dnf)]
        for r in rows:
            r["source"] = f"file:{os.path.basename(fpath)}"
        out.extend(rows)
    return out


async def search_api_users(query: str, dnf: Optional[DNF] = None) -> List[Dict[str, Any]]:
    dnf = dnf or parse_query_to_dnf(query)

    # Ask mock API for /users; we keep it simple and filter locally to support AND/OR.
    req = QueryAPIRequest(method="GET", path="/users", api_key=os.getenv("MOCK_API_KEY", "demo-key"))
    resp = await query_api(req)
    if not resp.get("success"):
        return []
    data = resp.get("data") or []
    if not isinstance(data, list):
        return []

    rows = [r for r in data if isinstance(r, dict) and row_matches_dnf(r, dnf)]
    for r in rows:
        r["source"] = "api"
    return rows


def dedupe_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Dedupe by email if present, otherwise by (name, id). Also merge sources.
    """
    merged: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        email = str(r.get("email") or "").strip().lower()
        key = email if email else f"{str(r.get('name') or '').lower()}::{r.get('id')}"
        if key not in merged:
            rr = dict(r)
            rr["sources"] = [r.get("source")] if r.get("source") else []
            rr.pop("source", None)
            merged[key] = rr
        else:
            cur = merged[key]
            src = r.get("source")
            if src and src not in cur.get("sources", []):
                cur["sources"].append(src)
            # fill missing fields if present
            for k, v in r.items():
                if k in ("source",):
                    continue
                if cur.get(k) in (None, "", "nan") and v not in (None, "", "nan"):
                    cur[k] = v
    # make sources string for tabular display
    out = list(merged.values())
    for r in out:
        r["sources"] = ", ".join([s for s in r.get("sources", []) if s])
    return out


async def search_everywhere_users(query: str) -> List[Dict[str, Any]]:
    dnf = parse_query_to_dnf(query)
    sql_rows, api_rows, file_rows = await _gather_all_sources(query, dnf)
    return dedupe_rows(sql_rows + api_rows + file_rows)


async def _gather_all_sources(query: str, dnf: DNF):
    # keep sequential to avoid saturating local services in small demos
    sql_rows = await search_sql_users(query, dnf)
    api_rows = await search_api_users(query, dnf)
    file_rows = await search_file_users(query, dnf)
    return sql_rows, api_rows, file_rows

