from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.services.sql_engine import execute_sql

router = APIRouter()


class TransformSpec(BaseModel):
    filter: Optional[str] = None  # pandas query string, e.g. 'region == "NA"'
    sort: Optional[List[str]] = None
    select: Optional[List[str]] = None
    rename: Optional[Dict[str, str]] = None
    groupby: Optional[List[str]] = None
    aggregations: Optional[Dict[str, str]] = None  # e.g. {"quantity":"sum"}
    limit: Optional[int] = None
    offset: Optional[int] = None


class TransformRequest(BaseModel):
    sql: Optional[str] = None
    rows: Optional[List[Dict[str, Any]]] = None
    transform_spec: Optional[TransformSpec] = None


@router.post("/transform_data")
async def transform_data(req: TransformRequest):
    # Get the data either by executing SQL or using provided rows
    if not req.sql and not req.rows:
        raise HTTPException(status_code=400, detail="Either `sql` or `rows` must be provided")

    data_rows = []
    if req.sql:
        exec_result = await execute_sql(req.sql)
        if not exec_result.get("success"):
            raise HTTPException(status_code=400, detail=exec_result.get("error"))
        data_rows = exec_result.get("rows", [])
    else:
        data_rows = req.rows or []

    # Try to use pandas if available, otherwise do simple operations
    try:
        import pandas as pd
    except Exception:
        pd = None

    spec = req.transform_spec

    if pd is None:
        # Minimal fallback: apply simple select and limit
        rows = data_rows
        if spec and spec.select:
            rows = [{k: r.get(k) for k in spec.select} for r in rows]
        if spec and spec.limit:
            rows = rows[spec.offset or 0 : (spec.offset or 0) + spec.limit]
        return {"success": True, "rows": rows, "columns": list(rows[0].keys()) if rows else []}

    # Use pandas for transformations
    df = pd.DataFrame(data_rows)

    if spec:
        if spec.select:
            df = df[spec.select]
        if spec.rename:
            df = df.rename(columns=spec.rename)
        if spec.filter:
            try:
                df = df.query(spec.filter)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"Invalid filter expression: {e}")
        if spec.sort:
            df = df.sort_values(by=spec.sort)
        if spec.groupby and spec.aggregations:
            try:
                df = df.groupby(spec.groupby).agg(spec.aggregations).reset_index()
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"Invalid aggregation: {e}")
        if spec.offset:
            df = df.iloc[spec.offset :]
        if spec.limit:
            df = df.iloc[: spec.limit]

    rows = df.to_dict(orient="records")
    columns = list(df.columns)

    return {"success": True, "columns": columns, "rows": rows}
