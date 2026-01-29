from io import BytesIO
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse, Response, JSONResponse
from pydantic import BaseModel

from app.services.sql_engine import execute_sql

router = APIRouter()


class TransformSpec(BaseModel):
    filter: Optional[str] = None
    sort: Optional[List[str]] = None
    select: Optional[List[str]] = None
    rename: Optional[Dict[str, str]] = None
    groupby: Optional[List[str]] = None
    aggregations: Optional[Dict[str, str]] = None
    limit: Optional[int] = None
    offset: Optional[int] = None


class ExportRequest(BaseModel):
    sql: Optional[str] = None
    rows: Optional[List[Dict[str, Any]]] = None
    transform_spec: Optional[TransformSpec] = None
    format: Optional[str] = "csv"  # csv, json, xlsx
    filename: Optional[str] = None


@router.post("/export_data")
async def export_data(req: ExportRequest):
    if not req.sql and not req.rows:
        raise HTTPException(status_code=400, detail="Either `sql` or `rows` must be provided")

    if req.sql:
        exec_result = await execute_sql(req.sql)
        if not exec_result.get("success"):
            raise HTTPException(status_code=400, detail=exec_result.get("error"))
        data_rows = exec_result.get("rows", [])
    else:
        data_rows = req.rows or []

    # Use pandas if available for convenient export and transforms
    try:
        import pandas as pd
    except Exception:
        pd = None

    spec = req.transform_spec

    if pd is None:
        # Minimal processing and only JSON/CSV support
        if req.format not in ("json", "csv"):
            raise HTTPException(status_code=400, detail="xlsx export requires pandas and openpyxl")
        rows = data_rows
        if spec and spec.select:
            rows = [{k: r.get(k) for k in spec.select} for r in rows]
        if req.format == "json":
            return JSONResponse(content={"rows": rows})
        # csv fallback
        if rows:
            headers = list(rows[0].keys())
            lines = [",".join(headers)]
            for r in rows:
                lines.append(",".join(str(r.get(h, "")) for h in headers))
            csv_bytes = ("\n".join(lines)).encode("utf-8")
        else:
            csv_bytes = b""
        fname = req.filename or "export.csv"
        return Response(content=csv_bytes, media_type="text/csv", headers={"Content-Disposition": f"attachment; filename=\"{fname}\""})

    # Build DataFrame
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

    fmt = (req.format or "csv").lower()
    fname = req.filename or (f"export.{fmt}")

    if fmt == "json":
        return JSONResponse(content={"rows": df.to_dict(orient="records")})

    if fmt == "csv":
        buf = BytesIO()
        df.to_csv(buf, index=False)
        buf.seek(0)
        return StreamingResponse(buf, media_type="text/csv", headers={"Content-Disposition": f"attachment; filename=\"{fname}\""})

    if fmt == "xlsx":
        try:
            import openpyxl  # noqa: F401
        except Exception:
            raise HTTPException(status_code=500, detail="XLSX export requires openpyxl (`pip install openpyxl`)")
        buf = BytesIO()
        df.to_excel(buf, index=False)
        buf.seek(0)
        return StreamingResponse(buf, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={"Content-Disposition": f"attachment; filename=\"{fname}\""})

    raise HTTPException(status_code=400, detail="Unsupported format")
