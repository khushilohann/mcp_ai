from io import BytesIO
from typing import Any, Dict, List, Optional
import json

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
    format: Optional[str] = "csv"  # csv, json, xlsx, report
    filename: Optional[str] = None
    include_summary: Optional[bool] = False  # Generate summary report
    include_visualization: Optional[bool] = False  # Generate visualization data


def _generate_summary_report(df, data_rows: List[Dict]) -> Dict[str, Any]:
    """Generate a summary report of the data."""
    try:
        import pandas as pd
    except Exception:
        pd = None
    
    if pd is None:
        # Basic summary without pandas
        summary = {
            "total_rows": len(data_rows),
            "total_columns": len(data_rows[0].keys()) if data_rows else 0,
            "columns": list(data_rows[0].keys()) if data_rows else []
        }
        return summary
    
    summary = {
        "total_rows": len(df),
        "total_columns": len(df.columns),
        "columns": list(df.columns),
        "data_types": {col: str(dtype) for col, dtype in df.dtypes.items()},
        "missing_values": {
            col: int(df[col].isnull().sum()) for col in df.columns
        }
    }
    
    # Add statistics for numeric columns
    numeric_cols = df.select_dtypes(include=['number']).columns
    if len(numeric_cols) > 0:
        summary["statistics"] = {}
        for col in numeric_cols:
            summary["statistics"][col] = {
                "mean": float(df[col].mean()),
                "median": float(df[col].median()),
                "std": float(df[col].std()),
                "min": float(df[col].min()),
                "max": float(df[col].max())
            }
    
    # Add value counts for categorical columns (top 10)
    categorical_cols = df.select_dtypes(include=['object']).columns
    if len(categorical_cols) > 0:
        summary["value_counts"] = {}
        for col in categorical_cols[:5]:  # Limit to first 5 categorical columns
            value_counts = df[col].value_counts().head(10)
            summary["value_counts"][col] = {
                k: int(v) for k, v in value_counts.items()
            }
    
    return summary


def _generate_visualization_data(df, data_rows: List[Dict]) -> Dict[str, Any]:
    """Generate visualization data (chart-ready format)."""
    try:
        import pandas as pd
    except Exception:
        pd = None
    
    if pd is None:
        return {"error": "Visualization requires pandas"}
    
    viz_data = {
        "charts": []
    }
    
    # Generate bar chart data for categorical columns
    categorical_cols = df.select_dtypes(include=['object']).columns[:3]  # Top 3
    for col in categorical_cols:
        value_counts = df[col].value_counts().head(10)
        viz_data["charts"].append({
            "type": "bar",
            "title": f"Distribution of {col}",
            "data": {
                "labels": list(value_counts.index.astype(str)),
                "values": [int(v) for v in value_counts.values]
            }
        })
    
    # Generate line/bar chart for numeric columns grouped by categorical
    numeric_cols = df.select_dtypes(include=['number']).columns[:2]  # Top 2
    categorical_cols = df.select_dtypes(include=['object']).columns[:1]  # Top 1
    
    if len(numeric_cols) > 0 and len(categorical_cols) > 0:
        cat_col = categorical_cols[0]
        num_col = numeric_cols[0]
        grouped = df.groupby(cat_col)[num_col].agg(['mean', 'sum']).head(10)
        viz_data["charts"].append({
            "type": "bar",
            "title": f"{num_col} by {cat_col}",
            "data": {
                "labels": list(grouped.index.astype(str)),
                "values": [float(v) for v in grouped['mean']]
            }
        })
    
    # Generate pie chart for single categorical column
    if len(categorical_cols) > 0:
        cat_col = categorical_cols[0]
        value_counts = df[cat_col].value_counts().head(8)
        viz_data["charts"].append({
            "type": "pie",
            "title": f"Distribution of {cat_col}",
            "data": {
                "labels": list(value_counts.index.astype(str)),
                "values": [int(v) for v in value_counts.values]
            }
        })
    
    return viz_data


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
        
        # Generate summary if requested
        summary = None
        if req.include_summary:
            summary = _generate_summary_report(None, rows)
        
        if req.format == "json":
            response_data = {"rows": rows}
            if summary:
                response_data["summary"] = summary
            return JSONResponse(content=response_data)
        
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

    # Generate summary report if requested
    summary = None
    if req.include_summary:
        summary = _generate_summary_report(df, data_rows)
    
    # Generate visualization data if requested
    visualization = None
    if req.include_visualization:
        visualization = _generate_visualization_data(df, data_rows)

    # Handle report format
    if fmt == "report":
        report = {
            "data": df.to_dict(orient="records"),
            "summary": summary or _generate_summary_report(df, data_rows),
            "visualization": visualization or _generate_visualization_data(df, data_rows) if req.include_visualization else None
        }
        return JSONResponse(content=report)

    if fmt == "json":
        response_data = {"rows": df.to_dict(orient="records")}
        if summary:
            response_data["summary"] = summary
        if visualization:
            response_data["visualization"] = visualization
        return JSONResponse(content=response_data)

    if fmt == "csv":
        buf = BytesIO()
        df.to_csv(buf, index=False)
        buf.seek(0)
        return StreamingResponse(buf, media_type="text/csv", headers={"Content-Disposition": f"attachment; filename=\"{fname}\""})

    if fmt == "xlsx":
        try:
            import openpyxl  # noqa: F401
            from openpyxl import Workbook
            from openpyxl.styles import Font, PatternFill
        except Exception:
            raise HTTPException(status_code=500, detail="XLSX export requires openpyxl (`pip install openpyxl`)")
        
        buf = BytesIO()
        with pd.ExcelWriter(buf, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Data', index=False)
            
            # Add summary sheet if requested
            if req.include_summary and summary:
                summary_df = pd.DataFrame([summary])
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        buf.seek(0)
        return StreamingResponse(
            buf,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename=\"{fname}\""}
        )

    raise HTTPException(status_code=400, detail="Unsupported format")
