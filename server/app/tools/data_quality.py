"""
Data Quality Check Tool
Detects anomalies, missing values, inconsistencies in data.
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from app.services.sql_engine import execute_sql
from app.db.explorer import list_tables, table_info
import pandas as pd

router = APIRouter()


class DataQualityRequest(BaseModel):
    sql: Optional[str] = None  # SQL query to check
    table_name: Optional[str] = None  # Table to check
    rows: Optional[List[Dict[str, Any]]] = None  # Direct data to check


@router.post("/check_data_quality")
async def check_data_quality(req: DataQualityRequest):
    """Check data quality: missing values, anomalies, inconsistencies."""
    try:
        # Get data
        data_rows = []
        if req.sql:
            exec_result = await execute_sql(req.sql)
            if not exec_result.get("success"):
                raise HTTPException(status_code=400, detail=exec_result.get("error"))
            data_rows = exec_result.get("rows", [])
        elif req.table_name:
            exec_result = await execute_sql(f"SELECT * FROM {req.table_name}")
            if not exec_result.get("success"):
                raise HTTPException(status_code=400, detail=f"Table '{req.table_name}' not found")
            data_rows = exec_result.get("rows", [])
        elif req.rows:
            data_rows = req.rows
        else:
            raise HTTPException(status_code=400, detail="Either sql, table_name, or rows must be provided")
        
        if not data_rows:
            return {
                "success": True,
                "checks": {
                    "missing_values": {},
                    "anomalies": [],
                    "inconsistencies": [],
                    "summary": "No data to check"
                }
            }
        
        # Convert to DataFrame for analysis
        try:
            df = pd.DataFrame(data_rows)
        except Exception:
            return {
                "success": False,
                "error": {"message": "Failed to convert data to DataFrame"}
            }
        
        checks = {
            "missing_values": {},
            "anomalies": [],
            "inconsistencies": [],
            "data_types": {},
            "statistics": {}
        }
        
        # Check for missing values
        missing_counts = df.isnull().sum()
        missing_percentages = (missing_counts / len(df) * 100).round(2)
        checks["missing_values"] = {
            col: {
                "count": int(missing_counts[col]),
                "percentage": float(missing_percentages[col])
            }
            for col in df.columns if missing_counts[col] > 0
        }
        
        # Check data types
        checks["data_types"] = {col: str(dtype) for col, dtype in df.dtypes.items()}
        
        # Statistical summary for numeric columns
        numeric_cols = df.select_dtypes(include=['number']).columns
        if len(numeric_cols) > 0:
            stats = df[numeric_cols].describe()
            checks["statistics"] = {
                col: {
                    "mean": float(stats.loc['mean', col]) if 'mean' in stats.index else None,
                    "std": float(stats.loc['std', col]) if 'std' in stats.index else None,
                    "min": float(stats.loc['min', col]) if 'min' in stats.index else None,
                    "max": float(stats.loc['max', col]) if 'max' in stats.index else None,
                    "median": float(df[col].median()) if col in numeric_cols else None
                }
                for col in numeric_cols
            }
        
        # Detect anomalies (outliers) for numeric columns
        for col in numeric_cols:
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)]
            if len(outliers) > 0:
                checks["anomalies"].append({
                    "column": col,
                    "type": "outlier",
                    "count": int(len(outliers)),
                    "threshold": {"lower": float(lower_bound), "upper": float(upper_bound)},
                    "sample_values": outliers[col].head(5).tolist()
                })
        
        # Check for inconsistencies
        # Check for duplicate rows
        duplicates = df.duplicated()
        if duplicates.any():
            checks["inconsistencies"].append({
                "type": "duplicate_rows",
                "count": int(duplicates.sum()),
                "description": f"{duplicates.sum()} duplicate rows found"
            })
        
        # Check for inconsistent formats (e.g., email, date patterns)
        for col in df.columns:
            # Check email format if column name suggests email
            if 'email' in col.lower():
                invalid_emails = df[~df[col].astype(str).str.contains(r'^[^@]+@[^@]+\.[^@]+', na=False, regex=True)]
                if len(invalid_emails) > 0:
                    checks["inconsistencies"].append({
                        "type": "invalid_format",
                        "column": col,
                        "count": int(len(invalid_emails)),
                        "description": f"Invalid email format in {len(invalid_emails)} rows",
                        "sample_values": invalid_emails[col].head(3).tolist()
                    })
            
            # Check date format if column name suggests date
            if 'date' in col.lower():
                # Try to parse dates
                try:
                    pd.to_datetime(df[col], errors='raise')
                except Exception:
                    checks["inconsistencies"].append({
                        "type": "invalid_format",
                        "column": col,
                        "count": len(df),
                        "description": f"Invalid date format in column {col}"
                    })
        
        # Summary
        total_issues = (
            len(checks["missing_values"]) +
            len(checks["anomalies"]) +
            len(checks["inconsistencies"])
        )
        
        checks["summary"] = {
            "total_rows": len(df),
            "total_columns": len(df.columns),
            "total_issues": total_issues,
            "quality_score": max(0, 100 - (total_issues * 10))  # Simple scoring
        }
        
        return {
            "success": True,
            "checks": checks,
            "data_sample": df.head(5).to_dict(orient="records")
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": {"message": str(e)}
        }


@router.get("/check_table_quality/{table_name}")
async def check_table_quality(table_name: str):
    """Quick quality check for a specific table."""
    req = DataQualityRequest(table_name=table_name)
    return await check_data_quality(req)
