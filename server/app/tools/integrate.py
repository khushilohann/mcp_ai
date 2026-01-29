from typing import List, Dict, Any, Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import pandas as pd

router = APIRouter()

class IntegrateRequest(BaseModel):
	sources: List[List[Dict[str, Any]]]
	join_on: Optional[List[str]] = None  # columns to join on
	deduplicate_on: Optional[List[str]] = None  # columns to deduplicate on
	conflict_strategy: Optional[str] = "prefer_first"  # or "prefer_last", "merge"

@router.post("/integrate_data")
async def integrate_data(req: IntegrateRequest):
	if not req.sources or len(req.sources) < 2:
		raise HTTPException(status_code=400, detail="At least two sources are required for integration.")

	# Convert all sources to DataFrames
	dfs = [pd.DataFrame(src) for src in req.sources]

	# Schema alignment: union columns
	all_columns = set()
	for df in dfs:
		all_columns.update(df.columns)
	dfs = [df.reindex(columns=all_columns) for df in dfs]

	# Concatenate all data
	combined = pd.concat(dfs, ignore_index=True)

	# Deduplication
	if req.deduplicate_on:
		keep = "first" if req.conflict_strategy == "prefer_first" else "last"
		combined = combined.drop_duplicates(subset=req.deduplicate_on, keep=keep)

	# Conflict resolution (simple: prefer_first or prefer_last)
	# More advanced merging can be added here

	# Return as list of dicts
	return {"success": True, "rows": combined.fillna("").to_dict(orient="records")}
