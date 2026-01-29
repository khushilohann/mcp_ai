import os
import csv
import json
import xml.etree.ElementTree as ET
import pandas as pd
from fastapi import APIRouter, HTTPException, UploadFile, File, Query
from typing import List, Dict, Any, Optional

router = APIRouter()

@router.get("/list_files")
def list_files(directory: str = Query("./", description="Directory to list files from")):
    if not os.path.isdir(directory):
        raise HTTPException(status_code=400, detail="Directory does not exist")
    files = []
    for root, dirs, filenames in os.walk(directory):
        for fname in filenames:
            files.append(os.path.relpath(os.path.join(root, fname), directory))
    return {"files": files}

@router.post("/parse_file")
async def parse_file(file: UploadFile = File(...)):
    ext = os.path.splitext(file.filename)[1].lower()
    content = await file.read()
    if ext == ".csv":
        decoded = content.decode()
        reader = csv.DictReader(decoded.splitlines())
        return {"rows": list(reader)}
    elif ext == ".json":
        decoded = content.decode()
        return json.loads(decoded)
    elif ext in (".xls", ".xlsx"):
        df = pd.read_excel(BytesIO(content))
        return {"rows": df.to_dict(orient="records")}
    elif ext == ".xml":
        root = ET.fromstring(content)
        def xml_to_dict(elem):
            return {elem.tag: {**elem.attrib, **{c.tag: xml_to_dict(c) for c in elem}} or elem.text}
        return xml_to_dict(root)
    else:
        raise HTTPException(status_code=400, detail="Unsupported file type")
