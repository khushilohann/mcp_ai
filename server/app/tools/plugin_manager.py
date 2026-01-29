import importlib
import os
from fastapi import APIRouter, HTTPException
from typing import List

router = APIRouter()

PLUGIN_FOLDER = os.getenv("PLUGIN_FOLDER", "plugins")

@router.get("/list_plugins")
def list_plugins():
    if not os.path.isdir(PLUGIN_FOLDER):
        return {"plugins": []}
    plugins = [f[:-3] for f in os.listdir(PLUGIN_FOLDER) if f.endswith(".py") and not f.startswith("__")]
    return {"plugins": plugins}

@router.post("/load_plugin")
def load_plugin(name: str):
    try:
        module = importlib.import_module(f"plugins.{name}")
        if hasattr(module, "register"):
            module.register()
        return {"success": True, "message": f"Plugin {name} loaded."}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to load plugin: {e}")
