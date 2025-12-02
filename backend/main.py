from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import List
import os

from excel_service import (
    list_excel_files,
    list_sheets,
    preview_sheet,
    union_columns_for_sheet,
    merge_columns,
    OUTPUT_DIR,
)

app = FastAPI(title="Mini Excel Platform Backend")

# Allow frontend (opened from file:// or localhost) to call API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # for local dev, it's fine
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class MergeRequest(BaseModel):
    sheet_name: str
    columns: List[str]

from typing import Literal, Optional

class WorkflowNode(BaseModel):
    id: str
    type: Literal["select_files", "select_sheet", "select_columns", "merge_columns"]
    # For select_files
    files: Optional[List[str]] = None
    # For select_sheet / select_columns
    sheet_name: Optional[str] = None
    # For select_columns
    columns: Optional[List[str]] = None

class WorkflowRequest(BaseModel):
    nodes: List[WorkflowNode]

@app.get("/files")
def get_files():
    return {"files": list_excel_files()}


@app.get("/sheets")
def get_sheets(filename: str):
    try:
        sheets = list_sheets(filename)
        return {"filename": filename, "sheets": sheets}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/workflow/run")
def run_workflow(req: WorkflowRequest):
    """
    Very simple workflow executor, now with file selection:

    Supported node types (executed in order of appearance in req.nodes):
      - "select_files": set current_files
      - "select_sheet": set current_sheet
      - "select_columns": set current_columns
      - "merge_columns": call merge_columns(current_sheet, current_columns, current_files)

    current_files:
      * if None  -> use all Excel files
      * if list -> restrict to those files
    """
    from typing import Optional

    current_files: Optional[List[str]] = None
    current_sheet: Optional[str] = None
    current_columns: List[str] = []

    for node in req.nodes:
        if node.type == "select_files":
            if not node.files:
                raise HTTPException(
                    status_code=400,
                    detail=f"Node {node.id}: files are required for select_files",
                )
            # Optional: validate these filenames exist
            all_files = set(list_excel_files())
            missing = [f for f in node.files if f not in all_files]
            if missing:
                raise HTTPException(
                    status_code=400,
                    detail=f"Node {node.id}: unknown files: {missing}",
                )
            current_files = list(node.files)

        elif node.type == "select_sheet":
            if not node.sheet_name:
                raise HTTPException(
                    status_code=400,
                    detail=f"Node {node.id}: sheet_name is required for select_sheet",
                )
            current_sheet = node.sheet_name

        elif node.type == "select_columns":
            if not node.columns:
                raise HTTPException(
                    status_code=400,
                    detail=f"Node {node.id}: columns are required for select_columns",
                )
            current_columns = list(node.columns)

        elif node.type == "merge_columns":
            if not current_sheet:
                raise HTTPException(
                    status_code=400,
                    detail="No sheet selected before merge_columns",
                )
            if not current_columns:
                raise HTTPException(
                    status_code=400,
                    detail="No columns selected before merge_columns",
                )
            try:
                out_name = merge_columns(
                    current_sheet,
                    current_columns,
                    files=current_files,
                )
                return {
                    "status": "ok",
                    "output_filename": out_name,
                    "sheet_name": current_sheet,
                    "columns": current_columns,
                    "files": current_files or list_excel_files(),
                }
            except Exception as e:
                raise HTTPException(status_code=400, detail=str(e))

    raise HTTPException(
        status_code=400,
        detail="No merge_columns node executed in workflow",
    )

@app.get("/preview")
def get_preview(filename: str, sheet: str):
    try:
        data = preview_sheet(filename, sheet)
        return data
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/columns")
def get_columns(sheet: str):
    try:
        cols = union_columns_for_sheet(sheet)
        return {"sheet": sheet, "columns": cols}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/merge")
def post_merge(req: MergeRequest):
    try:
        out_name = merge_columns(req.sheet_name, req.columns)
        return {"output_filename": out_name}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/download/{filename}")
def download_file(filename: str):
    path = os.path.join(OUTPUT_DIR, filename)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(
        path,
        media_type=(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ),
        filename=filename,
    )
