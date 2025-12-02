import os
from typing import List, Dict, Any

import pandas as pd

# Base directory = backend folder
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

# Ensure folders exist
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)
from typing import Optional

def list_excel_files() -> List[str]:
    """Return list of .xlsx files in DATA_DIR."""
    files = [
        f for f in os.listdir(DATA_DIR)
        if f.lower().endswith(".xlsx")
    ]
    return sorted(files)


def get_file_path(filename: str) -> str:
    return os.path.join(DATA_DIR, filename)


def list_sheets(filename: str) -> List[str]:
    path = get_file_path(filename)
    xls = pd.ExcelFile(path, engine="openpyxl")
    return list(xls.sheet_names)


def preview_sheet(filename: str, sheet_name: str, nrows: int = 50) -> Dict[str, Any]:
    """
    Return a small preview of a sheet:
    - columns: list of column names
    - rows: list of dicts {col: value}
    """
    path = get_file_path(filename)
    df = pd.read_excel(path, sheet_name=sheet_name, nrows=nrows, engine="openpyxl")
    df = df.fillna("")  # avoid NaN in JSON

    columns = [str(c) for c in df.columns]
    rows = df.to_dict(orient="records")

    return {"columns": columns, "rows": rows}


def union_columns_for_sheet(sheet_name: str) -> List[str]:
    """Across all files, collect all columns that appear in this sheet."""
    union_cols: List[str] = []

    for fname in list_excel_files():
        path = get_file_path(fname)
        try:
            xls = pd.ExcelFile(path, engine="openpyxl")
            if sheet_name not in xls.sheet_names:
                continue
            df_header = pd.read_excel(path, sheet_name=sheet_name, nrows=0, engine="openpyxl")
            cols = [str(c) for c in df_header.columns]
            for c in cols:
                if c not in union_cols:
                    union_cols.append(c)
        except Exception:
            # skip file on error
            continue

    return union_cols


# def merge_columns(sheet_name: str, columns: List[str]) -> str:
#     """
#     Merge given columns from all files that contain the sheet.
#     Returns output filename (inside OUTPUT_DIR).
#     """
#     all_dfs = []
#
#     for fname in list_excel_files():
#         path = get_file_path(fname)
#         try:
#             xls = pd.ExcelFile(path, engine="openpyxl")
#             if sheet_name not in xls.sheet_names:
#                 continue
#             df = pd.read_excel(path, sheet_name=sheet_name, engine="openpyxl")
#         except Exception:
#             continue
#
#         # Ensure all selected columns exist
#         for col in columns:
#             if col not in df.columns:
#                 df[col] = pd.NA
#
#         df_selected = df[columns].copy()
#         df_selected["SourceFile"] = fname
#         all_dfs.append(df_selected)
#
#     if not all_dfs:
#         raise ValueError(f"No data to merge for sheet '{sheet_name}'.")
#
#     merged_df = pd.concat(all_dfs, ignore_index=True)
#
#     safe_sheet = sheet_name.replace(" ", "_")
#     out_name = f"merged_{safe_sheet}.xlsx"
#     out_path = os.path.join(OUTPUT_DIR, out_name)
#     merged_df.to_excel(out_path, index=False)
#
#     return out_name
def merge_columns(sheet_name: str,
                  columns: List[str],
                  files: Optional[List[str]] = None) -> str:
    """
    Merge given columns from a specific sheet across a set of Excel files.

    If `files` is None, all Excel files in DATA_DIR are used.
    """
    all_dfs = []

    available_files = files if files is not None else list_excel_files()

    for fname in available_files:
        path = get_file_path(fname)
        try:
            xls = pd.ExcelFile(path, engine="openpyxl")
            if sheet_name not in xls.sheet_names:
                continue
            df = pd.read_excel(path, sheet_name=sheet_name, engine="openpyxl")
        except Exception:
            continue

        # Ensure all selected columns exist
        for col in columns:
            if col not in df.columns:
                df[col] = pd.NA

        df_selected = df[columns].copy()
        df_selected["SourceFile"] = fname
        all_dfs.append(df_selected)

    if not all_dfs:
        raise ValueError(
            f"No data to merge for sheet '{sheet_name}' "
            f"using files: {available_files}"
        )

    merged_df = pd.concat(all_dfs, ignore_index=True)

    safe_sheet = sheet_name.replace(" ", "_")
    out_name = f"merged_{safe_sheet}.xlsx"
    out_path = os.path.join(OUTPUT_DIR, out_name)
    merged_df.to_excel(out_path, index=False)

    return out_name