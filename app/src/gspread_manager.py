import gspread
from google.oauth2.service_account import Credentials
from typing import Any, Dict, List
from dotenv import load_dotenv
import re
import json
import os

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
SERVICE_ACCOUNT_KEY = "./keys/rock-perception-419201-eb5dbe72985b.json"
load_dotenv(override=False)

def load_sheet(spreadsheet_id: str):
    service_account_key = os.environ["SERVICE_ACCOUNT_KEY"]
    creds = Credentials.from_service_account_file(service_account_key, scopes=SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(spreadsheet_id)
    return sh

def load_table(spreadsheet_id, 
               sheet_title = "Shift"):
    sh = load_sheet(spreadsheet_id)
    ws = sh.worksheet(sheet_title)
    data = ws.get_all_values()
    return normalize_table(data)

def normalize_table(data):
    if not data:
        return []
    ncols = max(len(r) for r in data)
    for r in data:
        r += [""] * (ncols - len(r))
    return data

def create_sheet(sh, sheet_title, total_rows, total_cols, *, resize_if_smaller=False):
    try:
        ws = sh.worksheet(sheet_title)
        if resize_if_smaller and (ws.row_count < total_rows or ws.col_count < total_cols):
            ws.resize(
                rows=max(ws.row_count, int(total_rows)),
                cols=max(ws.col_count, int(total_cols)),
            )
        return ws
    except gspread.exceptions.WorksheetNotFound:
        pass
    try:
        ws = sh.add_worksheet(title=sheet_title, rows=total_rows, cols=total_cols)
        return ws
    except gspread.exceptions.APIError as e:
        try:
            ws = sh.worksheet(sheet_title)
            if resize_if_smaller and (ws.row_count < total_rows or ws.col_count < total_cols):
                ws.resize(
                    rows=max(ws.row_count, int(total_rows)),
                    cols=max(ws.col_count, int(total_cols)),
                )
            return ws
        except gspread.exceptions.WorksheetNotFound:
            raise ValueError(
                f"シート '{sheet_title}' の作成に失敗しました。"
                "SpreadSheet への権限が足りない可能性があります。"
            ) from e

_ISO8601_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d{1,6})?)?(?:Z|[+\-]\d{2}:\d{2})?)?$"
)

def _coerce_scalar(s: str) -> Any:
    s = s.strip()
    if s == "":
        return ""

    if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
        try:
            return json.loads(s)
        except Exception:
            pass

    low = s.lower()
    if low in ("true", "false"):
        return low == "true"
    if low in ("null", "none"):
        return None

    try:
        if re.fullmatch(r"[+\-]?\d+", s):
            return int(s)
    except Exception:
        pass

    try:
        if re.fullmatch(r"[+\-]?(?:\d+\.\d+|\d+\.|\.\d+|\d+)(?:[eE][+\-]?\d+)?", s):
            return float(s)
    except Exception:
        pass

    if _ISO8601_RE.match(s):
        return s

    return s

def _coerce_values(cells: List[str]) -> Any:
    vals = [c.strip() for c in cells if isinstance(c, str) and c.strip() != ""]
    if not vals:
        return None
    if len(vals) == 1:
        v = vals[0]
        if not ((v.startswith("{") and v.endswith("}")) or (v.startswith("[") and v.endswith("]"))) and ("," in v):
            splitted = [p.strip() for p in v.split(",") if p.strip() != ""]
            return [_coerce_scalar(p) for p in splitted] if len(splitted) > 1 else _coerce_scalar(v)
        return _coerce_scalar(v)
    return [_coerce_scalar(v) for v in vals]

def read_config_values(spreadsheet_id: str, sheet_name: str = "Config") -> Dict[str, Any]:
    ws = load_sheet(spreadsheet_id).worksheet(sheet_name)
    rows = ws.get_all_values()
    config: Dict[str, Any] = {}
    for row in rows:
        if not row:
            continue
        key = (row[0] or "").strip() if len(row) >= 1 else ""
        if not key:
            continue
        values = row[1:] if len(row) > 1 else []
        config[key] = _coerce_values(values)
    return config