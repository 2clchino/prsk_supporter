from zoneinfo import ZoneInfo
from gspread.utils import rowcol_to_a1
import gspread_manager
from datetime import datetime, timedelta
import re
from typing import List, Dict, Any, Union, Optional, Tuple

def format_pt_table(spreadsheet_id: str,
                    start: datetime,
                    end: datetime,
                    trackings: List[int],
                    sheet_title: str = "PtLogs") -> None:
    if start > end:
        raise ValueError("start must be <= end")
    sh = gspread_manager.load_sheet(spreadsheet_id)
    times = []
    t = start
    while t <= end:
        times.append(t)
        t += timedelta(hours=1)

    n_rows = 1 + len(times)
    n_cols = 2 + max(len(trackings), 0)
    ws = gspread_manager.create_sheet(sh, sheet_title, n_rows, max(n_cols, 2))

    header = ["日付", "時間"] + [str(x) for x in trackings]
    ws.update(values=[header], range_name=f"A1:{_col_letter(len(header))}1")

    zero_row = [0] * len(header)
    ws.update(values=[zero_row], range_name=f"A2:{_col_letter(len(header))}2")

    day_hour_rows = []
    prev_date = None
    for dt in times:
        d = dt.date()
        day_cell = f"{dt.month}/{dt.day}" if d != prev_date else ""
        hour_cell = dt.strftime("%H:%M")
        day_hour_rows.append([day_cell, hour_cell])
        prev_date = d
    ws.update(values=day_hour_rows, range_name=f"A2:B{1+len(times)}")

def _col_letter(n: int) -> str:
    if n < 1:
        return "A"
    result = []
    while n > 0:
        n, r = divmod(n - 1, 26)
        result.append(chr(ord('A') + r))
    return ''.join(reversed(result))

import math
def write_values(spreadsheet_id: str,
                 iso_timestamp: str,
                 values_by_header: Dict[Union[int, str], Any],
                 tz_name: str = "Asia/Tokyo",
                 sheet_title: str = "PtLogs") -> None:
    ts = iso_timestamp.strip()
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    dt_utc = datetime.fromisoformat(ts)
    dt_local = dt_utc.astimezone(ZoneInfo(tz_name))
    target_day_str = f"{dt_local.month}/{dt_local.day}"
    tgt_seconds = dt_local.hour * 3600 + dt_local.minute * 60 + dt_local.second

    sh = gspread_manager.load_sheet(spreadsheet_id)
    ws = sh.worksheet(sheet_title)

    header = ws.row_values(1)
    header_map = {h: idx + 1 for idx, h in enumerate(header) if h}

    col_a = ws.col_values(1)
    col_b = ws.col_values(2)
    n_rows = max(len(col_a), len(col_b))
    data_start_row = 2

    def parse_day_cell(s: str) -> Optional[Tuple[int, int]]:
        if not s:
            return None
        m = re.fullmatch(r"(\d{1,2})/(\d{1,2})", s.strip())
        if not m:
            return None
        return int(m.group(1)), int(m.group(2))

    best_row = None
    best_diff = math.inf
    best_minutes = None
    current_month_day: Optional[Tuple[int, int]] = None
    for r in range(data_start_row, n_rows + 1):
        day_cell = col_a[r-1] if r-1 < len(col_a) else ""
        time_cell = col_b[r-1] if r-1 < len(col_b) else ""
        md = parse_day_cell(day_cell)
        if md is not None:
            current_month_day = md
        if current_month_day is None:
            continue

        cur_day_str = f"{current_month_day[0]}/{current_month_day[1]}"
        if cur_day_str != target_day_str:
            continue

        if not time_cell or ":" not in time_cell:
            continue
        try:
            hh, mm = map(int, time_cell.strip().split(":", 1))
        except Exception:
            continue

        cur_seconds = hh * 3600 + mm * 60
        diff = abs(cur_seconds - tgt_seconds)

        cur_minutes = hh * 60 + mm
        if diff < best_diff:
            best_diff = diff
            best_row = r
            best_minutes = cur_minutes
        elif diff == best_diff and best_row is not None and best_minutes is not None:
            if cur_minutes < best_minutes:
                best_row = r
                best_minutes = cur_minutes

    if best_row is None:
        raise ValueError(f"対象日 {target_day_str} の行が見つかりませんでした。")

    data_requests = []
    for k, v in values_by_header.items():
        key_str = str(k)
        col = header_map.get(key_str)
        if col is None:
            continue
        a1 = f"{_col_letter(col)}{best_row}"
        data_requests.append({"range": a1, "values": [[v]]})

    if not data_requests:
        return
    ws.batch_update(data_requests)