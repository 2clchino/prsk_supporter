from zoneinfo import ZoneInfo
from gspread.utils import rowcol_to_a1
import gspread_manager
from datetime import datetime, timedelta, time
import re

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
TIME_RE = re.compile(r"^\d{2}:\d{2}$")

def format_shift_table(
    spreadsheet_id: str,
    start: datetime,
    end: datetime,
    gap_cols: int = 4,
    sheet_title: str = "Shift",
) -> str:
    if start > end:
        raise ValueError("start must be <= end")
    if gap_cols < 0:
        raise ValueError("gap_cols must be >= 0")
    sh = gspread_manager.load_sheet(spreadsheet_id)
    start_h = start.replace(minute=0, second=0, microsecond=0)
    end_h   = end.replace(minute=0, second=0, microsecond=0)

    days = []
    d = start.date()
    end_d = end.date()
    while d <= end_d:
        days.append(d)
        d += timedelta(days=1)
    total_rows = 25
    total_cols = 1 + (len(days) - 1) * (1 + gap_cols) + gap_cols if days else 1 + gap_cols
    ws = gspread_manager.create_sheet(sh, sheet_title, total_rows, total_cols)
    table = [[""] * total_cols for _ in range(total_rows)]
    
    for i, day in enumerate(days):
        base_col = 1 + i * (1 + gap_cols) - 1  # 0-based index
        day_start_hour = start_h.hour if day == start_h.date() else 0
        day_end_hour   = end_h.hour   if day == end_h.date()   else 23
        table[0][base_col] = day.strftime("%Y-%m-%d")
        for h in range(24):
            table[h+1][base_col] = f"{h:02d}:00" if day_start_hour <= h <= day_end_hour else ""

        for j in range(gap_cols):
            col = base_col + 1 + j
            table[0][col] = "アンコ" if j == gap_cols - 1 else f"支援者{j+1}"

    cell_range = f"{rowcol_to_a1(1,1)}:{rowcol_to_a1(total_rows, total_cols)}"
    sh.values_update(
        f"{ws.title}!{cell_range}",
        params={"valueInputOption": "USER_ENTERED"},
        body={"values": table},
    )

    try:
        ws.freeze(rows=1)
    except Exception:
        pass

    return ws.title

def find_date_columns(header_row):
    return [c for c, v in enumerate(header_row) if DATE_RE.match(v.strip())]

def parse_date(date_str, tz):
    try:
        y, m, d = map(int, date_str.split("-"))
        return datetime(y, m, d, tzinfo=tz).date()
    except Exception:
        return None
    
def collect_candidates(data, date_cols, tz, max_shifters_per_block):
    candidates = []
    ncols = len(data[0])
    for bi, date_col in enumerate(date_cols):
        date_str = data[0][date_col].strip()
        base_date = parse_date(date_str, tz)
        if not base_date:
            continue

        next_date_col = date_cols[bi + 1] if bi + 1 < len(date_cols) else ncols
        shift_start = date_col + 1
        shift_end_exclusive = min(date_col + 1 + max_shifters_per_block, next_date_col)

        for r in range(1, min(len(data), 25)):
            tstr = data[r][date_col].strip()
            if not TIME_RE.match(tstr):
                continue
            hh, mm = map(int, tstr.split(":"))
            dt = datetime.combine(base_date, time(hh, mm, tzinfo=tz))

            shifters = [data[r][c].strip() for c in range(shift_start, shift_end_exclusive) if data[r][c].strip()]
            diff = abs((dt - datetime.now(tz)).total_seconds())
            candidates.append((diff, dt, shifters, bi, r, date_col))
    return candidates

def choose_best_candidate(candidates, tz):
    if not candidates:
        raise ValueError("no valid time rows found")

    now = datetime.now(tz)
    past_candidates = [c for c in candidates if c[1] <= now]
    if not past_candidates:
        raise ValueError("no past time rows found")
    past_candidates.sort(key=lambda x: (abs((now - x[1]).total_seconds()), x[1]))
    _, best_dt, best_shifters, bi, r, date_col = past_candidates[0]

    return {
        "datetime": best_dt,
        "shifters": best_shifters,
    }
    
def extract_nearest_shift(
    spreadsheet_id: str,
    sheet_title: str = "Shift",
    tz_str: str = "Asia/Tokyo",
    max_shifters_per_block: int = 4,
):
    tz = ZoneInfo(tz_str)
    data = gspread_manager.load_table(spreadsheet_id, sheet_title)
    if not data:
        raise ValueError("sheet is empty")

    date_cols = find_date_columns(data[0])
    if not date_cols:
        raise ValueError("no date headers found")

    candidates = collect_candidates(data, date_cols, tz, max_shifters_per_block)
    return choose_best_candidate(candidates, tz)