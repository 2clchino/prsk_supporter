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
    runners: list = None,
) -> str:
    if start > end:
        raise ValueError("start must be <= end")
    if gap_cols < 0:
        raise ValueError("gap_cols must be >= 0")
    runners = [str(r).strip() for r in (runners or []) if str(r).strip()]
    # 支援者列(アンコ除く)は "gap_cols - ランナー数" 分だけ確保する（例: gap_cols=4, ランナー2人 → 支援者2列+アンコ1列）
    support_cols = max(0, gap_cols - len(runners))
    sh = gspread_manager.load_sheet(spreadsheet_id)
    start_h = start.replace(minute=0, second=0, microsecond=0)
    end_h   = end.replace(minute=0, second=0, microsecond=0)

    days = []
    d = start.date()
    end_d = end.date()
    while d <= end_d:
        days.append(d)
        d += timedelta(days=1)

    # 1ブロック = 日付 + ランナー列 + 支援者列 + アンコ列
    block_width = 1 + len(runners) + support_cols + 1
    expected_dates = {day.strftime("%Y-%m-%d") for day in days}
    total_rows = 25
    total_cols = len(days) * block_width if days else block_width

    existing = next((ws for ws in sh.worksheets() if ws.title == sheet_title), None)
    old_values = None
    if existing is not None:
        header = existing.row_values(1)
        existing_dates = {v.strip() for v in header if DATE_RE.match(v.strip())}
        header_set = {h.strip() for h in header}
        has_all_runner_cols = all(r in header_set for r in runners)
        # 列数(レイアウト)まで一致していることを確認する。ランナー名が存在するだけでは
        # 「支援者列数がランナー数に応じて正しく詰まっているか」「順序が最新か」は判定できないため。
        same_layout = (
            existing_dates == expected_dates
            and has_all_runner_cols
            and existing.col_count == total_cols
        )
        if same_layout:
            return sheet_title
        old_values = existing.get_all_values()  # 列構成変更で再作成する前に既存のシフト記入内容を退避
        sh.del_worksheet(existing)

    ws = gspread_manager.create_sheet(sh, sheet_title, total_rows, total_cols)
    table = [[""] * total_cols for _ in range(total_rows)]

    runner_start = 1
    support_start = runner_start + len(runners)
    ancho_col_offset = support_start + support_cols

    for i, day in enumerate(days):
        base_col = i * block_width
        day_start_hour = start_h.hour if day == start_h.date() else 0
        day_end_hour   = end_h.hour   if day == end_h.date()   else 23
        table[0][base_col] = day.strftime("%Y-%m-%d")
        for h in range(24):
            in_range = day_start_hour <= h <= day_end_hour
            table[h+1][base_col] = f"{h:02d}:00" if in_range else ""
            for ri, runner in enumerate(runners):
                table[h+1][base_col + runner_start + ri] = runner if in_range else ""

        for ri, runner in enumerate(runners):
            table[0][base_col + runner_start + ri] = runner
        for j in range(support_cols):
            table[0][base_col + support_start + j] = f"支援者{j+1}"
        table[0][base_col + ancho_col_offset] = "アンコ"

    if old_values:
        _migrate_shift_table_data(table, old_values, days, block_width)

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

def _migrate_shift_table_data(table, old_values, days, block_width) -> None:
    """列構成が変わってシートを再作成した際、(日付, 時刻)と列見出しが一致する既存の
    シフト記入内容(支援者割当・Auto指定など)を新シートへ引き継ぐ。"""
    if not old_values:
        return
    old_header = old_values[0] if old_values else []
    old_date_cols = find_date_columns(old_header)

    old_lookup: dict = {}
    for oi, odate_col in enumerate(old_date_cols):
        date_str = old_header[odate_col].strip()
        next_col = old_date_cols[oi + 1] if oi + 1 < len(old_date_cols) else len(old_header)
        for r in range(1, len(old_values)):
            row = old_values[r]
            hour_label = row[odate_col].strip() if odate_col < len(row) else ""
            if not hour_label:
                continue
            cell_map = {}
            for c in range(odate_col + 1, next_col):
                if c < len(old_header) and c < len(row):
                    h = old_header[c].strip()
                    if h:
                        cell_map[h] = row[c]
            old_lookup[(date_str, hour_label)] = cell_map

    for i, day in enumerate(days):
        base_col = i * block_width
        date_str = table[0][base_col]
        for r in range(1, len(table)):
            hour_label = table[r][base_col]
            if not hour_label:
                continue
            cell_map = old_lookup.get((date_str, hour_label))
            if not cell_map:
                continue
            for c in range(base_col + 1, base_col + block_width):
                header = table[0][c]
                old_val = cell_map.get(header)
                if old_val and old_val.strip():
                    table[r][c] = old_val

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

def choose_row_and_next(
    candidates,
    tz,
    data,
    date_cols,
    max_shifters_per_block,
):
    if not candidates:
        raise ValueError("no valid time rows found")

    now = datetime.now(tz)
    past_candidates = [c for c in candidates if c[1] <= now]
    if not past_candidates:
        raise ValueError("no past time rows found")

    past_candidates.sort(key=lambda x: (abs((now - x[1]).total_seconds()), x[1]))
    _, _, _, bi, r, date_col = past_candidates[0]

    return _build_two_rows(data, date_cols, bi, r, date_col, tz, max_shifters_per_block)


def _build_two_rows(data, date_cols, bi, start_r, date_col, tz, max_shifters_per_block):
    out = []
    ncols = len(data[0])
    base_date = parse_date(data[0][date_col].strip(), tz)
    if not base_date:
        raise ValueError("invalid base date in header")

    next_date_col = date_cols[bi + 1] if bi + 1 < len(date_cols) else ncols
    shift_start = date_col + 1
    shift_end_exclusive = min(date_col + 1 + max_shifters_per_block, next_date_col)

    for r in (start_r, start_r + 1):
        if not (1 <= r < len(data)):
            continue
        tstr = data[r][date_col].strip()
        if not TIME_RE.match(tstr):
            continue
        hh, mm = map(int, tstr.split(":"))
        dt = datetime.combine(base_date, time(hh, mm, tzinfo=tz))
        shifters = [
            data[r][c].strip()
            for c in range(shift_start, shift_end_exclusive)
            if data[r][c].strip()
        ]
        out.append({"datetime": dt, "shifters": shifters})

    if not out:
        raise ValueError("no valid time rows found at target rows")
    return out
    
def extract_nearest_shift(
    spreadsheet_id: str,
    max_shifters_per_block: int = 4,
    sheet_title: str = "Shift",
    tz_str: str = "Asia/Tokyo",
):
    tz = ZoneInfo(tz_str)
    data = gspread_manager.load_table(spreadsheet_id, sheet_title)
    if not data:
        raise ValueError("sheet is empty")

    date_cols = find_date_columns(data[0])
    if not date_cols:
        raise ValueError("no date headers found")

    candidates = collect_candidates(data, date_cols, tz, max_shifters_per_block)
    return choose_row_and_next(
        candidates, tz,
        data=data,
        date_cols=date_cols,
        max_shifters_per_block=max_shifters_per_block,
    )

def is_auto_period(
    spreadsheet_id: str,
    dt: datetime,
    sheet_title: str = "Shift",
    runner_name: str = None,
) -> bool:
    """runner_name を指定すると、その Runner 専用列が "Auto" になっているかどうかで判定する
    (プレイヤー別のAuto判定)。省略時は従来通り、いずれかの列が "Auto" なら True を返す。"""
    tz = dt.tzinfo or ZoneInfo("Asia/Tokyo")
    try:
        data = gspread_manager.load_table(spreadsheet_id, sheet_title)
    except Exception:
        return False
    if not data or len(data) < 2:
        return False

    header_row = data[0]
    date_cols = find_date_columns(header_row)
    target_date = dt.date()

    for i, date_col in enumerate(date_cols):
        col_date = parse_date(header_row[date_col].strip(), tz)
        if col_date != target_date:
            continue
        next_date_col = date_cols[i + 1] if i + 1 < len(date_cols) else len(header_row)
        for r in range(1, len(data)):
            tstr = data[r][date_col].strip() if date_col < len(data[r]) else ""
            if TIME_RE.match(tstr):
                row_hour = int(tstr.split(":")[0])
            elif r <= 24:
                row_hour = r - 1
            else:
                continue
            if row_hour != dt.hour:
                continue
            for c in range(date_col + 1, next_date_col):
                if c >= len(data[r]):
                    continue
                if runner_name is not None:
                    header_text = header_row[c].strip() if c < len(header_row) else ""
                    if header_text != runner_name:
                        continue
                if data[r][c].strip().lower() == "auto":
                    return True
    return False


def get_auto_hours(
    spreadsheet_id: str,
    date,
    sheet_title: str = "Shift",
) -> list[int]:
    """Return sorted list of hours (0-23) that have 'Auto' in any shifter column for the given date."""
    tz = ZoneInfo("Asia/Tokyo")
    try:
        data = gspread_manager.load_table(spreadsheet_id, sheet_title)
    except Exception:
        return []
    if not data or len(data) < 2:
        return []

    date_cols = find_date_columns(data[0])
    hours = []
    for i, date_col in enumerate(date_cols):
        col_date = parse_date(data[0][date_col].strip(), tz)
        if col_date != date:
            continue
        next_date_col = date_cols[i + 1] if i + 1 < len(date_cols) else len(data[0])
        for r in range(1, len(data)):
            tstr = data[r][date_col].strip() if date_col < len(data[r]) else ""
            if TIME_RE.match(tstr):
                h = int(tstr.split(":")[0])
            elif r <= 24:
                h = r - 1  # row 1 = 00:00, row 2 = 01:00, ...
            else:
                continue
            for c in range(date_col + 1, next_date_col):
                if c < len(data[r]) and data[r][c].strip().lower() == "auto":
                    hours.append(h)
                    break
    return sorted(hours)


def count_runners(value):
    if isinstance(value, list):
        return len(value)
    elif isinstance(value, str):
        return 1
    else:
        return 0