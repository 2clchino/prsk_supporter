from zoneinfo import ZoneInfo
from gspread.utils import rowcol_to_a1
import gspread_manager
from datetime import datetime, timedelta
import re
from typing import List, Dict, Any, Union, Optional, Tuple

def _find_tracking_refs(sh, trackings: list, config_sheet: str = "Config") -> list:
    """Config シートの Trackings 行から =Config!XX セル参照式を生成する。
    各トラッキング値が別セルに入っている場合のみ参照式を返す。それ以外は空リスト。"""
    try:
        cws = sh.worksheet(config_sheet)
        rows = cws.get_all_values()
        for row_idx, row in enumerate(rows):
            if not row or row[0].strip() != "Trackings":
                continue
            # 値が入っているセルを左から順に収集
            refs = []
            for col_idx in range(1, len(row)):
                if row[col_idx].strip():
                    refs.append(f"={config_sheet}!{_col_letter(col_idx + 1)}{row_idx + 1}")
                else:
                    break
            # セル数がトラッキング数と一致する場合のみ採用（カンマ区切り1セルの場合は不一致になる）
            if len(refs) == len(trackings):
                return refs
    except Exception:
        pass
    return []

def refle_value_header(tracking) -> str:
    return f"{tracking}_リフレ値"

def _is_player_tracking(t) -> bool:
    """main.py の _is_player と同じ判定: 文字列(ユーザー名/TwitterID)または15桁以上の数値IDのみ対象。
    1,2,3... のような順位トラッキングにはリフレ値列を作らない。"""
    if isinstance(t, str):
        return True
    return isinstance(t, int) and len(str(abs(t))) >= 15

def format_pt_table(spreadsheet_id: str,
                    start: datetime,
                    end: datetime,
                    trackings: List[int],
                    interval_minutes: int = 60,
                    sheet_title: str = "PtLogs") -> None:
    if start > end:
        raise ValueError("start must be <= end")
    sh = gspread_manager.load_sheet(spreadsheet_id)
    times = []
    t = start
    while t <= end:
        times.append(t)
        t += timedelta(minutes=interval_minutes)

    n_rows = 1 + len(times)
    refle_headers = [refle_value_header(x) for x in trackings if _is_player_tracking(x)]
    n_cols = 2 + max(len(trackings), 0) + len(refle_headers)

    existing = next((ws for ws in sh.worksheets() if ws.title == sheet_title), None)
    old_values = None
    if existing is not None:
        if existing.row_count == n_rows and existing.col_count >= n_cols:
            return
        old_values = existing.get_all_values()  # 列構成変更で再作成する前に既存ログを退避
        sh.del_worksheet(existing)

    ws = gspread_manager.create_sheet(sh, sheet_title, n_rows, max(n_cols, 2))

    tracking_refs = _find_tracking_refs(sh, trackings)
    tracking_headers = tracking_refs if tracking_refs else [str(x) for x in trackings]
    header = ["日付", "時間"] + tracking_headers + refle_headers
    ws.update(values=[header], range_name=f"A1:{_col_letter(len(header))}1",
              value_input_option="USER_ENTERED")
    # tracking_headers がセル参照式の場合、列照合には評価後の表示値を使う
    new_header_display = ws.row_values(1)
    new_header_display += [""] * (len(header) - len(new_header_display))

    zero_row = [0] * len(header)
    ws.update(values=[zero_row], range_name=f"A2:{_col_letter(len(header))}2")

    day_hour_rows = []
    prev_date = None
    for dt in times:
        d = dt.date()
        day_cell = f"{dt.month}/{dt.day}" if d != prev_date else ""
        time_cell = dt.strftime("%H:%M")
        day_hour_rows.append([day_cell, time_cell])
        prev_date = d

    ws.update(values=day_hour_rows, range_name=f"A2:B{1 + len(times)}")

    if old_values:
        _migrate_pt_table_data(ws, new_header_display, day_hour_rows, old_values)

def _forward_fill_daytime(day_time_pairs) -> list:
    """日付列は日付が変わった行にしか入っていないため、直近の日付で埋めて (日付, 時間) の組にする。"""
    result = []
    last_day = None
    for day, tm in day_time_pairs:
        if day:
            last_day = day
        result.append((last_day, tm))
    return result

def _migrate_pt_table_data(ws, new_header_display: list, new_day_hour_rows: list, old_values: list) -> None:
    """列構成が変わってシートを再作成した際、見出しと(日付,時間)が一致する既存データを新シートへ引き継ぐ。"""
    if not old_values:
        return
    old_header = old_values[0] if old_values else []
    old_rows = old_values[1:]

    old_daytime = _forward_fill_daytime(
        (r[0] if len(r) > 0 else "", r[1] if len(r) > 1 else "") for r in old_rows
    )
    old_row_by_daytime = {dt: r for dt, r in zip(old_daytime, old_rows) if dt[0] and dt[1]}

    new_daytime = _forward_fill_daytime(new_day_hour_rows)

    old_col_index = {h: i for i, h in enumerate(old_header) if h}
    shared_cols = [h for h in new_header_display[2:] if h in old_col_index]
    if not shared_cols:
        return

    header_to_col = {h: i + 1 for i, h in enumerate(new_header_display)}

    for h in shared_cols:
        oi = old_col_index[h]
        col_values = []
        for dt_key in new_daytime:
            old_row = old_row_by_daytime.get(dt_key)
            col_values.append([old_row[oi] if old_row is not None and oi < len(old_row) else ""])
        if not any(v[0] for v in col_values):
            continue  # コピーする値が無ければ書き込み自体をスキップ
        col_letter = _col_letter(header_to_col[h])
        ws.update(values=col_values, range_name=f"{col_letter}2:{col_letter}{1 + len(col_values)}")

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
                 sheet_title: str = "PtLogs",
                 overwrite: bool = False) -> None:
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
    ranges = []
    idx_to_key: dict[int, str] = {}
    for k in values_by_header.keys():
        col = header_map.get(str(k))
        if col:
            a1 = f"{_col_letter(col)}{best_row}"
            ranges.append(a1)
            idx_to_key[len(ranges) - 1] = str(k)

    if not ranges:
        return

    current_vals = ws.batch_get(ranges)
    for i, a1 in enumerate(ranges):
        cur = current_vals[i][0] if (i < len(current_vals) and current_vals[i]) else ""
        if overwrite or not str(cur).strip():
            key_str = idx_to_key[i]
            data_requests.append({"range": a1, "values": [[values_by_header[key_str if key_str in values_by_header else int(key_str)]]]} )

    if not data_requests:
        return  # all columns already written for this row
    ws.batch_update(data_requests)

def get_cumulative_value(spreadsheet_id: str,
                         header_name: str,
                         sheet_title: str = "PtLogs") -> float:
    """指定した列の数値をすべて合算して返す（累積リフレ値の算出用）。"""
    sh = gspread_manager.load_sheet(spreadsheet_id)
    ws = sh.worksheet(sheet_title)
    header = ws.row_values(1)
    if header_name not in header:
        return 0.0
    col = header.index(header_name) + 1
    total = 0.0
    for v in ws.col_values(col)[1:]:
        try:
            total += float(v)
        except (TypeError, ValueError):
            continue
    return total