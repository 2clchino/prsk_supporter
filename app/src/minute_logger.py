# minute_logger.py
"""jp.seka.ing から上位ランカーの1分毎のポイント推移(Download as CSV と同じ元データ)を取得し、
PtLogs1min シートに timestamp | 1 | 2 | 3 | 4 | 5 の形式で記録する。

各列は「取得時点で N 位のユーザー」のイベント開始からの全履歴で、毎回シート全体を上書きする。
最終的に(イベント終了時点で)上位のユーザーそれぞれのポイント推移を追えるようにするため。
/minutelog_targets でユーザー名を設定した場合は、順位の代わりにそのユーザーたちを
ランキング100位以内から名前で検索して列にする。
Google Sheets API の呼び出し回数制限を避けるため、取得・書き込みは5分に1回とする。
"""
from __future__ import annotations
import asyncio
import bisect
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import discord
import gspread

import gspread_manager
import sekai_api
import storage
from timeutils import now_jst, ensure_aware_jst

SHEET_TITLE = "PtLogs1min"
RANKS = [1, 2, 3, 4, 5]
INTERVAL_MINUTES = 5
# seka.ing 側の更新を待つため、5分の区切りから少し遅らせて取得する
OFFSET_SECONDS = 30
_TS_FORMAT = "%Y-%m-%d %H:%M"

# 複数のスプレッドシートを同時に運用していても、スクレイピングは1回で済ませる
_fetch_lock = asyncio.Lock()
_fetch_cache: Optional[tuple[float, set, dict]] = None  # (取得時刻, 要求したユーザー名, 取得結果)
_FETCH_CACHE_TTL = 120

# 指定ユーザーが一時的に100位圏外になったり取得に失敗した場合、シートの既存データを消さないよう直近の推移を使う
_last_timelines_by_name: Dict[str, Dict[datetime, int]] = {}

_ws_cache: Dict[str, gspread.Worksheet] = {}


def is_enabled(cfg: dict) -> bool:
    val = cfg.get("MinuteLog", True)
    if isinstance(val, str):
        return val.strip().lower() not in ("false", "0", "off", "no", "")
    return bool(val) if val is not None else True


def cached_leaderboard() -> List[dict]:
    """直近に取得したランキング(100位まで)。"""
    return _fetch_cache[2]["leaderboard"] if _fetch_cache else []


async def fetch(names: List[str] = (), *, force: bool = False) -> dict:
    global _fetch_cache
    wanted = set(names)
    async with _fetch_lock:
        if (not force and _fetch_cache and time.monotonic() - _fetch_cache[0] < _FETCH_CACHE_TTL
                and wanted <= _fetch_cache[1]):
            return _fetch_cache[2]
        data = await asyncio.to_thread(sekai_api.fetch_minute_points, RANKS, sorted(wanted))
        if data["leaderboard"]:
            _fetch_cache = (time.monotonic(), wanted, data)
        return data


def _get_worksheet(spreadsheet_id: str) -> gspread.Worksheet:
    ws = _ws_cache.get(spreadsheet_id)
    if ws is not None:
        return ws
    sh = gspread_manager.load_sheet(spreadsheet_id)
    try:
        ws = sh.worksheet(SHEET_TITLE)
    except gspread.exceptions.WorksheetNotFound:
        ws = gspread_manager.create_sheet(sh, SHEET_TITLE, 1, 1 + len(RANKS))
        ws.freeze(rows=1)
    _ws_cache[spreadsheet_id] = ws
    return ws


def _value_at(series: Dict[datetime, int], keys: list, ts: datetime) -> Optional[int]:
    """ts 時点のポイントを返す。データ点の秒ずれ等で抜けた分は直前の値で補い、
    記録範囲外(記録開始前・最終データより後)は None を返す。"""
    if not keys or ts < keys[0] or ts > keys[-1]:
        return None
    return series[keys[bisect.bisect_right(keys, ts) - 1]]


def build_rows(columns: List[tuple], start: datetime, end: datetime) -> list:
    """見出し行 + イベント開始から最新までの1分毎の行を作る。
    各列は (見出し, 推移, 終端の判定に使うか) の組。"""
    keys = [sorted(series) for _, series, _ in columns]
    # 各ユーザーの推移データは取得時刻が少しずつずれるため、今回取得できた全員分が揃っている時刻までにする。
    # 圏外で更新が止まっている列は終端の判定に使わない
    last = min(min(k[-1] for k, (_, _, live) in zip(keys, columns) if live), end)
    first = max(min(k[0] for k in keys if k), start)

    rows = [["timestamp"] + [label for label, _, _ in columns]]
    ts = first
    while ts <= last:
        values = [_value_at(series, k, ts) for (_, series, _), k in zip(columns, keys)]
        rows.append([ts.strftime(_TS_FORMAT)] + ["" if v is None else v for v in values])
        ts += timedelta(minutes=1)
    return rows


def write_rows(spreadsheet_id: str, rows: list) -> None:
    """シート全体を rows で上書きする(update 1回)。前回より行や列が減った場合のみ余りを消す。"""
    ws = _get_worksheet(spreadsheet_id)
    nrows, ncols = len(rows), len(rows[0])
    if ws.row_count < nrows or ws.col_count < ncols:
        ws.resize(rows=max(ws.row_count, nrows), cols=max(ws.col_count, ncols))
    last_col = gspread_manager._col_letter(ncols)
    ws.update(values=rows, range_name=f"A1:{last_col}{nrows}", value_input_option="USER_ENTERED")
    stale = []
    if ws.row_count > nrows:
        stale.append(f"A{nrows + 1}:{gspread_manager._col_letter(ws.col_count)}{ws.row_count}")
    if ws.col_count > ncols:
        stale.append(f"{gspread_manager._col_letter(ncols + 1)}1:{gspread_manager._col_letter(ws.col_count)}{nrows}")
    if stale:
        ws.batch_clear(stale)


def _rank_columns(data: dict) -> tuple[list, list]:
    """1〜5位の列と、取得できなかった順位を返す。"""
    timelines = data["timelines"]
    top = {e["rank"]: e for e in data["leaderboard"] if e["rank"] in RANKS}
    missing = [r for r in RANKS if r not in top or top[r]["hash"] not in timelines]
    columns = [] if missing else [
        (f"{r} ({top[r]['name']})", timelines[top[r]["hash"]], True) for r in RANKS
    ]
    return columns, missing


def _name_columns(data: dict, names: List[str]) -> tuple[list, list]:
    """指定ユーザー名の列と、今回取得できなかったユーザー名を返す。"""
    timelines = data["timelines"]
    columns, missing = [], []
    for name in names:
        entry = next((e for e in data["leaderboard"] if e["name"] == name), None)
        series = timelines.get(entry["hash"]) if entry else None
        if series:
            _last_timelines_by_name[name] = series
            columns.append((f"{name} ({entry['rank']}位)", series, True))
        else:
            missing.append(name)
            columns.append((f"{name} (圏外)", _last_timelines_by_name.get(name, {}), False))
    return columns, missing


async def run_once(guild_id: int, spreadsheet_id: str, cfg: dict, *, force_fetch: bool = False) -> str:
    start = ensure_aware_jst(cfg["EventStart"])
    end = ensure_aware_jst(cfg["EventEnd"])
    names = storage.get_minute_log_players(guild_id, spreadsheet_id)
    data = await fetch(names, force=force_fetch)

    if names:
        columns, missing = _name_columns(data, names)
        if missing:
            print(f"[MinuteLog] guild={guild_id}: 100位以内に見つからない/取得失敗: {missing}")
        if not any(live for _, _, live in columns):
            return f"指定ユーザーが誰も100位以内に見つからないため書き込みを見送ります（{', '.join(missing)}）"
    else:
        columns, missing = _rank_columns(data)
        if missing:
            # 次回も全履歴で上書きするので、欠けがある回は書き込まずに見送る
            msg = f"rank {missing} の取得に失敗したため書き込みを見送ります"
            print(f"[MinuteLog] guild={guild_id}: {msg}")
            return msg

    rows = build_rows(columns, start, end)
    t = time.monotonic()
    try:
        await asyncio.to_thread(write_rows, spreadsheet_id, rows)
    except gspread.exceptions.APIError:
        # シートが手動で削除された等でキャッシュが古くなった可能性があるため、取り直して1回だけ再試行
        _ws_cache.pop(spreadsheet_id, None)
        await asyncio.to_thread(write_rows, spreadsheet_id, rows)
    msg = f"{len(rows) - 1} 行上書き ({time.monotonic() - t:.1f}s)"
    if names and missing:
        msg += f" / 100位以内に見つからないユーザー: {', '.join(missing)}"
    print(f"[MinuteLog] guild={guild_id} ss={spreadsheet_id}: {msg}")
    return msg


def _next_tick(now: datetime) -> datetime:
    base = now.replace(second=0, microsecond=0)
    base -= timedelta(minutes=base.minute % INTERVAL_MINUTES)
    tick = base + timedelta(seconds=OFFSET_SECONDS)
    while tick <= now:
        tick += timedelta(minutes=INTERVAL_MINUTES)
    return tick


async def run_loop(bot: discord.Client, guild_id: int, spreadsheet_id: str, cfg: dict) -> None:
    """EventScheduler の補助ループとして、イベント期間中5分毎に run_once を実行する。"""
    if not is_enabled(cfg):
        return
    start = ensure_aware_jst(cfg["EventStart"])
    end = ensure_aware_jst(cfg["EventEnd"])
    while True:
        try:
            now = now_jst()
            if now >= end + timedelta(minutes=INTERVAL_MINUTES):
                break
            target = _next_tick(max(now, start))
            await asyncio.sleep(max(0.0, (target - now_jst()).total_seconds()))

            # 同じスプレッドシートを複数のサーバーで /setup している場合や、ECS のローリングデプロイ中に
            # 新旧タスクが動いている場合に、同じシートへ二重に書き込まないよう、スプレッドシート単位で1回だけ実行する
            if not storage.mark_tick_if_new(0, f"{spreadsheet_id}#minutelog", target.isoformat()):
                continue
            await run_once(guild_id, spreadsheet_id, cfg)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"[MinuteLog] guild={guild_id} ss={spreadsheet_id}: {type(e).__name__}: {e}")
            await asyncio.sleep(60)
