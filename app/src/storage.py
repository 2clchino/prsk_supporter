# storage.py
from __future__ import annotations
import json
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import os as _os
_DATA_DIR = Path(_os.environ.get("DATA_DIR", "."))
_STORE_PATH = _DATA_DIR / "config_store.json"

def _read_all() -> Dict[str, Any]:
    if not _STORE_PATH.exists():
        return {}
    try:
        return json.loads(_STORE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}

def _write_all(data: Dict[str, Any]) -> None:
    tmp = _STORE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(_STORE_PATH)

def _is_old_format(guild_data: Any) -> bool:
    return isinstance(guild_data, dict) and "SpreadsheetID" in guild_data

def _get_guild_spreadsheets(data: Dict[str, Any], guild_id_str: str) -> Dict[str, Dict[str, Any]]:
    """Returns {spreadsheet_id: config}, migrating old flat-config format if needed."""
    guild_data = data.get("guilds", {}).get(guild_id_str, {})
    if _is_old_format(guild_data):
        ss_id = guild_data.get("SpreadsheetID", "")
        return {ss_id: guild_data} if ss_id else {}
    return {k: v for k, v in guild_data.items() if isinstance(v, dict)}

def try_acquire_lease(guild_id: int, instance_id: str, ttl_sec: int = 3600) -> bool:
    data = _read_all()
    leases = data.setdefault("_leases", {})
    from time import time
    now = time()
    info = leases.get(str(guild_id))
    if info and now < info.get("expires_at", 0) and info.get("instance_id") != instance_id:
        return False
    leases[str(guild_id)] = {"instance_id": instance_id, "expires_at": now + ttl_sec}
    _write_all(data)
    return True

def mark_tick_if_new(guild_id: int, spreadsheet_id: str, tick_iso: str) -> bool:
    data = _read_all()
    ticks = data.setdefault("_ticks", {})
    key = f"{guild_id}:{spreadsheet_id}"
    if ticks.get(key) == tick_iso:
        return False
    ticks[key] = tick_iso
    _write_all(data)
    return True

def release_lease(guild_id: int, instance_id: str):
    data = _read_all()
    leases = data.get("_leases", {})
    if leases.get(str(guild_id), {}).get("instance_id") == instance_id:
        leases.pop(str(guild_id), None)
        _write_all(data)

def save_guild_config(guild_id: int, cfg: Dict[str, Any]) -> None:
    data = _read_all()
    guilds = data.setdefault("guilds", {})
    guild_str = str(guild_id)
    # migrate old flat format
    if _is_old_format(guilds.get(guild_str, {})):
        old = guilds[guild_str]
        old_ss = old.get("SpreadsheetID", "")
        guilds[guild_str] = {old_ss: old} if old_ss else {}
    guilds.setdefault(guild_str, {})[cfg["SpreadsheetID"]] = cfg
    _write_all(data)

def load_guild_config(guild_id: int, spreadsheet_id: str) -> Optional[Dict[str, Any]]:
    data = _read_all()
    return _get_guild_spreadsheets(data, str(guild_id)).get(spreadsheet_id)

def load_all_guild_configs(guild_id: int) -> Dict[str, Dict[str, Any]]:
    """Returns {spreadsheet_id: config} for a guild."""
    data = _read_all()
    return _get_guild_spreadsheets(data, str(guild_id))

def load_all_configs() -> Dict[Tuple[int, str], Dict[str, Any]]:
    """Returns {(guild_id, spreadsheet_id): config} for all guilds."""
    data = _read_all()
    result: Dict[Tuple[int, str], Dict[str, Any]] = {}
    for guild_id_str, guild_data in data.get("guilds", {}).items():
        if not guild_id_str.isdigit():
            continue
        guild_id = int(guild_id_str)
        for ss_id, cfg in _get_guild_spreadsheets(data, guild_id_str).items():
            result[(guild_id, ss_id)] = cfg
    return result

def _pending_refle_key(guild_id: int, spreadsheet_id: str, tracking_key) -> str:
    return f"{guild_id}:{spreadsheet_id}:{tracking_key}"

def save_pending_refle(guild_id: int, spreadsheet_id: str, tracking_key, value: float, time_iso: str) -> None:
    """次回の時報で消化される、プレイヤーごとの手動リフレ値補正(値・入力時刻)を保存する。"""
    data = _read_all()
    pending = data.setdefault("_pending_refle", {})
    pending[_pending_refle_key(guild_id, spreadsheet_id, tracking_key)] = {"value": value, "time": time_iso}
    _write_all(data)

def get_pending_refle(guild_id: int, spreadsheet_id: str, tracking_key) -> Optional[Dict[str, Any]]:
    data = _read_all()
    return data.get("_pending_refle", {}).get(_pending_refle_key(guild_id, spreadsheet_id, tracking_key))

def clear_pending_refle(guild_id: int, spreadsheet_id: str, tracking_key) -> None:
    data = _read_all()
    pending = data.get("_pending_refle", {})
    key = _pending_refle_key(guild_id, spreadsheet_id, tracking_key)
    if key in pending:
        pending.pop(key)
        _write_all(data)

def delete_guild_config(guild_id: int, spreadsheet_id: str = None) -> None:
    data = _read_all()
    guilds = data.get("guilds", {})
    guild_str = str(guild_id)
    if spreadsheet_id:
        if _is_old_format(guilds.get(guild_str, {})):
            guilds.pop(guild_str, None)
        else:
            guilds.get(guild_str, {}).pop(spreadsheet_id, None)
            if not guilds.get(guild_str):
                guilds.pop(guild_str, None)
    else:
        guilds.pop(guild_str, None)
    _write_all(data)
