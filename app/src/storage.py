# storage.py
from __future__ import annotations
import json
from pathlib import Path
from typing import Any, Dict, Optional

_STORE_PATH = Path("config_store.json")

def _read_all() -> Dict[str, Any]:
    if not _STORE_PATH.exists():
        return {}
    try:
        return json.loads(_STORE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}
    
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

def mark_tick_if_new(guild_id: int, tick_iso: str) -> bool:
    data = _read_all()
    g = data.setdefault("guilds", {}).setdefault(str(guild_id), {})
    if g.get("_last_tick") == tick_iso:
        return False
    g["_last_tick"] = tick_iso
    _write_all(data)
    return True

def release_lease(guild_id: int, instance_id: str):
    data = _read_all()
    leases = data.get("_leases", {})
    if leases.get(str(guild_id), {}).get("instance_id") == instance_id:
        leases.pop(str(guild_id), None)
        _write_all(data)

def _write_all(data: Dict[str, Any]) -> None:
    tmp = _STORE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(_STORE_PATH)

def _get_guilds_view(data: Dict[str, Any]) -> Dict[str, Any]:
    if isinstance(data.get("guilds"), dict):
        return data["guilds"]
    return {k: v for k, v in data.items() if isinstance(k, str) and k.isdigit()}

def save_guild_config(guild_id: int, cfg: Dict[str, Any]) -> None:
    data = _read_all()
    guilds = data.setdefault("guilds", {})
    guilds[str(guild_id)] = cfg
    _write_all(data)

def load_guild_config(guild_id: int) -> Optional[Dict[str, Any]]:
    data = _read_all()
    guilds = _get_guilds_view(data)
    return guilds.get(str(guild_id))

def load_all_configs() -> Dict[int, Dict[str, Any]]:
    data = _read_all()
    guilds = _get_guilds_view(data)
    return {int(k): v for k, v in guilds.items() if isinstance(v, dict)}

def delete_guild_config(guild_id: int) -> None:
    data = _read_all()
    if isinstance(data.get("guilds"), dict):
        data["guilds"].pop(str(guild_id), None)
    else:
        data.pop(str(guild_id), None)
    _write_all(data)