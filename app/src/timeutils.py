# timeutils.py
from __future__ import annotations
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

JST = ZoneInfo("Asia/Tokyo")

def now_jst() -> datetime:
    return datetime.now(tz=JST)

def ensure_aware_jst(dt) -> datetime:
    if isinstance(dt, str):
        s = dt.strip()
        if " " in s and "T" not in s:
            s = s.replace(" ", "T", 1)
        if s.endswith("Z"):
            d = datetime.fromisoformat(s[:-1]).replace(tzinfo=ZoneInfo("UTC"))
        else:
            d = datetime.fromisoformat(s)
    else:
        d = dt
    if d.tzinfo is None:
        d = d.replace(tzinfo=JST)
    else:
        d = d.astimezone(JST)
    return d

def first_tick_on_or_after(dt: datetime, minute: int) -> datetime:
    dt = ensure_aware_jst(dt).astimezone(JST)
    minute = max(0, min(59, int(minute)))
    base = dt.replace(second=0, microsecond=0)
    candidate = base.replace(minute=minute)
    if candidate < dt:
        candidate = (base.replace(minute=0) + timedelta(hours=1)).replace(minute=minute)
    return candidate