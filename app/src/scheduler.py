# scheduler.py
from __future__ import annotations
import asyncio
from typing import Awaitable, Callable, Dict, List, Any, Optional
from dataclasses import dataclass
import os, uuid
INSTANCE_ID = os.environ.get("INSTANCE_ID", str(uuid.uuid4()))
import discord
from collections import defaultdict
from timeutils import now_jst, ensure_aware_jst, first_tick_on_or_after, JST
import storage
import re
Callback = Callable[[dict], Awaitable[Any]] | Callable[[dict], Any]

def _cb_key(func) -> str:
    return getattr(func, "__qualname__", getattr(func, "__name__", repr(func)))

def _coerce_minutes(val) -> list[int]:
    out: list[int] = []
    def add_one(x):
        try:
            m = int(x)
            if 0 <= m <= 59:
                out.append(m)
        except Exception:
            pass
    if val is None:
        pass
    elif isinstance(val, (int, float)):
        add_one(val)
    elif isinstance(val, str):
        for tok in re.findall(r"\d+", val):
            add_one(tok)
    elif isinstance(val, (list, tuple, set)):
        for item in val:
            if isinstance(item, (int, float)):
                add_one(item)
            elif isinstance(item, str):
                for tok in re.findall(r"\d+", item):
                    add_one(tok)
    return sorted(set(out))

class MultiMinuteRegistry:
    def __init__(self) -> None:
        self._fixed: Dict[int, List[Callback]] = defaultdict(list)
        self._by_key: Dict[str, List[Callback]] = defaultdict(list)
        self._dedup: set[str] = set()

    def every_hour_at(self, minute: int):
        minute = max(0, min(59, int(minute)))
        def deco(func: Callback):
            k = _cb_key(func) + f"@fixed:{minute}"
            if k not in self._dedup:
                self._dedup.add(k)
                self._fixed[minute].append(func)
            return func
        return deco

    def every_hour_at_config(self, key: str):
        def deco(func: Callback):
            k = _cb_key(func) + f"@conf:{key}"
            if k not in self._dedup:
                self._dedup.add(k)
                self._by_key[key].append(func)
            return func
        return deco

    async def run_for_minute(self, minute: int, ctx: dict) -> List[Any]:
        results: List[Any] = []
        for cb in self._fixed.get(minute, []):
            results.append(await cb(ctx) if _is_coro(cb) else await _to_thread(cb, ctx))

        cfg = ctx.get("config", {})
        for key, cbs in self._by_key.items():
            mins = _coerce_minutes(cfg.get(key))
            if minute not in mins:
                continue
            for cb in cbs:
                results.append(await cb(ctx) if _is_coro(cb) else await _to_thread(cb, ctx))
        return results

def _is_coro(f): 
    import asyncio, inspect
    return inspect.iscoroutinefunction(f)

async def _to_thread(fn, *a, **kw):
    import asyncio
    return await asyncio.to_thread(fn, *a, **kw)

@dataclass
class ManagedJob:
    task: asyncio.Task
    guild_id: int

class EventScheduler:
    def __init__(self, bot: discord.Client, registry: MultiMinuteRegistry) -> None:
        self.bot = bot
        self.registry = registry
        self.jobs: Dict[int, ManagedJob] = {}
        self._locks: Dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)

    def is_running(self, guild_id: int) -> bool:
        return guild_id in self.jobs and not self.jobs[guild_id].task.done()

    async def start_or_restart(self, guild_id: int, cfg: dict) -> None:
        async with self._locks[guild_id]:
            await self.stop(guild_id)
            loop_task = asyncio.create_task(self._event_loop(guild_id, cfg))
            self.jobs[guild_id] = ManagedJob(task=loop_task, guild_id=guild_id)

    async def stop(self, guild_id: int) -> None:
        job = self.jobs.get(guild_id)
        if job and not job.task.done():
            job.task.cancel()
            try:
                await job.task
            except asyncio.CancelledError:
                pass
        self.jobs.pop(guild_id, None)

    async def _event_loop(self, guild_id: int, cfg: dict) -> None:
        channel_id = cfg.get("ChannelID")
        if not channel_id:
            return
        channel = self.bot.get_channel(int(channel_id)) or await self.bot.fetch_channel(int(channel_id))

        start = ensure_aware_jst(cfg["EventStart"])
        end   = ensure_aware_jst(cfg["EventEnd"])
        def pick_minutes(cfg: dict) -> list[int]:
            mins = set(range(1, 60, 30))
            return sorted(mins)

        minutes = pick_minutes(cfg)

        while True:
            now = now_jst()
            if now >= end:
                await _safe_send(channel, f"⏹️ イベント期間が終了しました（End: {end}）。定期実行を停止します。")
                break

            minutes_set = set(range(1, 60, 30))
            minutes = sorted(minutes_set)

            base = start if now < start else now
            candidates = [(m, first_tick_on_or_after(base, m)) for m in minutes]
            minute, target = min(candidates, key=lambda t: t[1])

            if target > end:
                await _safe_send(channel, f"⏹️ 次の実行時刻がイベント終了後のため停止します（Next: {target}, End: {end}）。")
                break

            await asyncio.sleep(max(0.0, (target - now_jst()).total_seconds()))

            tick_iso = target.strftime("%Y-%m-%dT%H:%M:%S%z")
            if not storage.mark_tick_if_new(guild_id, tick_iso):
                continue

            if start <= target <= end:
                ctx = {"guild_id": guild_id, "config": cfg, "now": target, "channel": channel}
                try:
                    results = await self.registry.run_for_minute(minute, ctx)
                except Exception as e:
                    await _safe_send(channel, f"⚠️ 毎時処理でエラー: {type(e).__name__}: {e}")
                    continue

                summary = " / ".join([_shorten(str(r)) for r in results if r is not None]) or "OK"
                await _safe_send(channel, f"⏱️ {target:%Y-%m-%d %H:%M}（毎時{minute:02d}分）定期処理完了: {summary}")
                
async def _safe_send(channel: discord.abc.Messageable, content: str) -> None:
    try:
        await channel.send(content)
    except Exception:
        pass

def _shorten(s: str, n: int = 100) -> str:
    return s if len(s) <= n else s[: n - 1] + "…"