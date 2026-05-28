# scheduler.py
from __future__ import annotations
import asyncio
from typing import Awaitable, Callable, Dict, List, Any, Optional, Tuple
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

def _compute_log_minutes(cfg: dict) -> list[int]:
    interval = cfg.get("LogInterval", 60)
    try:
        interval = int(interval)
        if interval <= 0 or interval > 60:
            interval = 60
    except (TypeError, ValueError):
        interval = 60
    return sorted(set((m + 1) % 60 for m in range(0, 60, interval)))

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

JobKey = Tuple[int, str]  # (guild_id, spreadsheet_id)

@dataclass
class ManagedJob:
    task: asyncio.Task
    guild_id: int
    spreadsheet_id: str

class EventScheduler:
    def __init__(self, bot: discord.Client, registry: MultiMinuteRegistry) -> None:
        self.bot = bot
        self.registry = registry
        self.jobs: Dict[JobKey, ManagedJob] = {}
        self._locks: Dict[JobKey, asyncio.Lock] = defaultdict(asyncio.Lock)

    def is_running(self, guild_id: int, spreadsheet_id: str) -> bool:
        key = (guild_id, spreadsheet_id)
        return key in self.jobs and not self.jobs[key].task.done()

    def running_spreadsheets(self, guild_id: int) -> List[str]:
        return [ss_id for (gid, ss_id), job in self.jobs.items()
                if gid == guild_id and not job.task.done()]

    async def start_or_restart(self, guild_id: int, spreadsheet_id: str, cfg: dict, on_event_start=None) -> None:
        key = (guild_id, spreadsheet_id)
        async with self._locks[key]:
            await self.stop(guild_id, spreadsheet_id)
            loop_task = asyncio.create_task(self._event_loop(guild_id, spreadsheet_id, cfg, on_event_start))
            self.jobs[key] = ManagedJob(task=loop_task, guild_id=guild_id, spreadsheet_id=spreadsheet_id)

    async def stop(self, guild_id: int, spreadsheet_id: str = None) -> None:
        keys = [(guild_id, spreadsheet_id)] if spreadsheet_id else \
               [k for k in list(self.jobs) if k[0] == guild_id]
        for key in keys:
            job = self.jobs.get(key)
            if job and not job.task.done():
                job.task.cancel()
                try:
                    await job.task
                except asyncio.CancelledError:
                    pass
            self.jobs.pop(key, None)

    async def _event_loop(self, guild_id: int, spreadsheet_id: str, cfg: dict, on_event_start=None) -> None:
        channel_id = cfg.get("ChannelID")
        if not channel_id:
            return
        try:
            channel = self.bot.get_channel(int(channel_id)) or await self.bot.fetch_channel(int(channel_id))
        except Exception as e:
            print(f"[ERROR] guild {guild_id}: failed to get channel: {type(e).__name__}: {e}")
            return

        start = ensure_aware_jst(cfg["EventStart"])
        end   = ensure_aware_jst(cfg["EventEnd"])
        log_minutes = _compute_log_minutes(cfg)
        auto_minutes = _coerce_minutes(cfg.get("AutoMinutes"))
        all_minutes = sorted(set(log_minutes + auto_minutes)) if auto_minutes else log_minutes
        event_announced = now_jst() >= start

        while True:
            try:
                now = now_jst()
                if now >= end:
                    await _safe_send(channel, f"イベント期間が終了しています（End: {end}）。定期処理は実行されません。")
                    storage.delete_guild_config(guild_id, spreadsheet_id)
                    break

                minutes = all_minutes

                base = start if now < start else now
                candidates = [(m, first_tick_on_or_after(base, m)) for m in minutes]
                minute, target = min(candidates, key=lambda t: t[1])

                if target > end:
                    await _safe_send(channel, f"いべおつ！定期処理を停止します。")
                    storage.delete_guild_config(guild_id, spreadsheet_id)
                    break

                await asyncio.sleep(max(0.0, (target - now_jst()).total_seconds()))

                tick_iso = target.strftime("%Y-%m-%dT%H:%M:%S%z")
                if not storage.mark_tick_if_new(guild_id, spreadsheet_id, tick_iso):
                    continue

                if start <= target <= end:
                    if not event_announced:
                        event_announced = True
                        if on_event_start:
                            try:
                                await on_event_start(cfg, guild_id, channel)
                            except Exception as e:
                                await _safe_send(channel, f"⚠️ イベント開始通知でエラー: {type(e).__name__}: {e}")
                    else:
                        ctx = {"guild_id": guild_id, "spreadsheet_id": spreadsheet_id, "config": cfg, "now": target, "channel": channel}
                        try:
                            results = await self.registry.run_for_minute(minute, ctx)
                        except Exception as e:
                            await _safe_send(channel, f"⚠️ 毎時処理でエラー: {type(e).__name__}: {e}")
                            continue

                        non_null = [str(r) for r in results if r is not None]
                        if non_null:
                            summary = " / ".join(non_null)
                            await _safe_send(channel, _shorten(f"{target:%Y-%m-%d %H:%M} 定期処理完了: {summary}", 1990))

            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"[ERROR] guild {guild_id}: _event_loop iteration crashed: {type(e).__name__}: {e}")
                await _safe_send(channel, f"⚠️ スケジューラでエラーが発生しました: {type(e).__name__}: {e}")
                await asyncio.sleep(60)
                
async def _safe_send(channel: discord.abc.Messageable, content: str) -> None:
    try:
        await channel.send(content)
    except Exception:
        pass

def _shorten(s: str, n: int = 100) -> str:
    return s if len(s) <= n else s[: n - 1] + "…"