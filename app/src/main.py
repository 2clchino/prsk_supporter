import os
import logging
import asyncio, random
from typing import Iterable, Tuple
import discord
from discord.ext import commands
from discord import app_commands
from dotenv import load_dotenv
import gspread_manager
import sekai_api
import shift_manager
import ptlogger
import storage
from timeutils import ensure_aware_jst, now_jst, JST
from scheduler import EventScheduler, MultiMinuteRegistry

load_dotenv(override=False)
TOKEN = os.environ["DISCORD_TOKEN"]
GUILD_ID = os.environ.get("GUILD_ID")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bot")

intents = discord.Intents.default()
if os.environ.get("ENABLE_MESSAGE_CONTENT", "0") == "1":
    intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)

registry = MultiMinuteRegistry()

@registry.every_hour_at_config("ChangeNotice")
async def shift_change(ctx: dict) -> str:
    cfg = ctx["config"]
    channel = ctx.get("channel")
    if channel is None:
        return "ChangeNotice(no-channel)"
    def runner_count(val) -> int:
        if isinstance(val, list):
            return len(val)
        return 1 if val else 0

    max_cols = max(1, 5 - runner_count(cfg.get("Runners")))
    rows = shift_manager.extract_nearest_shift(
        cfg.get("SpreadsheetID"),
        max_shifters_per_block=max_cols,
    )

    lines = []
    for i, item in enumerate(rows, 1):
        dt = item.get("datetime")
        try:
            dt = dt.astimezone(JST)
        except Exception:
            pass
        ts = dt.strftime("%m/%d %H:%M") if dt else "??:??"
        names = ", ".join(item.get("shifters", [])) or "（割当なし）"
        lines.append(f"{i}. {ts} — {names}")

    return None

@registry.every_hour_at_config("NextServer")
async def check_next_server(ctx: dict) -> str:
    cfg = ctx["config"]
    channel = ctx.get("channel")
    if channel is None:
        return "NextServer(no-channel)"
    def runner_count(val) -> int:
        if isinstance(val, list):
            return len(val)
        return 1 if val else 0

    max_cols = max(1, 5 - runner_count(cfg.get("Runners")))
    rows = shift_manager.extract_nearest_shift(
        cfg.get("SpreadsheetID"),
        max_shifters_per_block=max_cols,
    )

    lines = []
    for i, item in enumerate(rows, 1):
        dt = item.get("datetime")
        try:
            dt = dt.astimezone(JST)
        except Exception:
            pass
        ts = dt.strftime("%m/%d %H:%M") if dt else "??:??"
        names = ", ".join(item.get("shifters", [])) or "（割当なし）"
        lines.append(f"{i}. {ts} — {names}")

    return None

def _exp_backoff(attempt: int, base: float = 2.0, cap: float = 15.0, jitter: float = 0.25) -> float:
    d = min(cap, base * (2 ** (attempt - 1)))
    return d * (1 + (random.random() * 2 - 1) * jitter)

async def retry_async(call, *, attempts: int = 3, catch: Tuple[type, ...] = (Exception,)) -> any:
    last = None
    for n in range(1, attempts + 1):
        try:
            return await call()
        except catch as e:
            last = e
            if n == attempts:
                break
            await asyncio.sleep(_exp_backoff(n))
    raise last
from requests.exceptions import Timeout, ReadTimeout, ConnectionError as ReqConnError, RequestException

def _tracking_to_runner(cfg: dict) -> dict:
    trackings = cfg.get("Trackings") or []
    runners_raw = cfg.get("Runners") or []
    runners_list = runners_raw if isinstance(runners_raw, list) else ([runners_raw] if runners_raw else [])
    return {t: runners_list[i] for i, t in enumerate(trackings) if i < len(runners_list)}

def _tracking_to_discord_id(cfg: dict) -> dict:
    trackings = cfg.get("Trackings") or []
    ids_raw = cfg.get("DiscordIDs") or []
    ids_list = ids_raw if isinstance(ids_raw, list) else ([ids_raw] if ids_raw else [])
    return {t: str(ids_list[i]) for i, t in enumerate(trackings) if i < len(ids_list)}

def _event_ended(cfg: dict) -> bool:
    try:
        return now_jst() > ensure_aware_jst(cfg["EventEnd"])
    except Exception:
        return False

def _is_player(t) -> bool:
    if isinstance(t, str):
        return True
    return isinstance(t, int) and len(str(abs(t))) >= 15

_STALE_THRESHOLD_MINUTES = 30

async def _fetch_all_scores(cfg: dict) -> tuple[dict, dict, str, bool, dict]:
    if cfg.get("isWorldBloom"):
        times = sekai_api.get_chapter_time(cfg["EventID"], cfg["CharaID"])
    else:
        times = sekai_api.get_event_time(cfg["EventID"])

    raw = None
    last_time = None
    used_fallback = False

    if times:
        candidate = times[-1]
        age_minutes = (now_jst() - ensure_aware_jst(candidate)).total_seconds() / 60
        if age_minutes <= _STALE_THRESHOLD_MINUTES:
            if cfg.get("isWorldBloom"):
                raw = sekai_api.get_chapter_rankings(cfg["EventID"], cfg["CharaID"], candidate)
            else:
                raw = sekai_api.get_event_rankings(cfg["EventID"], candidate)
            if raw:
                last_time = candidate
        else:
            print(f"[INFO] api.sekai.best data is stale ({age_minutes:.0f} min old), falling back to seka.ing")

    if not raw:
        chara_id = cfg.get("CharaID") if cfg.get("isWorldBloom") else None
        raw = await asyncio.to_thread(sekai_api._get_leaderboard_sekai_ing, chara_id)
        if not raw:
            raise RuntimeError("API unavailable and fallback also failed")
        last_time = now_jst().strftime("%Y-%m-%dT%H:%M:%S%z")
        used_fallback = True

    trackings = cfg.get("Trackings") or []
    focus_raw = cfg.get("Focus") or []
    focus_targets = [focus_raw] if isinstance(focus_raw, int) else list(focus_raw)
    all_targets = trackings + [f for f in focus_targets if f not in trackings]
    scores = sekai_api.extract_scores(raw, all_targets)
    ranks = sekai_api.extract_ranks(raw, all_targets)
    resolved_names = sekai_api.resolve_display_names(raw, all_targets)
    return scores, ranks, last_time, used_fallback, resolved_names

_auto_prev_scores: dict[tuple, dict] = {}
_logger_prev_scores: dict[tuple, dict] = {}
_logger_prev_deltas: dict[tuple, dict] = {}

def _format_ranking_lines(
    cfg: dict, guild_id: int, spreadsheet_id: str,
    all_scores: dict, used_fallback: bool, *,
    update_prev: bool = True,
    prev_override: dict = None,
    all_ranks: dict = None,
    resolved_names: dict = None,
) -> str:
    trackings = cfg.get("Trackings") or []
    focus_raw = cfg.get("Focus") or []
    focus_targets = [focus_raw] if isinstance(focus_raw, int) else list(focus_raw)
    t2r = _tracking_to_runner(cfg)

    rankings = {k: v for k, v in all_scores.items() if k in trackings and v is not None}
    focus_scores = {k: v for k, v in all_scores.items() if k in focus_targets and v is not None}
    player_scores = {k: v for k, v in rankings.items() if _is_player(k)}

    log_interval = cfg.get("LogInterval", 30)
    rate_mult = max(1, 60 // log_interval)
    key = (guild_id, spreadsheet_id)
    prev = prev_override if prev_override is not None else _logger_prev_scores.get(key, {})
    prev_deltas = _logger_prev_deltas.get(key, {})

    def _fmt(n: int) -> str:
        return f"+{n:,}" if n >= 0 else f"{n:,}"

    def change_info(k, v) -> str:
        pv = prev.get(k)
        if pv is None:
            return ""
        delta = v - pv
        hourly_rate = delta * rate_mult
        pd = prev_deltas.get(k)
        dc_part = f" ({_fmt(delta - pd)})" if pd is not None else ""
        return f"　{_fmt(delta)}{dc_part}　{hourly_rate:,}/h"

    def _rank_of(k):
        r = all_ranks.get(k) if all_ranks else None
        return (r is None, r or 0)

    sorted_players = sorted(player_scores.items(), key=lambda item: _rank_of(item[0]))

    def _display(k) -> str:
        if k in t2r:
            return t2r[k]
        if resolved_names and k in resolved_names:
            return resolved_names[k]
        return str(k)

    lines = []
    for k, v in sorted_players:
        rank = all_ranks.get(k) if all_ranks else None
        rank_prefix = f"[{rank}位] " if rank is not None else ""
        lines.append(f"{rank_prefix}{_display(k)}: {v:,}{change_info(k, v)}")

    if focus_scores:
        if lines:
            lines.append("")
        for fk, fv in focus_scores.items():
            focus_line = f"{fk}: {fv:,}{change_info(fk, fv)}"
            for k, v in sorted_players:
                diff = v - fv
                focus_line += f"\n   | {_display(k)} {_fmt(diff)}"
            lines.append(focus_line)

    if update_prev:
        existing_scores = _logger_prev_scores.get(key, {})
        new_deltas = {k: v - existing_scores[k] for k, v in {**player_scores, **focus_scores}.items() if k in existing_scores}
        existing_deltas = _logger_prev_deltas.get(key, {})
        _logger_prev_deltas[key] = {**existing_deltas, **new_deltas}
        _logger_prev_scores[key] = {**existing_scores, **player_scores, **focus_scores}
    suffix = " (fallback)" if used_fallback else ""
    return "\n" + "\n".join(lines) + suffix if lines else f"api checked{suffix}"

class PointInputModal(discord.ui.Modal):
    point = discord.ui.TextInput(
        label="現在のポイント",
        placeholder="例: 1234567",
        max_length=20,
    )

    def __init__(self, tracking_key, spreadsheet_id: str, timestamp: str,
                 display_name: str = None, cfg: dict = None, guild_id: int = None,
                 view_ref: "MissingUsersView" = None):
        display = display_name or str(tracking_key)
        super().__init__(title=f"ポイント入力: {display[:40]}")
        self.tracking_key = tracking_key
        self.spreadsheet_id = spreadsheet_id
        self.timestamp = timestamp
        self.display_name = display
        self.cfg = cfg
        self.guild_id = guild_id
        self.view_ref = view_ref

    async def on_submit(self, interaction: discord.Interaction):
        try:
            score = int(str(self.point.value).replace(",", "").replace("_", ""))
        except ValueError:
            await interaction.response.send_message("数値を入力してください。", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        try:
            await asyncio.to_thread(
                ptlogger.write_values,
                self.spreadsheet_id,
                self.timestamp,
                {self.tracking_key: score},
            )
            if self.view_ref is not None:
                await self.view_ref.on_score_submitted(self.tracking_key, score)
            await interaction.followup.send(
                f"✅ {self.display_name} のポイント {score:,} を記録しました。", ephemeral=True
            )
        except Exception as e:
            try:
                await interaction.followup.send(f"記録に失敗しました: {e}", ephemeral=True)
            except Exception:
                pass

class PointInputButton(discord.ui.Button):
    def __init__(self, tracking_key, spreadsheet_id: str, timestamp: str,
                 display_name: str = None, cfg: dict = None, guild_id: int = None,
                 view_ref: "MissingUsersView" = None):
        display = display_name or str(tracking_key)
        super().__init__(label=display[:80], style=discord.ButtonStyle.primary)
        self.tracking_key = tracking_key
        self.spreadsheet_id = spreadsheet_id
        self.timestamp = timestamp
        self.display_name = display
        self.cfg = cfg
        self.guild_id = guild_id
        self.view_ref = view_ref

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(
            PointInputModal(self.tracking_key, self.spreadsheet_id, self.timestamp,
                            self.display_name, self.cfg, self.guild_id, self.view_ref)
        )

class RefreshValueModal(discord.ui.Modal, title="リフレ値ログ入力"):
    value = discord.ui.TextInput(
        label="リフレ値",
        placeholder="例: 34.5",
        max_length=30,
        required=True,
    )

    def __init__(self, spreadsheet_id: str, timestamp: str):
        super().__init__()
        self.spreadsheet_id = spreadsheet_id
        self.timestamp = timestamp

    async def on_submit(self, interaction: discord.Interaction):
        time_str = now_jst().strftime("%H:%M")
        formatted = f"{self.value.value.strip()} ({time_str})"
        await interaction.response.defer(ephemeral=True)
        try:
            await asyncio.to_thread(
                ptlogger.write_values,
                self.spreadsheet_id,
                self.timestamp,
                {"リフレ値ログ": formatted},
                overwrite=True,
            )
            await interaction.followup.send(f"✅ リフレ値 {formatted} を記録しました。", ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"記録に失敗しました: {e}", ephemeral=True)

class RefreshValueButton(discord.ui.Button):
    def __init__(self, spreadsheet_id: str, timestamp: str):
        super().__init__(label="リフレ値ログを入力", style=discord.ButtonStyle.secondary)
        self.spreadsheet_id = spreadsheet_id
        self.timestamp = timestamp

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(RefreshValueModal(self.spreadsheet_id, self.timestamp))

class MissingUsersView(discord.ui.View):
    def __init__(self, missing_keys: list, spreadsheet_id: str, timestamp: str,
                 tracking_to_runner: dict = None, cfg: dict = None, guild_id: int = None,
                 all_scores: dict = None, used_fallback: bool = False,
                 resolved_names: dict = None, timeout: float = 600):
        super().__init__(timeout=timeout)
        self.cfg = cfg
        self.guild_id = guild_id
        self.spreadsheet_id = spreadsheet_id
        self.all_scores = dict(all_scores or {})
        self.used_fallback = used_fallback
        self.resolved_names = resolved_names or {}
        self.pending_keys = set(missing_keys)
        self.message = None
        # snapshot prev at creation time — before _format_ranking_lines(update_prev=True) runs
        key = (guild_id, spreadsheet_id) if guild_id is not None else None
        self._prev_snapshot = dict(_logger_prev_scores.get(key, {})) if key else {}
        for key_btn in missing_keys:
            display = (tracking_to_runner or {}).get(key_btn) or self.resolved_names.get(key_btn)
            self.add_item(PointInputButton(key_btn, spreadsheet_id, timestamp, display, cfg, guild_id, view_ref=self))

    async def on_score_submitted(self, tracking_key, score: int) -> None:
        self.all_scores[tracking_key] = score
        self.pending_keys.discard(tracking_key)
        for child in self.children:
            if isinstance(child, PointInputButton) and child.tracking_key == tracking_key:
                child.disabled = True
                break
        # use the creation-time prev snapshot so hourly reflects the previous period,
        # not the values that _format_ranking_lines(update_prev=True) may have just written
        if self.message is not None and self.cfg is not None and self.guild_id is not None:
            try:
                ranking_str = _format_ranking_lines(
                    self.cfg, self.guild_id, self.spreadsheet_id,
                    self.all_scores, self.used_fallback,
                    update_prev=False, prev_override=self._prev_snapshot,
                    resolved_names=self.resolved_names,
                )
                header = "⚠️ 未入力あり" if self.pending_keys else "✅ 全員入力完了"
                await self.message.edit(content=header + ranking_str, view=self)
            except Exception:
                pass
        # update snapshot and logger so next submission within this view also gets correct prev
        self._prev_snapshot[tracking_key] = score
        if self.guild_id is not None:
            _logger_prev_scores.setdefault((self.guild_id, self.spreadsheet_id), {})[tracking_key] = score

@registry.every_hour_at_config("LogMinutes")
async def ranking_logger(ctx: dict) -> None:
    cfg = ctx["config"]
    guild_id = ctx["guild_id"]
    spreadsheet_id = ctx["spreadsheet_id"]
    channel = ctx.get("channel")
    target = ctx.get("now", now_jst())
    view_timeout = cfg.get("LogInterval", 60) * 60

    async def _run_once():
        all_scores, all_ranks, last_time, used_fallback, resolved_names = await _fetch_all_scores(cfg)

        trackings = cfg.get("Trackings") or []
        rankings = {k: v for k, v in all_scores.items() if k in trackings and v is not None}
        await asyncio.to_thread(ptlogger.write_values, cfg["SpreadsheetID"], last_time, rankings)

        missing = [t for t in trackings if t not in rankings]
        has_missing = bool(missing)
        if missing and channel:
            view = MissingUsersView(
                missing, cfg["SpreadsheetID"], last_time,
                _tracking_to_runner(cfg), cfg, guild_id,
                all_scores=all_scores, used_fallback=used_fallback,
                resolved_names=resolved_names, timeout=view_timeout,
            )
            initial_ranking = _format_ranking_lines(
                cfg, guild_id, spreadsheet_id, all_scores, used_fallback,
                update_prev=False, prev_override=view._prev_snapshot,
                all_ranks=all_ranks, resolved_names=resolved_names,
            )
            sent = await channel.send("⚠️ 未入力あり" + initial_ranking, view=view)
            view.message = sent

        ranking_str = _format_ranking_lines(
            cfg, guild_id, spreadsheet_id, all_scores, used_fallback,
            update_prev=True, all_ranks=all_ranks, resolved_names=resolved_names,
        )
        return ranking_str, last_time, cfg["SpreadsheetID"], has_missing

    try:
        ranking_str, last_time, ss_id, has_missing = await retry_async(
            _run_once,
            attempts=3,
            catch=(ReadTimeout, Timeout, ReqConnError, RequestException, RuntimeError, IndexError),
        )
    except Exception as e:
        print(f"[ranking_logger ERROR] {type(e).__name__}: {e}")
        if not channel:
            return None
        trackings = cfg.get("Trackings") or []
        focus_raw = cfg.get("Focus") or []
        focus_targets = [focus_raw] if isinstance(focus_raw, int) else list(focus_raw)
        player_trackings = [t for t in trackings if _is_player(t)]
        all_missing = player_trackings + [f for f in focus_targets if f not in player_trackings]
        last_time = now_jst().strftime("%Y-%m-%dT%H:%M:%S%z")
        view = MissingUsersView(
            all_missing, cfg["SpreadsheetID"], last_time,
            _tracking_to_runner(cfg), cfg, guild_id,
            all_scores={}, used_fallback=True,
            timeout=view_timeout,
        )
        sent = await channel.send(
            "⚠️ APIからデータを取得できませんでした。全員のポイントを手動で入力してください。",
            view=view,
        )
        view.message = sent
        return None

    if channel:
        refresh_view = discord.ui.View(timeout=view_timeout)
        refresh_view.add_item(RefreshValueButton(ss_id, last_time))
        if has_missing:
            # 未入力ありメッセージ送信済みのため本文は省略、リフレ値ログボタンのみ
            try:
                await channel.send(f"{target:%Y-%m-%d %H:%M} リフレ値ログ:", view=refresh_view)
            except Exception as e:
                print(f"[ranking_logger] refresh button send failed ({type(e).__name__}: {e})")
        else:
            raw_msg = f"{target:%Y-%m-%d %H:%M} 定期処理完了: {ranking_str}"
            full_msg = raw_msg if len(raw_msg) <= 1990 else raw_msg[:1989] + "…"
            try:
                await channel.send(full_msg, view=refresh_view)
            except Exception as e:
                print(f"[ranking_logger] view send failed ({type(e).__name__}: {e}), retrying without view")
                await _safe_send(channel, full_msg)
    return None

@registry.every_hour_at_config("AutoMinutes")
async def auto_ranking_check(ctx: dict) -> str:
    cfg = ctx["config"]
    guild_id = ctx["guild_id"]
    spreadsheet_id = ctx["spreadsheet_id"]
    channel = ctx.get("channel")

    in_auto = await asyncio.to_thread(
        shift_manager.is_auto_period, cfg.get("SpreadsheetID"), now_jst()
    )
    if not in_auto:
        return None

    async def _run_once():
        scores, _, _, _, _ = await _fetch_all_scores(cfg)
        return scores

    log_prefix = f"[AutoCheck] guild={guild_id} {now_jst().strftime('%H:%M')}"

    try:
        scores = await retry_async(
            _run_once,
            attempts=3,
            catch=(ReadTimeout, Timeout, ReqConnError, RequestException, RuntimeError, IndexError),
        )
    except Exception as e:
        print(f"{log_prefix}: データ取得失敗 ({type(e).__name__}: {e})")
        return None

    key = (guild_id, spreadsheet_id)
    prev = _auto_prev_scores.get(key)
    _auto_prev_scores[key] = scores

    t2r = _tracking_to_runner(cfg)
    t2did = _tracking_to_discord_id(cfg)
    trackings = cfg.get("Trackings") or []

    def _delta(k, v) -> str:
        if prev is None or prev.get(k) is None:
            return ""
        d = v - prev[k]
        return f"（{'+'if d>=0 else ''}{d:,}）"

    log_lines = [
        f"  {t2r.get(k, k)}: {scores[k]:,} {_delta(k, scores[k])}".rstrip()
        for k in trackings
        if _is_player(k) and scores.get(k) is not None
    ]
    print(f"{log_prefix}:\n" + "\n".join(log_lines) if log_lines else f"{log_prefix}: スコアなし")

    if prev is None:
        return None

    stalled_keys = [
        k for k in scores
        if _is_player(k) and k in prev and scores[k] is not None and prev[k] is not None and scores[k] <= prev[k]
    ]
    if stalled_keys and channel:
        stalled_lines = [f"{t2r.get(k, k)}: {scores[k]:,}（前回 {prev[k]:,}）" for k in stalled_keys]
        mentions = " ".join(f"<@{t2did[k]}>" for k in stalled_keys if k in t2did)
        prefix = mentions + "\n" if mentions else ""
        await channel.send(
            prefix + "⚠️ オート期間中にポイント増加が確認できませんでした。次のユーザは端末を確認してください:\n" + "\n".join(stalled_lines)
        )
    return None

scheduler = EventScheduler(bot, registry)

@bot.event
async def on_guild_join(guild: discord.Guild):
    bot.tree.copy_global_to(guild=guild)
    await bot.tree.sync(guild=guild)
    print(f"Synced commands to new guild {guild.id}")

@bot.event
async def on_ready():
    print(f"Logged in as {bot.user} (id={bot.user.id})")
    for guild in bot.guilds:
        try:
            bot.tree.copy_global_to(guild=guild)
            synced = await bot.tree.sync(guild=guild)
            print(f"Synced {len(synced)} commands to guild {guild.id} ({guild.name})")
        except Exception as e:
            print(f"[WARN] Failed to sync commands to guild {guild.id} ({guild.name}): {e}")
    bot.tree.clear_commands(guild=None)
    await bot.tree.sync()
    print("Cleared global commands")
    saved = storage.load_all_configs()
    print(f"Restoring {len(saved)} scheduler(s): {list(saved.keys())}")
    for (guild_id, spreadsheet_id), cfg in saved.items():
        if _event_ended(cfg):
            storage.delete_guild_config(guild_id, spreadsheet_id)
            print(f"Removed ended event for guild {guild_id} / ss {spreadsheet_id}")
            continue
        try:
            await scheduler.start_or_restart(guild_id, spreadsheet_id, cfg, on_event_start=_on_event_start)
            print(f"Restored scheduler for guild {guild_id} / ss {spreadsheet_id}")
        except Exception as e:
            print(f"[WARN] restore failed for guild {guild_id} / ss {spreadsheet_id}: {type(e).__name__}: {e}")

@bot.tree.command(name="help", description="botの使い方を表示")
async def help_command(interaction: discord.Interaction):
    msg = (
        "**プロセカ支援者用bot ヘルプ**\n"
        "\n"
        "__**セットアップ**__\n"
        "`/setup <スプレッドシートID>` — イベント設定を読み込み、定期実行を開始します。\n"
        "　スプレッドシートには 'Config' シートを作成し後に示す項目を記入してください。 \n"
        "　初回実行時に 'PtLogs' シートと 'Shift' シートを作成・フォーマットします。\n"
        "`/status` — 現在の設定とスケジューラの状態を表示します。\n"
        "`/clear_setup` — 設定を削除し、定期実行を停止します。\n"
        "\n"
        "__**スプレッドシート設定項目**__\n"
        "`EventName` — イベント名（部分一致）\n"
        "`ChapterNo` — WL チャプター番号（通常イベントは0または省略）\n"
        "`Trackings` — 記録するユーザー名または順位のリスト\n"
        "`Focus` — 差分を表示する順位\n"
        "`Runners` — ランナーのリスト\n"
        "`LogInterval` — ログ記録間隔（分）\n"
        "`ChangeNotice` — シフト通知を送る分（例: 55）\n"
        "\n"
        "__**定期実行（設定後に自動で動作）**__\n"
        "`ランキングログ` — LogInterval 分ごとにランキングを取得し、スプレッドシートに記録・チャンネルに投稿します。\n"
        "`シフト通知` — ChangeNotice で指定した時刻に直近のシフトをチャンネルに投稿します。(現在機能していません)\n"
        "`Autoチェック` — シフトシートで Auto と記入された時間帯に5分ごとポイント増加を監視します。\n"
    )
    await interaction.response.send_message(msg, ephemeral=False)

@bot.tree.command(name="ping", description="動作確認用。'Ping-Pong!'の文字列を返却")
async def ping(interaction: discord.Interaction):
    await interaction.response.send_message("PongPong!")
    
@bot.tree.command(name="setup", description="スプレッドシートのセットアップ")
@app_commands.describe(text="スプレッドシートID")
async def setup(interaction: discord.Interaction, text: str):
    await interaction.response.defer(ephemeral=True, thinking=True)
    config = gspread_manager.read_config_values(text)
    event_name = (config.get("EventName") or "").strip()
    chapter_no = int(config.get("ChapterNo") or 0)

    if event_name:
        if chapter_no > 0:
            event_id, _, _ = sekai_api.filter_event_info(sekai_api.get_event_info_by_name(event_name))
            chapter_info = sekai_api.get_chapter_info(event_id, chapter_no)
            if chapter_info is None:
                await interaction.followup.send(
                    f"設定エラー: イベント「{event_name}」のチャプター {chapter_no} が worldBlooms.json に見つかりませんでした。",
                    ephemeral=True,
                )
                return
            config["CharaID"], start, end = sekai_api.filter_chapter_info(chapter_info)
            config["isWorldBloom"] = True
        else:
            event_id, start, end = sekai_api.filter_event_info(
                sekai_api.get_event_info_by_name(event_name)
            )
            config["isWorldBloom"] = False
    else:
        try:
            event_id = int(str(config.get("EventID")).strip())
        except (TypeError, ValueError):
            await interaction.followup.send(
                "設定エラー: EventName も EventID も正しく取得できませんでした。", ephemeral=True
            )
            return

        event_start_raw = config.get("EventStart")
        event_end_raw = config.get("EventEnd")

        if not event_start_raw or not event_end_raw:
            try:
                ev_info = sekai_api.get_event_info_by_id(event_id)
                _, start, end = sekai_api.filter_event_info(ev_info)
                if not config.get("EventName"):
                    config["EventName"] = (ev_info.get("name") or "").strip() or None
            except Exception as e:
                await interaction.followup.send(
                    f"設定エラー: EventStart/EventEnd が未設定で、EventID からの自動取得にも失敗しました: {e}", ephemeral=True
                )
                return
        else:
            try:
                start = ensure_aware_jst(event_start_raw)
                end   = ensure_aware_jst(event_end_raw)
            except Exception as e:
                await interaction.followup.send(
                    f"設定エラー: EventStart/EventEnd の形式が不正です: {e}", ephemeral=True
                )
                return

        if chapter_no > 0:
            chapter_info = sekai_api.get_chapter_info(event_id, chapter_no)
            if chapter_info is None:
                await interaction.followup.send(
                    f"設定エラー: EventID {event_id} のチャプター {chapter_no} が worldBlooms.json に見つかりませんでした。",
                    ephemeral=True,
                )
                return
            config["CharaID"], start, end = sekai_api.filter_chapter_info(chapter_info)
            config["isWorldBloom"] = True
        else:
            config["isWorldBloom"] = False

    config["EventID"] = event_id
    config["EventStart"] = ensure_aware_jst(start).isoformat()
    config["EventEnd"]   = ensure_aware_jst(end).isoformat()
    config["ChannelID"] = int(interaction.channel_id)
    config["SpreadsheetID"] = text

    log_interval = config.get("LogInterval", 60)
    try:
        log_interval = int(log_interval)
        if log_interval <= 0 or log_interval > 60:
            log_interval = 60
    except (TypeError, ValueError):
        log_interval = 60
    config["LogInterval"] = log_interval
    config["LogMinutes"] = sorted(set((m + 1) % 60 for m in range(0, 60, log_interval)))
    config["AutoMinutes"] = list(range(0, 60, 5))

    guild_id = interaction.guild_id or 0
    is_update = storage.load_guild_config(guild_id, text) is not None
    storage.save_guild_config(guild_id, config)
    await scheduler.start_or_restart(guild_id, text, config, on_event_start=_on_event_start)
    runners = config.get("Runners")
    ptlogger.format_pt_table(text, start, end, config.get("Trackings"), interval_minutes=log_interval)
    shift_manager.format_shift_table(text, ensure_aware_jst(start), ensure_aware_jst(end))
    runners_str = ", ".join(runners) if isinstance(runners, list) else (str(runners) if runners is not None else "未設定")
    is_wb = config.get("isWorldBloom")
    event_name_for_msg = config.get("EventName") or f"(ID: {event_id})"
    change_min = int(config.get("ChangeNotice") or 0)
    log_minutes_str = ", ".join(f"{m:02d}" for m in config["LogMinutes"])
    chara_name = sekai_api._CHARA_ID_TO_NAME.get(config.get("CharaID"), str(config.get("CharaID"))) if is_wb else None
    prefix = "Updated: " if is_update else ""
    message = (
        f"{prefix}設定を保存し、定期実行を登録しました。\n"
        f"- イベント名: {event_name_for_msg}\n"
        f"- 種別: {f'WL {chara_name} チャプター' if is_wb else '通常イベント'}\n"
        f"- ランナー: {runners_str}\n"
        f"- 開始: {config['EventStart']}\n"
        f"- 終了: {config['EventEnd']}\n"
        f"- ログ記録: {log_interval}分間隔（毎時 {log_minutes_str} 分）\n"
        f"- 投稿チャンネル: <#{config['ChannelID']}>"
    )
    await interaction.followup.send(message, ephemeral=True)

async def _build_status_block(config: dict, status_str: str, *, ss_label: str = "") -> str:
    is_wb = config.get("isWorldBloom")
    event_name_for_msg = config.get("EventName") or f"(ID: {config.get('EventID')})"
    chara_name = sekai_api._CHARA_ID_TO_NAME.get(config.get("CharaID"), str(config.get("CharaID"))) if is_wb else None
    runners = config.get("Runners")
    runners_str = ", ".join(runners) if isinstance(runners, list) else (str(runners) if runners is not None else "未設定")
    log_interval = config.get("LogInterval", 60)
    log_minutes_str = ", ".join(f"{m:02d}" for m in config.get("LogMinutes", []))

    try:
        today = now_jst().date()
        auto_hours = await asyncio.to_thread(
            shift_manager.get_auto_hours, config.get("SpreadsheetID"), today
        )
        if auto_hours:
            ranges, rng_start = [], auto_hours[0]
            for prev, cur in zip(auto_hours, auto_hours[1:]):
                if cur != prev + 1:
                    ranges.append((rng_start, prev))
                    rng_start = cur
            ranges.append((rng_start, auto_hours[-1]))
            auto_str = ", ".join(
                f"{s:02d}:00" if s == e else f"{s:02d}:00-{e:02d}:00"
                for s, e in ranges
            )
        else:
            auto_str = "なし"
    except Exception:
        auto_str = "取得失敗"

    header = f"{status_str}{' — ' + ss_label if ss_label else ''}"
    return (
        f"{header}\n"
        f"- イベント名: {event_name_for_msg}\n"
        f"- 種別: {f'WL {chara_name} チャプター' if is_wb else '通常イベント'}\n"
        f"- ランナー: {runners_str}\n"
        f"- 開始: {config.get('EventStart')}\n"
        f"- 終了: {config.get('EventEnd')}\n"
        f"- ログ記録: {log_interval}分間隔（毎時 {log_minutes_str} 分）\n"
        f"- Auto監視（本日）: {auto_str}\n"
        f"- 投稿チャンネル: <#{config.get('ChannelID')}>"
    )

async def _on_event_start(cfg: dict, guild_id: int, channel) -> None:
    block = await _build_status_block(cfg, "▶ 稼働中")
    await channel.send(f"**イベントが開始しました！**\n{block}")

@bot.tree.command(name="status", description="現在の設定・スケジューラの状態を表示")
async def status(interaction: discord.Interaction):
    guild_id = interaction.guild_id or 0
    all_configs = storage.load_all_guild_configs(guild_id)
    if not all_configs:
        await interaction.response.send_message("設定がありません。`/setup` で設定してください。", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True, thinking=True)

    blocks = []
    for spreadsheet_id, config in list(all_configs.items()):
        if _event_ended(config):
            await scheduler.stop(guild_id, spreadsheet_id)
            storage.delete_guild_config(guild_id, spreadsheet_id)
            continue
    all_configs = {k: v for k, v in all_configs.items() if not _event_ended(v)}
    if not all_configs:
        await interaction.followup.send("終了したイベントの設定を削除しました。現在有効な設定はありません。", ephemeral=True)
        return
    multi = len(all_configs) > 1
    for spreadsheet_id, config in all_configs.items():
        if not scheduler.is_running(guild_id, spreadsheet_id):
            try:
                await scheduler.start_or_restart(guild_id, spreadsheet_id, config, on_event_start=_on_event_start)
                print(f"[status] auto-restarted scheduler for guild {guild_id} / ss {spreadsheet_id}")
            except Exception as e:
                print(f"[status] failed to auto-restart for guild {guild_id} / ss {spreadsheet_id}: {e}")
        is_running = scheduler.is_running(guild_id, spreadsheet_id)
        if not is_running:
            status_str = "⏹ 停止中"
        else:
            try:
                event_start = ensure_aware_jst(config.get("EventStart"))
                status_str = "⏸ イベント待機中" if now_jst() < event_start else "▶ 稼働中"
            except Exception:
                status_str = "▶ 稼働中"

        ss_label = f"`...{spreadsheet_id[-8:]}`" if multi else ""
        block = await _build_status_block(config, status_str, ss_label=ss_label)
        blocks.append(block)

    await interaction.followup.send("\n\n".join(blocks), ephemeral=True)

@bot.tree.command(name="clear_setup", description="保存済み設定を削除します（実行も停止）")
@app_commands.describe(spreadsheet_id="削除するスプレッドシートID（省略すると全削除）")
async def clear_setup(interaction: discord.Interaction, spreadsheet_id: str = None):
    guild_id = interaction.guild_id or 0
    await scheduler.stop(guild_id, spreadsheet_id)
    storage.delete_guild_config(guild_id, spreadsheet_id)
    if spreadsheet_id:
        await interaction.response.send_message(f"`...{spreadsheet_id[-8:]}` の設定を削除しました。", ephemeral=True)
    else:
        await interaction.response.send_message("全ての設定を削除しました。", ephemeral=True)

@bot.tree.error
async def on_app_command_error(
    interaction: discord.Interaction,
    error: app_commands.AppCommandError
):
    original = getattr(error, "original", error)
    logger.exception("Command error: %r", error)
    if isinstance(original, ValueError):
        msg = f"エラー: {original}"
    elif isinstance(error, app_commands.CommandOnCooldown):
        msg = f"クールダウン中です。{error.retry_after:.1f} 秒後に再試行してください。"
    elif isinstance(error, app_commands.MissingPermissions):
        msg = "権限が不足しています。"
    elif isinstance(error, app_commands.CheckFailure):
        msg = "このコマンドを実行できません。"
    else:
        msg = "予期しないエラーが発生しました。"
    try:
        if interaction.response.is_done():
            await interaction.followup.send(msg, ephemeral=True)
        else:
            await interaction.response.send_message(msg, ephemeral=True)
    except discord.InteractionResponded:
        await interaction.followup.send(msg, ephemeral=True)
    except discord.errors.NotFound:
        logger.warning("Interaction expired (10062): Could not send error message")
        
@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return
    if intents.message_content and message.content.strip().lower() == "ping":
        await message.channel.send("Pong!")
    await bot.process_commands(message)

# ---- Voice / Soundboard ----

_SOUNDS_DIR = os.path.join(os.path.dirname(__file__), "sounds")

def _ensure_test_tone():
    path = os.path.join(_SOUNDS_DIR, "test_tone.wav")
    if not os.path.exists(path):
        import sys
        sys.path.insert(0, _SOUNDS_DIR)
        from generate_test_tone import generate
        generate(path)
    return path

_ALLOWED_AUDIO_EXTENSIONS = {".mp3", ".wav", ".ogg", ".flac", ".m4a", ".aac", ".opus"}
_MAX_AUDIO_SIZE = 8 * 1024 * 1024  # 8MB

@bot.tree.command(name="play_sound", description="VCに入って音を鳴らす。ファイルを添付するとその音を再生。")
@app_commands.describe(file="再生する音声ファイル（mp3/wav/ogg など）。省略するとテスト音を再生。")
async def play_sound(interaction: discord.Interaction, file: discord.Attachment = None):
    if not interaction.user.voice or not interaction.user.voice.channel:
        await interaction.response.send_message("先にVCに入ってからコマンドを実行してください。", ephemeral=True)
        return

    vc_channel = interaction.user.voice.channel
    await interaction.response.defer(ephemeral=True)

    # 音声ファイルの準備
    tmp_path = None
    try:
        if file is not None:
            ext = os.path.splitext(file.filename)[1].lower()
            if ext not in _ALLOWED_AUDIO_EXTENSIONS:
                await interaction.followup.send(
                    f"対応していない形式です。対応形式: {', '.join(_ALLOWED_AUDIO_EXTENSIONS)}", ephemeral=True
                )
                return
            if file.size > _MAX_AUDIO_SIZE:
                await interaction.followup.send("ファイルサイズが大きすぎます（上限 8MB）。", ephemeral=True)
                return
            tmp_path = os.path.join("/tmp", f"play_sound_{interaction.id}{ext}")
            data = await file.read()
            await asyncio.to_thread(lambda: open(tmp_path, "wb").write(data))
            sound_path = tmp_path
            print(f"[play_sound] 添付ファイル保存: {file.filename} → {tmp_path}")
        else:
            sound_path = await asyncio.to_thread(_ensure_test_tone)
            print(f"[play_sound] テストトーン: {sound_path}")
    except Exception as e:
        print(f"[play_sound] ファイル準備失敗: {e}")
        await interaction.followup.send(f"音声ファイルの準備に失敗しました: {e}", ephemeral=True)
        return

    vc: discord.VoiceClient = discord.utils.get(bot.voice_clients, guild=interaction.guild)
    try:
        if vc is None:
            print("[play_sound] VCに接続中...")
            vc = await asyncio.wait_for(vc_channel.connect(), timeout=15.0)
            print("[play_sound] VC接続完了")
        elif vc.channel != vc_channel:
            print("[play_sound] VCを移動中...")
            await asyncio.wait_for(vc.move_to(vc_channel), timeout=15.0)
            print("[play_sound] VC移動完了")

        if vc.is_playing():
            vc.stop()

        def after_play(err):
            if err:
                print(f"[play_sound] 再生エラー: {err}")
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)
            asyncio.run_coroutine_threadsafe(vc.disconnect(), bot.loop)

        vc.play(
            discord.FFmpegPCMAudio(sound_path, options="-af adelay=1500|1500"),
            after=after_play,
        )
        label = file.filename if file else "テスト音"
        print(f"[play_sound] 再生開始: {label}")
        await interaction.followup.send(f"🔊 {vc_channel.name} で **{label}** を再生中...", ephemeral=True)

    except asyncio.TimeoutError:
        print("[play_sound] VC接続タイムアウト")
        await interaction.followup.send("VCへの接続がタイムアウトしました。botにVCへの接続・発言権限があるか確認してください。", ephemeral=True)
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)
        if vc and vc.is_connected():
            await vc.disconnect()
    except Exception as e:
        print(f"[play_sound] エラー: {type(e).__name__}: {e}")
        await interaction.followup.send(f"再生に失敗しました: {type(e).__name__}: {e}", ephemeral=True)
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)
        if vc and vc.is_connected():
            await vc.disconnect()

if __name__ == "__main__":
    bot.run(TOKEN, log_handler=None)