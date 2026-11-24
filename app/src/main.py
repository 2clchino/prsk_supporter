import os
import re
import json
import logging
import math
import asyncio, random
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Iterable, Tuple
import discord
from discord.ext import commands
from discord import app_commands
from dotenv import load_dotenv
import gspread_manager
import sekai_api
import shift_manager
import ptlogger
import minute_logger
import storage
from timeutils import ensure_aware_jst, now_jst, JST
from scheduler import EventScheduler, MultiMinuteRegistry, _safe_send

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

def _tracking_to_one_ebi(cfg: dict) -> dict:
    """`1Ebi` は Runners と同様、Trackings と位置で対応するリストとして指定できる
    （プレイヤーごとに1回あたりの想定ポイントが異なるため）。単一値の場合は全プレイヤー共通値として扱う。"""
    trackings = cfg.get("Trackings") or []
    raw = cfg.get("1Ebi")
    if raw is None or raw == "":
        return {}
    if isinstance(raw, list):
        mapping = {}
        for i, t in enumerate(trackings):
            if i >= len(raw):
                break
            try:
                val = float(raw[i])
            except (TypeError, ValueError):
                continue
            if val > 0:
                mapping[t] = val
        return mapping
    try:
        scalar = float(raw)
    except (TypeError, ValueError):
        return {}
    if scalar <= 0:
        return {}
    return {t: scalar for t in trackings if _is_player(t)}

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

def _focus_targets(cfg: dict) -> list:
    """Config の Focus を対象のリストにする。順位(数値)・ユーザー名・twitterId:xxx のいずれも可。
    単一値の場合はリストではなくそのまま入っているため包む(文字列を list() すると1文字ずつに分解されてしまう)。"""
    raw = cfg.get("Focus")
    if raw in (None, ""):
        return []
    if isinstance(raw, list):
        return [f for f in raw if f not in (None, "")]
    return [raw]

def _score_targets(cfg: dict) -> list:
    trackings = cfg.get("Trackings") or []
    focus_targets = _focus_targets(cfg)
    return trackings + [f for f in focus_targets if f not in trackings]

async def _fetch_all_scores(cfg: dict, *, prefer_realtime: bool = False) -> tuple[dict, dict, str, bool, dict]:
    """prefer_realtime の場合は、毎分更新される jp.seka.ing(100位まで)を先に見る。
    そこに見つからないプレイヤーだけ通常の取得結果(api.sekai.best)で補う。"""
    if prefer_realtime:
        chara_id = cfg.get("CharaID") if cfg.get("isWorldBloom") else None
        try:
            raw = await asyncio.to_thread(sekai_api._get_leaderboard_sekai_ing, chara_id)
        except Exception as e:
            print(f"[INFO] seka.ing の取得に失敗したため通常の取得に切り替えます ({type(e).__name__}: {e})")
            raw = None
        if raw:
            all_targets = _score_targets(cfg)
            scores = sekai_api.extract_scores(raw, all_targets)
            ranks = sekai_api.extract_ranks(raw, all_targets)
            resolved_names = sekai_api.resolve_display_names(raw, all_targets)
            last_time = now_jst().strftime("%Y-%m-%dT%H:%M:%S%z")
            missing = [t for t in all_targets if _is_player(t) and scores.get(t) is None]
            if not missing:
                return scores, ranks, last_time, True, resolved_names
            print(f"[INFO] seka.ing に見つからないプレイヤーを api.sekai.best で補います: {missing}")
            base_scores, base_ranks, _, _, base_names = await _fetch_all_scores(cfg)
            for t in missing:
                scores[t] = base_scores.get(t)
                ranks[t] = base_ranks.get(t)
                if t in base_names:
                    resolved_names[t] = base_names[t]
            return scores, ranks, last_time, True, resolved_names

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

    all_targets = _score_targets(cfg)
    scores = sekai_api.extract_scores(raw, all_targets)
    ranks = sekai_api.extract_ranks(raw, all_targets)
    resolved_names = sekai_api.resolve_display_names(raw, all_targets)
    return scores, ranks, last_time, used_fallback, resolved_names

_auto_prev_scores: dict[tuple, dict] = {}
_logger_prev_scores: dict[tuple, dict] = {}
_logger_prev_deltas: dict[tuple, dict] = {}

REFLE_VALUE_RATE = 0.1998  # 回数1回あたりのリフレ値蓄積率(%)
REFLE_DECAY_RATE_PER_30MIN = 8.33  # Auto中/ポイント増加なし時の30分あたり減衰率(%)

def _round_half_up(x: float) -> int:
    return int(Decimal(str(x)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))

async def _compute_refle_info(
    cfg: dict, guild_id: int, spreadsheet_id: str, last_time: str, report_time,
    trackings: list, all_scores: dict, prev_scores: dict,
) -> dict:
    """Config の `1Ebi`(1回あたりの想定ポイント)が設定されている場合、プレイヤーごとに
    回数・リフレ値増分を算出し、PtLogs に記録して累積値を返す。

    直近の報告以降に手動補正(RefleValueModal)が入力されていた場合は、その時刻以降分
    （時報間隔のうち手動入力時刻〜時報時刻の割合）の回数だけを手動入力値に加算し、
    累積値を手動入力値基準に更新する。この割合計算の基準時刻には、外部APIのランキング
    スナップショット時刻(last_time、最大30分まで古いデータの使用を許容しているため不安定)
    ではなく、スケジューラが実際に報告を実行した時刻(report_time)を用いる。

    手動補正が無い区間では、以下のいずれかに該当する場合は通常の加算の代わりに
    REFLE_DECAY_RATE(30分あたり)分だけ減衰させる:
      - Shift シートで Auto と判定される時間帯（実際のポイント増加量は無視）
      - 回数(count)が0、すなわちポイントの増加が無かった時間帯

    戻り値: {tracking_key: {"count", "increment", "cumulative", "manual", "decay_reason"}}
    "manual" は手動補正が適用された場合のみ {"value", "time_str", "remaining_minutes", "interval_minutes"}。
    "decay_reason" は減衰が適用された場合のみ "auto" または "no_gain"。
    Auto判定は Shift シートの Runner 専用列(プレイヤーごと)を見るため、プレイヤーごとに個別に行う。"""
    one_ebi_map = _tracking_to_one_ebi(cfg)
    if not one_ebi_map:
        return {}

    try:
        interval_end = ensure_aware_jst(report_time)
    except Exception:
        interval_end = now_jst()
    interval_minutes = cfg.get("LogInterval", 30) or 30
    decay_amount = -(REFLE_DECAY_RATE_PER_30MIN * (interval_minutes / 30.0))
    t2r = _tracking_to_runner(cfg)

    # Auto判定は「今回の区間(例: 1:30〜2:00)がどの時間帯に属するか」で行う必要がある。
    # interval_end(時報の実行時刻、LogMinutesの+1補正で例えば2:01など)をそのまま使うと、
    # 区間の大半が1時台であっても2時台のAuto設定を見てしまう。区間の中間時刻を使うことで
    # 常にその区間が実際に属する時間帯(この例では1時台)のAuto設定を参照するようにする。
    auto_check_time = interval_end - timedelta(minutes=interval_minutes / 2)

    info: dict = {}
    for k in trackings:
        if not _is_player(k):
            continue  # 順位トラッキング(1,2,3...)にはリフレ値を適用しない
        one_ebi = one_ebi_map.get(k)
        if not one_ebi:
            continue

        runner_name = t2r.get(k)
        try:
            is_auto = bool(runner_name) and await asyncio.to_thread(
                shift_manager.is_auto_period, spreadsheet_id, auto_check_time, runner_name=runner_name,
            )
        except Exception as e:
            print(f"[refle_value] {k} のAuto判定に失敗: {type(e).__name__}: {e}")
            is_auto = False

        v = all_scores.get(k)
        pv = prev_scores.get(k)
        has_data = v is not None and pv is not None
        count = 0
        if has_data:
            delta = v - pv
            if delta > 0:
                count = _round_half_up(delta / one_ebi)

        pending = storage.get_pending_refle(guild_id, spreadsheet_id, k)
        if not pending and not is_auto and count <= 0 and not has_data:
            continue  # Auto でも手動補正もなく、増加の有無すら判断できない場合は何もしない

        header_name = ptlogger.refle_value_header(k)
        try:
            prior_cumulative = await asyncio.to_thread(ptlogger.get_latest_value, spreadsheet_id, header_name)
        except Exception as e:
            print(f"[refle_value] {k} の直前累積値取得に失敗: {type(e).__name__}: {e}")
            prior_cumulative = None
        if prior_cumulative is None:
            prior_cumulative = 0.0

        manual_note = None
        decay_reason = None
        if pending:
            try:
                manual_value = float(pending["value"])
                manual_time = ensure_aware_jst(pending["time"])
                remaining_seconds = max(0.0, (interval_end - manual_time).total_seconds())
                fraction = min(1.0, remaining_seconds / (interval_minutes * 60)) if interval_minutes > 0 else 1.0
                remaining_minutes = round(remaining_seconds / 60)
                manual_note = {
                    "value": manual_value,
                    "time_str": manual_time.strftime("%H:%M"),
                    "remaining_minutes": remaining_minutes,
                    "interval_minutes": interval_minutes,
                }
                new_cumulative = manual_value + count * fraction * REFLE_VALUE_RATE
            except Exception as e:
                print(f"[refle_value] {k} の手動補正の解釈に失敗: {type(e).__name__}: {e}")
                manual_note = None
                new_cumulative = prior_cumulative + (count * REFLE_VALUE_RATE if count > 0 else decay_amount)
        elif is_auto:
            new_cumulative = prior_cumulative + decay_amount
            decay_reason = "auto"
        elif count <= 0:
            new_cumulative = prior_cumulative + decay_amount
            decay_reason = "no_gain"
        else:
            new_cumulative = prior_cumulative + count * REFLE_VALUE_RATE

        new_cumulative = max(0.0, min(100.0, new_cumulative))  # リフレ値は 0%〜100% の範囲
        new_cumulative = round(new_cumulative, 2)
        increment = round(new_cumulative - prior_cumulative, 2)

        cumulative = None
        try:
            await asyncio.to_thread(
                ptlogger.write_values, spreadsheet_id, last_time, {header_name: new_cumulative}, overwrite=True,
            )
            cumulative = new_cumulative
            if manual_note is not None:
                storage.clear_pending_refle(guild_id, spreadsheet_id, k)
        except Exception as e:
            print(f"[refle_value] {k} の記録に失敗: {type(e).__name__}: {e}")

        info[k] = {
            "count": count, "increment": increment, "cumulative": cumulative,
            "manual": manual_note, "decay_reason": decay_reason,
        }
    return info

def _format_ranking_lines(
    cfg: dict, guild_id: int, spreadsheet_id: str,
    all_scores: dict, used_fallback: bool, *,
    update_prev: bool = True,
    prev_override: dict = None,
    prev_deltas_override: dict = None,
    all_ranks: dict = None,
    resolved_names: dict = None,
    refle_info: dict = None,
) -> str:
    trackings = cfg.get("Trackings") or []
    focus_targets = _focus_targets(cfg)
    t2r = _tracking_to_runner(cfg)

    rankings = {k: v for k, v in all_scores.items() if k in trackings and v is not None}
    focus_scores = {k: v for k, v in all_scores.items() if k in focus_targets and v is not None}
    player_scores = {k: v for k, v in rankings.items() if _is_player(k)}

    log_interval = cfg.get("LogInterval", 30)
    rate_mult = max(1, 60 // log_interval)
    key = (guild_id, spreadsheet_id)
    prev = prev_override if prev_override is not None else _logger_prev_scores.get(key, {})
    prev_deltas = prev_deltas_override if prev_deltas_override is not None else _logger_prev_deltas.get(key, {})

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
        base = f"　{_fmt(delta)}{dc_part}　{hourly_rate:,}/h"
        if refle_info and k in refle_info:
            entry = refle_info[k]
            cumulative = entry["cumulative"]
            if cumulative is not None:
                decay_reason = entry.get("decay_reason")
                if decay_reason == "auto":
                    detail = "Autoにより"
                elif decay_reason == "no_gain":
                    detail = "Pt増加なしにより"
                else:
                    detail = f"{entry['count']}回"
                base += f"　リフレ概算: {cumulative:.2f}% ({entry['increment']:+.2f}%, {detail})"
            manual = entry.get("manual")
            if manual and cumulative is not None:
                fraction = (
                    manual["remaining_minutes"] / manual["interval_minutes"]
                    if manual["interval_minutes"] else 0.0
                )
                effective_count = math.floor(entry["count"] * fraction)
                base += (
                    f"\n　　↳ 手動入力({manual['time_str']}) {manual['value']:.2f}% を反映。"
                    f" 以降{manual['remaining_minutes']}分間の"
                    f"{effective_count}回分のリフレ値を加算"
                )
        return base

    def _rank_of(k):
        r = all_ranks.get(k) if all_ranks else None
        return (r is None, r or 0)

    sorted_players = [(k, player_scores[k]) for k in trackings if k in player_scores]

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

    def _focus_label(fk) -> str:
        # 順位指定は「N位」、ユーザー名指定は現在順位を添えて表示する
        if isinstance(fk, int) and not _is_player(fk):
            return f"{fk}位"
        rank = all_ranks.get(fk) if all_ranks else None
        return f"[{rank}位] {_display(fk)}" if rank is not None else _display(fk)

    focus_lines = []
    for fk in focus_targets:
        fv = focus_scores.get(fk)
        if fv is None:
            focus_lines.append(f"{_focus_label(fk)}: 取得できませんでした")
            continue
        focus_line = f"{_focus_label(fk)}: {fv:,}{change_info(fk, fv)}"
        for k, v in sorted_players:
            if k == fk:
                continue
            focus_line += f"\n   | {_display(k)} {_fmt(v - fv)}"
        focus_lines.append(focus_line)
    if focus_lines:
        if lines:
            lines.append("")
        lines.extend(focus_lines)

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

class RefleValueModal(discord.ui.Modal):
    """1Ebi 設定時の、プレイヤーごとの累積リフレ値を手動補正するモーダル。
    入力値と入力時刻を保留し、次回の時報で入力時刻以降分の回数を加算したうえで累積値を更新する。"""
    value = discord.ui.TextInput(
        label="現在のリフレ値(%)",
        placeholder="例: 34.5",
        max_length=30,
        required=True,
    )

    def __init__(self, tracking_key, spreadsheet_id: str, guild_id: int, display_name: str = None):
        display = display_name or str(tracking_key)
        super().__init__(title=f"リフレ値修正: {display[:30]}")
        self.tracking_key = tracking_key
        self.spreadsheet_id = spreadsheet_id
        self.guild_id = guild_id
        self.display_name = display

    async def on_submit(self, interaction: discord.Interaction):
        try:
            val = float(str(self.value.value).strip().replace("%", ""))
        except ValueError:
            await interaction.response.send_message("数値を入力してください。", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        now = now_jst()
        try:
            storage.save_pending_refle(self.guild_id, self.spreadsheet_id, self.tracking_key, val, now.isoformat())
            await interaction.followup.send(
                f"✅ {self.display_name} のリフレ値を {val:.2f}% ({now:%H:%M}) として記録しました。"
                f"次回の時報で、この時刻以降分の回数を加算して累積値を更新します。",
                ephemeral=True,
            )
        except Exception as e:
            await interaction.followup.send(f"記録に失敗しました: {e}", ephemeral=True)

class RefleValueButton(discord.ui.Button):
    def __init__(self, tracking_key, spreadsheet_id: str, guild_id: int, display_name: str = None):
        display = display_name or str(tracking_key)
        super().__init__(label=f"{display[:60]} リフレ値修正", style=discord.ButtonStyle.secondary)
        self.tracking_key = tracking_key
        self.spreadsheet_id = spreadsheet_id
        self.guild_id = guild_id
        self.display_name = display

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(
            RefleValueModal(self.tracking_key, self.spreadsheet_id, self.guild_id, self.display_name)
        )

class MissingUsersView(discord.ui.View):
    def __init__(self, missing_keys: list, spreadsheet_id: str, timestamp: str,
                 tracking_to_runner: dict = None, cfg: dict = None, guild_id: int = None,
                 all_scores: dict = None, used_fallback: bool = False,
                 resolved_names: dict = None, timeout: float = 600,
                 prev_snapshot: dict = None, prev_deltas_snapshot: dict = None):
        super().__init__(timeout=timeout)
        self.cfg = cfg
        self.guild_id = guild_id
        self.spreadsheet_id = spreadsheet_id
        self.all_scores = dict(all_scores or {})
        self.used_fallback = used_fallback
        self.resolved_names = resolved_names or {}
        self.pending_keys = set(missing_keys)
        self.message = None
        # snapshot prev at creation time — before _format_ranking_lines(update_prev=True) runs.
        # 呼び出し元(ranking_logger)が PtLogs 書き込み前に解決した prev を渡す
        # (Bot再起動でメモリが空でもシートから復元済みの値)。
        key = (guild_id, spreadsheet_id) if guild_id is not None else None
        self._prev_snapshot = dict(prev_snapshot) if prev_snapshot is not None else (
            dict(_logger_prev_scores.get(key, {})) if key else {}
        )
        self._prev_deltas_snapshot = dict(prev_deltas_snapshot) if prev_deltas_snapshot is not None else (
            dict(_logger_prev_deltas.get(key, {})) if key else {}
        )
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
                    prev_deltas_override=self._prev_deltas_snapshot,
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

async def _resolve_prev_state(
    spreadsheet_id: str, last_time: str, trackings: list, memory_prev: dict, memory_prev_deltas: dict,
) -> tuple[dict, dict]:
    """メモリ上(_logger_prev_scores/_logger_prev_deltas)に前回値・前回増分(加速度表示用)が
    無いトラッキング対象について、PtLogs シートから一括で補完する。Bot再起動直後など
    メモリが空の場合でも、シートに記録済みの値から復元できるようにするためのフォールバック。

    今回これから書き込む行(last_time に最も近い行)の、真上の行のみを前回値とみなす。
    真上の行が空欄の場合は、さらに遡って古い値を探すことはしない
    (間に未記録の区間があるのに古い値を前回値扱いしてしまうと、複数区間分の
    増分を1区間分として誤計算してしまうため)。

    トラッキング数が多い場合に1件ずつシートへアクセスするとAPIクォータに当たりやすいため、
    不足分はまとめて1回のシート読み取り(get_recent_values_bulk)で取得する。
    必ず、今回のスコアを PtLogs に書き込む前に呼び出すこと（書き込んだ後だと
    「直前の値」が今回の値になってしまう）。"""
    prev = dict(memory_prev)
    prev_deltas = dict(memory_prev_deltas)
    missing = [k for k in trackings if k not in prev or k not in prev_deltas]
    if not missing:
        return prev, prev_deltas

    try:
        bulk = await asyncio.to_thread(
            ptlogger.get_recent_values_bulk, spreadsheet_id, [str(k) for k in missing], last_time, 2,
        )
    except Exception as e:
        print(f"[prev_score] 前回値の一括取得に失敗: {type(e).__name__}: {e}")
        bulk = {}

    for k in missing:
        values = bulk.get(str(k), [])
        if k not in prev and values:
            prev[k] = values[-1]
        if k not in prev_deltas and len(values) == 2:
            prev_deltas[k] = values[-1] - values[0]
    return prev, prev_deltas

# 同じスプレッドシートを複数のサーバーで /setup している場合、同じ時刻の取得・シートへの書き込みは
# 最初に実行したサーバーだけが行い、他のサーバーはその結果を使って Discord への投稿だけを行う
_SHARED_RESULT_TTL = 600
_shared_results: dict[tuple, tuple[float, object]] = {}
_shared_locks: dict[tuple, asyncio.Lock] = {}


async def _run_shared(key: tuple, factory) -> tuple[object, bool]:
    """key ごとに factory() を1回だけ実行し、その結果を返す。戻り値は (結果, このサーバーで実行したか)。
    失敗した場合は結果を共有しないので、待っていた他のサーバーが改めて実行する。"""
    loop = asyncio.get_running_loop()
    for k in [k for k, (t, _) in _shared_results.items() if loop.time() - t > _SHARED_RESULT_TTL]:
        _shared_results.pop(k, None)
        _shared_locks.pop(k, None)
    async with _shared_locks.setdefault(key, asyncio.Lock()):
        hit = _shared_results.get(key)
        if hit is not None:
            return hit[1], False
        result = await factory()
        _shared_results[key] = (loop.time(), result)
        return result, True


@registry.every_hour_at_config("LogMinutes")
async def ranking_logger(ctx: dict) -> None:
    cfg = ctx["config"]
    guild_id = ctx["guild_id"]
    spreadsheet_id = ctx["spreadsheet_id"]
    channel = ctx.get("channel")
    target = ctx.get("now", now_jst())
    view_timeout = cfg.get("LogInterval", 60) * 60

    trackings = cfg.get("Trackings") or []

    async def _fetch_and_write() -> dict:
        all_scores, all_ranks, last_time, used_fallback, resolved_names = await _fetch_all_scores(cfg)
        rankings = {k: v for k, v in all_scores.items() if k in trackings and v is not None}

        # 今回のスコアを PtLogs に書き込む前に、前回値・前々回値を解決する。
        # メモリ(_logger_prev_scores等)に無ければシートから復元する(Bot再起動直後など)。
        mem_key = (guild_id, spreadsheet_id)
        prev_scores, prev_deltas = await _resolve_prev_state(
            cfg["SpreadsheetID"], last_time, trackings,
            _logger_prev_scores.get(mem_key, {}), _logger_prev_deltas.get(mem_key, {}),
        )

        await asyncio.to_thread(ptlogger.write_values, cfg["SpreadsheetID"], last_time, rankings)

        # リフレ値はシート上の直前の累積値に加算して書き込むため、複数のサーバーで実行すると二重に加算される。
        # _run_shared で同じシート・同じ時刻については1回だけ実行する
        refle_info = await _compute_refle_info(
            cfg, guild_id, cfg["SpreadsheetID"], last_time, target, trackings, all_scores, prev_scores,
        )
        return {
            "all_scores": all_scores, "all_ranks": all_ranks, "last_time": last_time,
            "used_fallback": used_fallback, "resolved_names": resolved_names, "rankings": rankings,
            "prev_scores": prev_scores, "prev_deltas": prev_deltas, "refle_info": refle_info,
        }

    async def _run_once():
        shared, computed_here = await _run_shared((spreadsheet_id, "ranking_logger", target.isoformat()), _fetch_and_write)
        if not computed_here:
            print(f"[ranking_logger] guild={guild_id}: 同じスプレッドシートを使う他サーバーの取得・書き込み結果を使います")
        all_scores, all_ranks, last_time = shared["all_scores"], shared["all_ranks"], shared["last_time"]
        used_fallback, resolved_names, rankings = shared["used_fallback"], shared["resolved_names"], shared["rankings"]
        prev_scores, prev_deltas, refle_info = shared["prev_scores"], shared["prev_deltas"], shared["refle_info"]

        missing = [t for t in trackings if t not in rankings]
        has_missing = bool(missing)
        if missing and channel:
            view = MissingUsersView(
                missing, cfg["SpreadsheetID"], last_time,
                _tracking_to_runner(cfg), cfg, guild_id,
                all_scores=all_scores, used_fallback=used_fallback,
                resolved_names=resolved_names, timeout=view_timeout,
                prev_snapshot=prev_scores, prev_deltas_snapshot=prev_deltas,
            )
            initial_ranking = _format_ranking_lines(
                cfg, guild_id, spreadsheet_id, all_scores, used_fallback,
                update_prev=False, prev_override=prev_scores, prev_deltas_override=prev_deltas,
                all_ranks=all_ranks, resolved_names=resolved_names,
                refle_info=refle_info,
            )
            sent = await channel.send("⚠️ 未入力あり" + initial_ranking, view=view)
            view.message = sent

        ranking_str = _format_ranking_lines(
            cfg, guild_id, spreadsheet_id, all_scores, used_fallback,
            update_prev=True, prev_override=prev_scores, prev_deltas_override=prev_deltas,
            all_ranks=all_ranks, resolved_names=resolved_names,
            refle_info=refle_info,
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
        focus_targets = _focus_targets(cfg)
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
        if cfg.get("1Ebi"):
            t2r = _tracking_to_runner(cfg)
            for t in (cfg.get("Trackings") or []):
                if not _is_player(t):
                    continue
                refresh_view.add_item(RefleValueButton(t, ss_id, guild_id, t2r.get(t)))
        has_refle_buttons = len(refresh_view.children) > 0

        if has_missing:
            # 未入力ありメッセージ送信済みのため本文は省略、リフレ値修正ボタンのみ(1Ebi未設定なら送信しない)
            if has_refle_buttons:
                try:
                    await channel.send(f"{target:%Y-%m-%d %H:%M} リフレ値修正:", view=refresh_view)
                except Exception as e:
                    print(f"[ranking_logger] refle button send failed ({type(e).__name__}: {e})")
        else:
            raw_msg = f"{target:%Y-%m-%d %H:%M} 定期処理完了: {ranking_str}"
            full_msg = raw_msg if len(raw_msg) <= 1990 else raw_msg[:1989] + "…"
            try:
                if has_refle_buttons:
                    await channel.send(full_msg, view=refresh_view)
                else:
                    await channel.send(full_msg)
            except Exception as e:
                print(f"[ranking_logger] view send failed ({type(e).__name__}: {e}), retrying without view")
                await _safe_send(channel, full_msg)
    return None

async def auto_ranking_check(ctx: dict) -> str:
    cfg = ctx["config"]
    guild_id = ctx["guild_id"]
    spreadsheet_id = ctx["spreadsheet_id"]
    channel = ctx.get("channel")

    auto_runner_names = await asyncio.to_thread(
        shift_manager.get_auto_runner_names_at, cfg.get("SpreadsheetID"), now_jst()
    )
    key = (guild_id, spreadsheet_id)
    if not auto_runner_names:
        if _alarm_active(key) and _stop_alarm(guild_id, by_user=False) and channel:
            await _safe_send(channel, "⏹ Auto の時間帯が終わったため、アラームを止めました。")
        return None

    async def _fetch():
        # Auto 監視は停滞をすぐ検知したいので、よりリアルタイムな jp.seka.ing を優先する
        scores, _, _, _, _ = await _fetch_all_scores(cfg, prefer_realtime=True)
        return scores

    async def _run_once():
        # 同じスプレッドシートを使う他サーバーの同じ時刻の監視とは、取得結果を共有する
        tick = ctx.get("now")
        if tick is None:
            return await _fetch()
        scores, _ = await _run_shared((spreadsheet_id, "autocheck", tick.isoformat()), _fetch)
        return dict(scores)

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

    if key in _alarm_skip_next:
        # ユーザーがアラームを止めた直後の1回は、端末の確認・復旧の時間として判定しない
        _alarm_skip_next.discard(key)
        print(f"{log_prefix}: アラーム停止直後のため今回の判定はスキップします")
        return None

    if prev is None:
        return None

    stalled_keys = [
        k for k in scores
        if _is_player(k) and k in prev and scores[k] is not None and prev[k] is not None and scores[k] <= prev[k]
        and t2r.get(k) in auto_runner_names  # 本人がAuto中でなければ「停滞」として扱わない
    ]
    if not stalled_keys:
        if _alarm_active(key) and _stop_alarm(guild_id, by_user=False) and channel:
            await _safe_send(channel, "✅ ポイントの増加を確認したため、アラームを止めました。")
        return None
    if channel:
        stalled_lines = [f"{t2r.get(k, k)}: {scores[k]:,}（前回 {prev[k]:,}）" for k in stalled_keys]
        mentions = " ".join(f"<@{t2did[k]}>" for k in stalled_keys if k in t2did)
        prefix = mentions + "\n" if mentions else ""
        alarm_channel_id = cfg.get("AlarmChannelID")
        guild = bot.get_guild(guild_id) if alarm_channel_id else None
        await channel.send(
            prefix + "⚠️ オート期間中にポイント増加が確認できませんでした。次のユーザは端末を確認してください:\n" + "\n".join(stalled_lines),
            view=AlarmStopView(guild_id) if guild else discord.utils.MISSING,
        )
        if guild:
            raw_sound = cfg.get("AlarmSoundPath")
            _default_alarm = os.path.join(_SOUNDS_DIR, "alarm_0.mp3")
            sound_path = raw_sound if raw_sound and os.path.exists(raw_sound) else _default_alarm
            _start_alarm(guild, alarm_channel_id, sound_path, key)
    return None

DEFAULT_AUTO_CHECK_SECONDS = 300


def _auto_check_seconds(cfg: dict) -> int:
    try:
        sec = int(cfg.get("AutoCheckSeconds") or DEFAULT_AUTO_CHECK_SECONDS)
    except (TypeError, ValueError):
        sec = DEFAULT_AUTO_CHECK_SECONDS
    return max(60, sec)


async def auto_check_loop(bot_: discord.Client, guild_id: int, spreadsheet_id: str, cfg: dict) -> None:
    """Auto 監視を AutoCheckSeconds 秒(既定5分)毎に実行する。/set_alarm で選んだ楽曲の
    1回の時間に合わせた秒単位の間隔にするため、毎時の分指定のスケジューラとは別に回す。"""
    channel_id = cfg.get("ChannelID")
    if not channel_id:
        return
    try:
        channel = bot_.get_channel(int(channel_id)) or await bot_.fetch_channel(int(channel_id))
    except Exception as e:
        print(f"[AutoCheck] guild={guild_id}: チャンネル取得失敗: {type(e).__name__}: {e}")
        return
    start = ensure_aware_jst(cfg["EventStart"])
    end = ensure_aware_jst(cfg["EventEnd"])
    interval = _auto_check_seconds(cfg)
    while True:
        try:
            now = now_jst()
            if now >= end:
                break
            base = max(now, start)
            # 実行時刻を interval の倍数に揃える(既定の5分なら毎時 00,05,...分)。ECS で二重実行を防ぐ判定にも使う
            target_ts = (int(base.timestamp()) // interval + 1) * interval
            await asyncio.sleep(max(0.0, target_ts - now_jst().timestamp()))
            target = datetime.fromtimestamp(target_ts, tz=JST)
            if not storage.mark_tick_if_new(guild_id, f"{spreadsheet_id}#autocheck", target.isoformat()):
                continue
            ctx = {"guild_id": guild_id, "spreadsheet_id": spreadsheet_id, "config": cfg, "now": target, "channel": channel}
            await auto_ranking_check(ctx)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"[AutoCheck] guild={guild_id}: {type(e).__name__}: {e}")
            await asyncio.sleep(30)


scheduler = EventScheduler(bot, registry, aux_loops=[minute_logger.run_loop, auto_check_loop])

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
        "`/minutelog_targets <名前,名前,...>` — 'PtLogs1min' シート(1分毎のポイント推移)に記録するユーザーを、順位ではなくユーザー名で指定します（100位以内）。省略すると1〜5位に戻します。\n"
        "`/set_alarm` — Autoチェックのアラート時に、コマンド実行者がいるVCへBotが来てアラームを鳴らすよう設定します。\n"
        "　アラームは止めるまで（または次回以降の監視でポイント増加を確認するまで）鳴り続けます。\n"
        "　通知の「⏹ アラームを止める」ボタン、`/stop_alarm`、または VC から Bot を切断すると止まり、次回の監視は1回スキップします。\n"
        "　（例: 寝る前に寝床VCへ移動してから実行する想定）\n"
        "\n"
        "__**スプレッドシート設定項目**__\n"
        "`EventName` — イベント名（部分一致）\n"
        "`ChapterNo` — WL チャプター番号（通常イベントは0または省略）\n"
        "`Trackings` — 記録するユーザー名または順位のリスト\n"
        "`Focus` — 差分を表示する対象。順位またはユーザー名（Trackings と同じ書式、カンマ区切りで複数可）\n"
        "`Runners` — ランナーのリスト（Trackingsと同じ順番）\n"
        "`DiscordIDs` — ランナーのDiscord ID（Trackingsと同じ順番。Autoチェックの停滞通知でメンションします）\n"
        "`LogInterval` — ログ記録間隔（分）\n"
        "`1Ebi` — 1回のプレイで得られる想定ポイント数。\n"
        "　Runners と同様に Trackings の順番に対応したリストで指定。設定すると毎時報告で各プレイヤーの回数・リフレ値（回数×0.1998%、累積）を算出します。\n"
        "　以下の時間帯は加算の代わりに30分あたり-8.33%減衰します:\n"
        "　① Shift シートでそのランナーの列が Auto になっている時間帯\n　② ポイント増加が無かった時間帯\n"
        "　報告メッセージの「○○ リフレ値修正」ボタンからプレイヤーごとに現在値を手動補正できます\n"
        "　（補正した時刻以降分の回数を補正値に加算し、次回の時報で累積値を更新されるようになっています）。\n"
        "`ChangeNotice` — シフト通知を送る分（例: 55）\n"
        "\n"
        "__**Shift シート**__\n"
        "日付×時間のマスに加え、Runners の人数分の専用列が自動作成され、デフォルトはRunner名で埋まっています。\n"
        "該当する時間のセルを \"Auto\" に書き換えると、そのランナーはその時間帯Auto中として扱われます\n"
        "（リフレ値の減衰・Autoチェックの判定の両方で、プレイヤーごとに使われます）。\n"
        "\n"
        "__**定期実行（設定後に自動で動作）**__\n"
        "`ランキングログ` — LogInterval 分ごとにランキングを取得し、スプレッドシートに記録・チャンネルに投稿します。\n"
        "`シフト通知` — ChangeNotice で指定した時刻に直近のシフトをチャンネルに投稿します。(現在機能していません)\n"
        "`Autoチェック` — Shift シートで Auto と記入された時間帯に5分ごと（`/set_alarm` で楽曲を選ぶとその曲の1回の時間の1.5倍ごと）、\n"
        "　jp.seka.ing を優先してポイント増加を監視し、\n"
        "　停滞していたら DiscordIDs のユーザーにメンションで通知します。\n"
    )
    await interaction.response.send_message(msg, ephemeral=False)

@bot.tree.command(name="ping", description="動作確認用。'PingPong!'の文字列を返却")
async def ping(interaction: discord.Interaction):
    await interaction.response.send_message("PingPong!")
    
async def _execute_setup(interaction: discord.Interaction, config: dict, spreadsheet_id: str, extra_msg: str = "") -> None:
    """config と spreadsheet_id を受け取り、イベント情報解決 → スプシ整形 → スケジューラ起動を行う。"""
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
    config["SpreadsheetID"] = spreadsheet_id

    log_interval = config.get("LogInterval", 60)
    try:
        log_interval = int(log_interval)
        if log_interval <= 0 or log_interval > 60:
            log_interval = 60
    except (TypeError, ValueError):
        log_interval = 60
    config["LogInterval"] = log_interval
    config["LogMinutes"] = sorted(set((m + 1) % 60 for m in range(0, 60, log_interval)))

    guild_id = interaction.guild_id or 0
    prev_config = storage.load_guild_config(guild_id, spreadsheet_id)
    is_update = prev_config is not None
    # /set_alarm で設定した内容は Config シートに無いため、/setup し直しても引き継ぐ
    for k in _ALARM_CONFIG_KEYS:
        if prev_config and prev_config.get(k) is not None:
            config[k] = prev_config[k]
    storage.save_guild_config(guild_id, config)
    await scheduler.start_or_restart(guild_id, spreadsheet_id, config, on_event_start=_on_event_start)
    runners = config.get("Runners")
    runners_list = runners if isinstance(runners, list) else ([runners] if runners else [])
    ptlogger.format_pt_table(spreadsheet_id, start, end, config.get("Trackings") or [], interval_minutes=log_interval)
    shift_manager.format_shift_table(spreadsheet_id, ensure_aware_jst(start), ensure_aware_jst(end), runners=runners_list)
    runners_str = ", ".join(runners) if isinstance(runners, list) else (str(runners) if runners is not None else "未設定")
    is_wb = config.get("isWorldBloom")
    event_name_for_msg = config.get("EventName") or f"(ID: {event_id})"
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
        + extra_msg
    )
    await interaction.followup.send(message, ephemeral=True)


class ConfigFieldEditModal(discord.ui.Modal):
    """特定の設定項目を1つだけ編集するモーダル。フィールドを動的に構成する。"""

    def __init__(
        self,
        spreadsheet_id: str,
        config: dict,
        field_key: str,
        field_label: str,
        current_value: str,
        guild_id: int,
        style: discord.TextStyle = discord.TextStyle.short,
        max_length: int = 500,
    ):
        super().__init__(title=f"{field_label}を編集")
        self.spreadsheet_id = spreadsheet_id
        self.config = config
        self.field_key = field_key
        self.guild_id = guild_id
        self.text_input = discord.ui.TextInput(
            label=field_label,
            default=current_value,
            style=style,
            max_length=max_length,
            required=False,
        )
        self.add_item(self.text_input)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)
        raw = self.text_input.value.strip()

        if self.field_key == "Trackings":
            parsed = [gspread_manager._coerce_scalar(t) for t in raw.split(",") if t.strip()]
            self.config["Trackings"] = parsed
        elif self.field_key == "Runners":
            self.config["Runners"] = [r.strip() for r in raw.split(",") if r.strip()]
        elif self.field_key in ("Focus", "1Ebi"):
            # 単一値(全員共通)とカンマ区切りリスト(Trackingsの順に対応、プレイヤーごと)の両方を許容する
            parsed = [gspread_manager._coerce_scalar(t) for t in raw.split(",") if t.strip()]
            self.config[self.field_key] = parsed[0] if len(parsed) == 1 else parsed
        elif self.field_key == "LogInterval":
            try:
                val = int(raw)
                if not (1 <= val <= 60):
                    raise ValueError
                self.config["LogInterval"] = val
                self.config["LogMinutes"] = sorted(set((m + 1) % 60 for m in range(0, 60, val)))
            except ValueError:
                await interaction.followup.send("ログ間隔は 1〜60 の整数で指定してください。", ephemeral=True)
                return
        else:
            self.config[self.field_key] = raw

        storage.save_guild_config(self.guild_id, self.config)

        try:
            await asyncio.to_thread(gspread_manager.write_config_values, self.spreadsheet_id, self.config)
        except Exception as e:
            await interaction.followup.send(f"Config シートへの書き込みに失敗しました: {e}", ephemeral=True)
            return

        await scheduler.start_or_restart(self.guild_id, self.spreadsheet_id, self.config, on_event_start=_on_event_start)

        if self.field_key == "Trackings":
            try:
                ev_start = ensure_aware_jst(self.config.get("EventStart"))
                ev_end = ensure_aware_jst(self.config.get("EventEnd"))
                await asyncio.to_thread(
                    ptlogger.format_pt_table,
                    self.spreadsheet_id, ev_start, ev_end,
                    self.config.get("Trackings", []),
                    interval_minutes=self.config.get("LogInterval", 60),
                )
            except Exception:
                pass

        if self.field_key == "Runners":
            try:
                ev_start = ensure_aware_jst(self.config.get("EventStart"))
                ev_end = ensure_aware_jst(self.config.get("EventEnd"))
                await asyncio.to_thread(
                    shift_manager.format_shift_table,
                    self.spreadsheet_id, ev_start, ev_end,
                    runners=self.config.get("Runners", []),
                )
            except Exception:
                pass

        await interaction.followup.send("✅ 設定を更新しました。", ephemeral=True)


class StatusEditView(discord.ui.View):
    """ステータス表示に付随する設定編集ボタン群。"""

    def __init__(self, spreadsheet_id: str, config: dict, guild_id: int):
        super().__init__(timeout=120)

        def _str(val) -> str:
            if isinstance(val, list):
                return ", ".join(str(v) for v in val)
            return str(val) if val not in (None, "") else ""

        trk_str = _str(config.get("Trackings", []))
        focus_str = _str(config.get("Focus"))
        one_ebi_str = _str(config.get("1Ebi"))

        for label, key, field_label, cur_val, style, mlen in [
            ("追跡対象", "Trackings", "追跡対象（カンマ区切り）", trk_str, discord.TextStyle.paragraph, 500),
            ("フォーカス", "Focus", "差分表示対象（順位 or ユーザー名、カンマ区切り）", focus_str, discord.TextStyle.short, 200),
            ("1エビPt", "1Ebi", "1回のポイント（単一値 or カンマ区切り）", one_ebi_str, discord.TextStyle.short, 200),
            ("ログ間隔", "LogInterval", "ログ間隔（分・1〜60）", str(config.get("LogInterval", 60)), discord.TextStyle.short, 5),
        ]:
            btn = discord.ui.Button(label=label, style=discord.ButtonStyle.secondary)

            def make_callback(ss_id, cfg, fk, fl, cv, st, ml):
                async def callback(btn_interaction: discord.Interaction):
                    await btn_interaction.response.send_modal(
                        ConfigFieldEditModal(ss_id, dict(cfg), fk, fl, cv, guild_id, style=st, max_length=ml)
                    )
                return callback

            btn.callback = make_callback(spreadsheet_id, config, key, field_label, cur_val, style, mlen)
            self.add_item(btn)


@bot.tree.command(name="setup", description="スプレッドシートから設定を読み込み、定期実行を開始します。")
@app_commands.describe(text="スプレッドシートのID")
async def setup(interaction: discord.Interaction, text: str = None):
    if text is None:
        await interaction.response.send_message(
            "スプレッドシートIDを指定してください。例: `/setup <ID>`\n"
            "設定の編集は `/status` のボタンから行えます。",
            ephemeral=True,
        )
        return
    await interaction.response.defer(ephemeral=True, thinking=True)
    config = gspread_manager.read_config_values(text)
    await _execute_setup(interaction, config, text)

async def _build_status_block(config: dict, status_str: str, *, ss_label: str = "", guild_id: int = 0) -> str:
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

    trk = config.get("Trackings", [])
    trk_str = ", ".join(str(t) for t in trk) if isinstance(trk, list) else (str(trk) if trk else "未設定")
    minute_players = storage.get_minute_log_players(guild_id, config.get("SpreadsheetID")) if guild_id else []
    minute_players_str = ", ".join(minute_players) or "1〜5位"

    header = f"{status_str}{' — ' + ss_label if ss_label else ''}"
    return (
        f"{header}\n"
        f"- イベント名: {event_name_for_msg}\n"
        f"- 種別: {f'WL {chara_name} チャプター' if is_wb else '通常イベント'}\n"
        f"- 追跡対象: {trk_str}\n"
        f"- 1分ログ記録対象（/minutelog_targets）: {minute_players_str}\n"
        f"- ランナー: {runners_str}\n"
        f"- 開始: {config.get('EventStart')}\n"
        f"- 終了: {config.get('EventEnd')}\n"
        f"- ログ記録: {log_interval}分間隔（毎時 {log_minutes_str} 分）\n"
        f"- Auto監視（本日）: {auto_str}\n"
        f"- Auto監視の間隔: {_fmt_seconds(_auto_check_seconds(config))}"
        f"{'（' + config['AlarmSong'] + '）' if config.get('AlarmSong') else ''}\n"
        f"- 投稿チャンネル: <#{config.get('ChannelID')}>"
    )

async def _on_event_start(cfg: dict, guild_id: int, channel) -> None:
    block = await _build_status_block(cfg, "▶ 稼働中", guild_id=guild_id)
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
        block = await _build_status_block(config, status_str, ss_label=ss_label, guild_id=guild_id)
        view = StatusEditView(spreadsheet_id, config, guild_id)
        blocks.append((block, view))

    for i, (block, view) in enumerate(blocks):
        await interaction.followup.send(block, view=view, ephemeral=True)

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
_ALARM_SOUNDS_DIR = os.path.join(os.environ.get("DATA_DIR", "/app"), "alarm_sounds")

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
        if vc is not None and not vc.is_connected():
            # 前回の接続タイムアウト等で未接続のVoiceClientが残っていると、
            # 再接続されないまま play() が "Not connected to voice." で失敗し続けるため破棄する
            print("[play_sound] 未接続のVoiceClientを破棄")
            await vc.disconnect(force=True)
            vc = None
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

        await _wait_dave_ready(vc, "play_sound")
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


def _log_dave_state(vc: discord.VoiceClient, tag: str) -> None:
    """E2EE(DAVE)のセッションが準備できていないと音声が暗号化されずに送られ、相手側で再生されない。
    準備が整わなかった場合の調査用に状態を出力する。"""
    conn = vc._connection
    session = conn.dave_session
    print(f"[{tag}] DAVE: protocol={conn.dave_protocol_version} session={'あり' if session else 'なし'} "
          f"ready={getattr(session, 'ready', None)} can_encrypt={conn.can_encrypt}")


async def _wait_dave_ready(vc: discord.VoiceClient, tag: str, timeout: float = 15.0) -> bool:
    """接続直後は E2EE(DAVE)のセッションが準備中で、この間に送った音声は暗号化されず相手側で再生されない。
    準備が整うまで待つ。準備できた場合 True。"""
    loop = asyncio.get_running_loop()
    started = loop.time()
    while loop.time() - started < timeout:
        if not vc.is_connected():
            return False
        conn = vc._connection
        if conn.dave_protocol_version == 0 or conn.can_encrypt:
            print(f"[{tag}] DAVE ready ({loop.time() - started:.1f}s, protocol={conn.dave_protocol_version})")
            return True
        await asyncio.sleep(0.2)
    _log_dave_state(vc, tag)
    print(f"[{tag}] DAVE の準備が {timeout:.0f} 秒以内に整いませんでした")
    return False


_ALARM_MAX_SECONDS = 2 * 60 * 60  # 止め忘れ・監視停止時に鳴り続けないための上限


class _AlarmState:
    def __init__(self, key: tuple):
        self.key = key  # (guild_id, spreadsheet_id): どのスプレッドシートの監視で鳴らしたか
        self.stop_event = asyncio.Event()
        self.stopped_by_bot = False
        self.task: asyncio.Task | None = None


_alarms: dict[int, _AlarmState] = {}  # guild_id -> 鳴っているアラーム
_alarm_skip_next: set[tuple] = set()  # ユーザーがアラームを止めた後、次の1回の監視をスキップする (guild_id, spreadsheet_id)


def _alarm_active(key: tuple) -> bool:
    state = _alarms.get(key[0])
    return state is not None and state.key == key


def _start_alarm(guild: discord.Guild, channel_id: int, sound_path: str, key: tuple) -> None:
    """アラームを鳴らし始める。既に鳴っている場合は鳴らし直さない。"""
    if guild.id in _alarms:
        return
    state = _AlarmState(key)
    _alarms[guild.id] = state
    state.task = asyncio.create_task(_alarm_loop(guild, channel_id, sound_path, state))


def _stop_alarm(guild_id: int, *, by_user: bool) -> bool:
    """鳴っているアラームを止める。by_user の場合は次の1回の監視をスキップする。止めた場合 True。"""
    state = _alarms.pop(guild_id, None)
    if state is None:
        return False
    state.stopped_by_bot = True
    state.stop_event.set()
    if by_user:
        _alarm_skip_next.add(state.key)
    return True


async def _alarm_loop(guild: discord.Guild, channel_id: int, sound_path: str, state: _AlarmState) -> None:
    """指定チャンネルに入って sound_path を、止められるまで繰り返し再生し、終わったら退出する。"""
    vc_channel = guild.get_channel(channel_id)
    if vc_channel is None:
        print(f"[alarm] channel {channel_id} not found in guild {guild.id}")
        _alarms.pop(guild.id, None)
        return
    vc: discord.VoiceClient = discord.utils.get(bot.voice_clients, guild=guild)
    started = asyncio.get_running_loop().time()
    try:
        if vc is not None and not vc.is_connected():
            # 前回の接続タイムアウト等で未接続のVoiceClientが残っていると、
            # 再接続されないまま play() が "Not connected to voice." で失敗し続けるため破棄する
            print("[alarm] 未接続のVoiceClientを破棄")
            await vc.disconnect(force=True)
            vc = None
        if vc is None:
            vc = await asyncio.wait_for(vc_channel.connect(), timeout=15.0)
        elif vc.channel != vc_channel:
            await asyncio.wait_for(vc.move_to(vc_channel), timeout=15.0)
        await _wait_dave_ready(vc, "alarm")
        print(f"[alarm] 再生開始: {vc_channel.name} / {sound_path}")

        while not state.stop_event.is_set():
            if not vc.is_connected():
                # Bot が VC から切断された = ユーザーが止めたとみなす
                print("[alarm] VCから切断されたため、ユーザーによる停止として扱います")
                if _alarms.get(guild.id) is state:
                    _stop_alarm(guild.id, by_user=True)
                break
            if asyncio.get_running_loop().time() - started > _ALARM_MAX_SECONDS:
                print("[alarm] 上限時間に達したため停止します")
                if _alarms.get(guild.id) is state:
                    _stop_alarm(guild.id, by_user=False)
                break
            if vc.is_playing():
                vc.stop()
            finished = asyncio.Event()

            def after_alarm(err, finished=finished):
                if err:
                    print(f"[alarm] 再生エラー: {err}")
                bot.loop.call_soon_threadsafe(finished.set)

            _log_dave_state(vc, "alarm")
            vc.play(discord.FFmpegPCMAudio(sound_path, options="-af adelay=1500|1500"), after=after_alarm)
            stop_wait = asyncio.create_task(state.stop_event.wait())
            done_wait = asyncio.create_task(finished.wait())
            await asyncio.wait({stop_wait, done_wait}, return_when=asyncio.FIRST_COMPLETED)
            stop_wait.cancel()
            done_wait.cancel()
        print(f"[alarm] 停止: {vc_channel.name}")
    except Exception as e:
        print(f"[alarm] エラー: {type(e).__name__}: {e}")
        if _alarms.get(guild.id) is state:
            _alarms.pop(guild.id, None)
    finally:
        if vc and vc.is_connected():
            if vc.is_playing():
                vc.stop()
            await vc.disconnect()


class AlarmStopView(discord.ui.View):
    def __init__(self, guild_id: int):
        super().__init__(timeout=_ALARM_MAX_SECONDS)
        self.guild_id = guild_id

    @discord.ui.button(label="アラームを止める", emoji="⏹", style=discord.ButtonStyle.danger)
    async def stop_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if _stop_alarm(self.guild_id, by_user=True):
            await interaction.response.send_message(
                f"⏹ {interaction.user.display_name} がアラームを止めました。次回の監視は1回スキップし、その次から再開します。"
            )
        else:
            await interaction.response.send_message("アラームは鳴っていません。", ephemeral=True)


async def _check_missing_auto(all_configs: dict) -> list:
    """現在〜次の1時間に、Shift シートで Auto と記入されたランナーが1人もいない
    イベント(設定)の名前を返す。auto_ranking_check のアラート条件
    (is_auto_period が全体で True になること)と同じ判定を使っており、
    これに該当するとアラームが鳴る機会が無いまま終わってしまう。"""
    missing = []
    now = now_jst()
    for ss_id, cfg in all_configs.items():
        try:
            in_auto_now = await asyncio.to_thread(shift_manager.is_auto_period, ss_id, now)
            in_auto_next = await asyncio.to_thread(shift_manager.is_auto_period, ss_id, now + timedelta(hours=1))
        except Exception as e:
            print(f"[set_alarm] Auto判定に失敗 ({ss_id}): {type(e).__name__}: {e}")
            continue
        if not in_auto_now and not in_auto_next:
            missing.append(cfg.get("EventName") or ss_id)
    return missing

# /set_alarm で設定し、/setup し直しても引き継ぐ項目
_ALARM_CONFIG_KEYS = ("AlarmChannelID", "AlarmSoundPath", "AlarmSong", "AutoCheckSeconds")

# 周回する楽曲ごとの1回の時間(秒・曲間込み)。Auto 監視の間隔をこの1.5倍にする
_AUTO_CHECK_SONG_FACTOR = 1.5
with open(os.path.join(os.path.dirname(__file__), "song_times.json"), encoding="utf-8") as _f:
    _SONG_SECONDS: dict[str, float] = {s["name"]: s["seconds"] for s in json.load(_f)["songs"]}
_NO_SONG = "__none__"


def _fmt_seconds(sec: int) -> str:
    m, s = divmod(int(sec), 60)
    return f"{m}分{s:02d}秒" if s else f"{m}分"


def _song_select_options() -> list[discord.SelectOption]:
    options = [discord.SelectOption(
        label="選択しない",
        value=_NO_SONG,
        description=f"監視間隔: {_fmt_seconds(DEFAULT_AUTO_CHECK_SECONDS)}（デフォルト）",
        default=True,
    )]
    for name, sec in _SONG_SECONDS.items():
        options.append(discord.SelectOption(
            label=name[:100],
            value=name[:100],
            description=f"1回 {sec:g}秒 → 監視間隔: {_fmt_seconds(round(sec * _AUTO_CHECK_SONG_FACTOR))}",
        ))
    return options[:25]


class AlarmSetView(discord.ui.View):
    def __init__(self, vc_channel: discord.VoiceChannel, file: discord.Attachment | None, guild_id: int,
                 missing_auto: list = None):
        super().__init__(timeout=120)
        self.vc_channel = vc_channel
        self.file = file
        self.guild_id = guild_id
        self.missing_auto = missing_auto or []
        self.song: str | None = None

    @discord.ui.select(placeholder="周回する楽曲（Auto 監視の間隔を合わせます）", options=_song_select_options(), row=0)
    async def select_song(self, interaction: discord.Interaction, select: discord.ui.Select):
        value = select.values[0]
        self.song = None if value == _NO_SONG else value
        # 選択状態を表示に残す
        for opt in select.options:
            opt.default = opt.value == value
        await interaction.response.edit_message(view=self)

    @discord.ui.button(label="このVCで設定する", style=discord.ButtonStyle.primary, row=1)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)

        sound_path = None
        if self.file:
            ext = os.path.splitext(self.file.filename)[1].lower()
            if ext not in _ALLOWED_AUDIO_EXTENSIONS:
                await interaction.followup.send(
                    f"対応していない形式です。対応形式: {', '.join(_ALLOWED_AUDIO_EXTENSIONS)}", ephemeral=True
                )
                return
            if self.file.size > _MAX_AUDIO_SIZE:
                await interaction.followup.send("ファイルサイズが大きすぎます（上限 8MB）。", ephemeral=True)
                return
            os.makedirs(_ALARM_SOUNDS_DIR, exist_ok=True)
            save_path = os.path.join(_ALARM_SOUNDS_DIR, f"alarm_{self.guild_id}{ext}")
            data = await self.file.read()
            await asyncio.to_thread(lambda: open(save_path, "wb").write(data))
            sound_path = save_path

        all_configs = storage.load_all_guild_configs(self.guild_id)
        if not all_configs:
            await interaction.followup.send(
                "⚠️ 有効な設定がありません。先に `/setup` でセットアップを行ってください。", ephemeral=True
            )
            return
        check_seconds = (
            round(_SONG_SECONDS[self.song] * _AUTO_CHECK_SONG_FACTOR) if self.song else DEFAULT_AUTO_CHECK_SECONDS
        )
        for ss_id, cfg in all_configs.items():
            cfg["AlarmChannelID"] = self.vc_channel.id
            cfg["AlarmSoundPath"] = sound_path  # None = デフォルト音
            cfg["AlarmSong"] = self.song
            cfg["AutoCheckSeconds"] = check_seconds
            storage.save_guild_config(self.guild_id, cfg)
            await scheduler.start_or_restart(self.guild_id, ss_id, cfg, on_event_start=_on_event_start)

        sound_label = self.file.filename if self.file else "デフォルト音"
        song_label = f"{self.song}（1回 {_SONG_SECONDS[self.song]:g}秒 × {_AUTO_CHECK_SONG_FACTOR}）" if self.song else "選択なし"
        warning = (
            f"\n⚠️ Shift シート上で、現在〜次の1時間にAutoと記入されたランナーが見つかりません"
            f"（{', '.join(self.missing_auto)}）。この状態のままだとアラームは鳴りません。Shift シートの記入をご確認ください。"
        ) if self.missing_auto else ""
        await interaction.followup.send(
            f"🔔 アラームを設定しました。\n- VC: **{self.vc_channel.name}**\n- 音声: {sound_label}\n"
            f"- 楽曲: {song_label}\n- Auto 監視の間隔: {_fmt_seconds(check_seconds)}{warning}",
            ephemeral=True,
        )

    @discord.ui.button(label="キャンセル", style=discord.ButtonStyle.secondary, row=1)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(content="キャンセルしました。", view=None)


@bot.tree.command(name="set_alarm", description="オート監視アラート時にVCでアラームを鳴らす設定をします。")
@app_commands.describe(file="アラーム音声ファイル（省略するとデフォルト音）")
async def set_alarm(interaction: discord.Interaction, file: discord.Attachment = None):
    if not interaction.user.voice or not interaction.user.voice.channel:
        await interaction.response.send_message(
            "VCに入った状態でコマンドを実行してください。", ephemeral=True
        )
        return
    vc_channel = interaction.user.voice.channel
    guild_id = interaction.guild_id or 0
    sound_label = f"（音声: {file.filename}）" if file else ""

    missing_auto = []
    try:
        all_configs = storage.load_all_guild_configs(guild_id)
        if all_configs:
            missing_auto = await _check_missing_auto(all_configs)
    except Exception as e:
        print(f"[set_alarm] Auto判定に失敗: {type(e).__name__}: {e}")

    warning = (
        f"\n⚠️ Shift シート上で、現在〜次の1時間にAutoと記入されたランナーが見つかりません"
        f"（{', '.join(missing_auto)}）。この状態のままだとアラームは鳴りません。Shift シートの記入をご確認ください。"
    ) if missing_auto else ""

    await interaction.response.send_message(
        f"🔔 **{vc_channel.name}** でアラームを設定します{sound_label}。よろしいですか？\n"
        f"周回する楽曲を選ぶと、Auto 監視の間隔をその曲の1回の時間の{_AUTO_CHECK_SONG_FACTOR}倍にします"
        f"（選ばなければ{_fmt_seconds(DEFAULT_AUTO_CHECK_SECONDS)}間隔）。{warning}",
        view=AlarmSetView(vc_channel, file, guild_id, missing_auto=missing_auto),
        ephemeral=True,
    )


@bot.tree.command(name="stop_alarm", description="鳴っているアラームを止めます（次回の監視は1回スキップ）。")
async def stop_alarm(interaction: discord.Interaction):
    if _stop_alarm(interaction.guild_id or 0, by_user=True):
        await interaction.response.send_message(
            f"⏹ {interaction.user.display_name} がアラームを止めました。次回の監視は1回スキップし、その次から再開します。"
        )
    else:
        await interaction.response.send_message("アラームは鳴っていません。", ephemeral=True)


def _target_spreadsheets(guild_id: int, spreadsheet_id: str | None) -> dict:
    all_configs = storage.load_all_guild_configs(guild_id)
    if spreadsheet_id:
        return {k: v for k, v in all_configs.items() if k == spreadsheet_id}
    return all_configs

def _parse_player_names(text: str | None) -> list[str]:
    if not text:
        return []
    return list(dict.fromkeys(n.strip() for n in re.split(r"[,、，]", text) if n.strip()))

@bot.tree.command(name="minutelog_targets", description="PtLogs1min に記録するユーザーを順位ではなくユーザー名で指定します（100位以内）。")
@app_commands.describe(
    players="ユーザー名をカンマ区切りで指定（例: 名前A,名前B）。省略すると1〜5位の記録に戻します。",
    spreadsheet_id="対象のスプレッドシートID（省略すると全て）",
)
async def minutelog_targets(interaction: discord.Interaction, players: str = None, spreadsheet_id: str = None):
    guild_id = interaction.guild_id or 0
    targets = _target_spreadsheets(guild_id, spreadsheet_id)
    if not targets:
        await interaction.response.send_message("設定がありません。先に `/setup` してください。", ephemeral=True)
        return
    names = _parse_player_names(players)
    await interaction.response.defer(thinking=True)
    for ss_id in targets:
        storage.set_minute_log_players(guild_id, ss_id, names)

    results = []
    for i, (ss_id, cfg) in enumerate(targets.items()):
        try:
            # 設定をすぐ反映するため、1件目は最新を取得し直す(2件目以降は同じ取得結果を使う)
            msg = await minute_logger.run_once(guild_id, ss_id, cfg, force_fetch=(i == 0))
        except Exception as e:
            msg = f"更新失敗: {type(e).__name__}: {e}"
        results.append(f"`{ss_id}`: {msg}")

    if names:
        ranks = {e["name"]: e["rank"] for e in reversed(minute_logger.cached_leaderboard())}
        lines = [f"- {n}: {ranks[n]}位" if n in ranks else f"- {n}: ⚠️ 100位以内に見つかりません" for n in names]
        head = "📈 PtLogs1min の記録対象をユーザー名指定にしました。\n" + "\n".join(lines)
    else:
        head = "📈 PtLogs1min の記録対象を 1〜5位 に戻しました。"
    await interaction.followup.send(head + "\n" + "\n".join(results))

if __name__ == "__main__":
    bot.run(TOKEN, log_handler=None)