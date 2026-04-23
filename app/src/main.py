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

    msg = f"**ChangeNotice** — {cfg.get('EventName')}\n" + "\n".join(lines)
    await channel.send(msg)
    return "ChangeNotice"

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

    msg = f"**NextServer** — {cfg.get('EventName')}\n" + "\n".join(lines)
    await channel.send(msg)
    return "NextServer"

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

@registry.every_hour_at_config("LogMinutes")
async def ranking_logger(ctx: dict) -> str:
    from requests.exceptions import ReadTimeout, Timeout, ConnectionError as ReqConnError, RequestException
    cfg = ctx["config"]
    async def _run_once():
        if cfg.get("isWorldBloom"):
            times = sekai_api.get_chapter_time(cfg["EventID"], cfg["CharaID"])
        else:
            times = sekai_api.get_event_time(cfg["EventID"])

        if times:
            last_time = times[-1]
            if cfg.get("isWorldBloom"):
                raw = sekai_api.get_chapter_rankings(cfg["EventID"], cfg["CharaID"], last_time)
            else:
                raw = sekai_api.get_event_rankings(cfg["EventID"], last_time)
            used_fallback = False
        else:
            raw = await asyncio.to_thread(sekai_api._get_leaderboard_sekai_run)
            if not raw:
                raise RuntimeError("API unavailable and fallback also failed")
            last_time = now_jst().strftime("%Y-%m-%dT%H:%M:%S%z")
            used_fallback = True

        rankings = sekai_api.extract_scores(raw, cfg.get("Trackings"))
        ptlogger.write_values(cfg["SpreadsheetID"], last_time, rankings)
        return "api checked (fallback)" if used_fallback else "api checked"

    return await retry_async(
        _run_once,
        attempts=3,
        catch=(ReadTimeout, Timeout, ReqConnError, RequestException, RuntimeError, IndexError),
    )

scheduler = EventScheduler(bot, registry)

@bot.event
async def on_ready():
    print(f"Logged in as {bot.user} (id={bot.user.id})")
    saved = storage.load_all_configs()
    for guild_id, cfg in saved.items():
        try:
            scheduler_running = scheduler.is_running(guild_id)
            await scheduler.start_or_restart(guild_id, cfg)
        except Exception as e:
            print(f"[WARN] restore failed for guild {guild_id}: {e}")

@bot.tree.command(name="ping", description="Ping-Pong!")
async def ping(interaction: discord.Interaction):
    await interaction.response.send_message("PongPong!")

@bot.tree.command(name="echo", description="そのまま返す")
@app_commands.describe(text="返してほしいテキスト")
async def echo(interaction: discord.Interaction, text: str):
    await interaction.response.send_message(text, ephemeral=True)
    
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
            config["CharaID"], start, end = sekai_api.filter_chapter_info(
                sekai_api.get_chapter_info(event_id, chapter_no)
            )
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
            config["CharaID"], start, end = sekai_api.filter_chapter_info(
                sekai_api.get_chapter_info(event_id, chapter_no)
            )
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

    guild_id = interaction.guild_id or 0
    storage.save_guild_config(guild_id, config)
    await scheduler.start_or_restart(guild_id, config)
    runners = config.get("Runners")
    ptlogger.format_pt_table(text, start, end, config.get("Trackings"), interval_minutes=log_interval)
    runners_str = ", ".join(runners) if isinstance(runners, list) else (str(runners) if runners is not None else "未設定")
    is_wb = config.get("isWorldBloom")
    event_name_for_msg = config.get("EventName") or f"(ID: {event_id})"
    change_min = int(config.get("ChangeNotice") or 0)
    log_minutes_str = ", ".join(f"{m:02d}" for m in config["LogMinutes"])
    message = (
        "設定を保存し、定期実行を登録しました。\n"
        f"- イベント名: {event_name_for_msg}\n"
        f"- 種別: {'イベントの１チャプター' if is_wb else '通常イベント'}\n"
        f"- ランナー: {runners_str}\n"
        f"- 開始: {config['EventStart']}\n"
        f"- 終了: {config['EventEnd']}\n"
        f"- シフト通知: 毎時 {change_min:02d} 分\n"
        f"- ログ記録: {log_interval}分間隔（毎時 {log_minutes_str} 分）\n"
        f"- 投稿チャンネル: <#{config['ChannelID']}>"
    )
    await interaction.followup.send(message, ephemeral=True)

@bot.tree.command(name="clear_setup", description="保存済み設定を削除します（実行も停止）")
async def clear_setup(interaction: discord.Interaction):
    guild_id = interaction.guild_id or 0
    scheduler.stop(guild_id)
    storage.delete_guild_config(guild_id)
    await interaction.response.send_message("設定を削除しました。", ephemeral=True)

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

if __name__ == "__main__":
    bot.run(TOKEN, log_handler=None)