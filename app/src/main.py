import os
import logging
import traceback
import discord
from discord.ext import commands
from discord import app_commands
from dotenv import load_dotenv
import gspread_manager
import sekai_api
import shift_manager
import ptlogger

load_dotenv(override=False)
TOKEN = os.environ["DISCORD_TOKEN"]
GUILD_ID = os.environ.get("GUILD_ID")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bot")

intents = discord.Intents.default()
if os.environ.get("ENABLE_MESSAGE_CONTENT", "0") == "1":
    intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)

@bot.event
async def on_ready():
    logger.info(f"Logged in as {bot.user} (ID: {bot.user.id})")
    if GUILD_ID:
        guild = discord.Object(id=int(GUILD_ID))
        bot.tree.copy_global_to(guild=guild)
        synced = await bot.tree.sync(guild=guild)
        logger.info(f"Synced {len(synced)} commands to guild {GUILD_ID}")
    else:
        synced = await bot.tree.sync()
        logger.info(f"Synced {len(synced)} global commands")

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
    config = gspread_manager.read_config_values(text)
    if (config.get("ChapterNo") > 0):
        event_id, _, _ = sekai_api.filter_event_info(sekai_api.get_event_info_by_name(config.get("EventName")))
        config["CharaID"], start, end = sekai_api.filter_chapter_info(sekai_api.get_chapter_info(event_id, config.get("ChapterNo")))
        config["isWorldBloom"] = True
    else:
        event_id, start, end = sekai_api.filter_event_info(sekai_api.get_event_info_by_name(config.get("EventName")))
        config["isWorldBloom"] = False
    config["EventStart"] = start
    config["EventEnd"] = end
    runners = config.get("Runners")
    if isinstance(runners, list):
        runners_str = ", ".join(runners)
    else:
        runners_str = str(runners) if runners is not None else "未設定"
    message = (
        "設定を読み込みました。\n"
        f"- イベント名は {config.get('EventName')}\n"
        f"- これは {'イベントの１チャプター' if config.get('isWorldBloom') else '通常イベント'}\n"
        f"- ランナーは {runners_str}\n"
        f"- イベントの開始日は {config.get('EventStart')}\n"
        f"- 終了日は {config.get('EventEnd')}"
    )
    await interaction.response.send_message(message, ephemeral=True)

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

@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return
    if intents.message_content and message.content.strip().lower() == "ping":
        await message.channel.send("Pong!")
    await bot.process_commands(message)

if __name__ == "__main__":
    bot.run(TOKEN, log_handler=None)