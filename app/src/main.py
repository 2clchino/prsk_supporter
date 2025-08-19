import os
import logging
import discord
from discord.ext import commands
from dotenv import load_dotenv

load_dotenv(override=False)
TOKEN = os.environ["DISCORD_TOKEN"]
GUILD_ID = os.environ.get("GUILD_ID")

intents = discord.Intents.default()
if os.environ.get("ENABLE_MESSAGE_CONTENT", "0") == "1":
    intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bot")

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
    await interaction.response.send_message("Pong!")

@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return
    if intents.message_content and message.content.strip().lower() == "ping":
        await message.channel.send("Pong!")
    await bot.process_commands(message)

if __name__ == "__main__":
    bot.run(TOKEN, log_handler=None)