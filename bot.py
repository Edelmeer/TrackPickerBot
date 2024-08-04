import discord
import random
import os
import pytz
import aiohttp
import io
import asyncpg
from discord.ext import commands, tasks
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

TOKEN = os.getenv('DISCORD_TOKEN')
CHANNEL_ID = int(os.getenv('DISCORD_CHANNEL_ID'))
DEST_CHANNEL_ID = int(os.getenv('DISCORD_DEST_CHANNEL_ID'))

DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

intents = discord.Intents.default()
intents.messages = True
intents.message_content = True

bot = commands.Bot(command_prefix='!', intents=intents)

async def get_db_pool():
    return await asyncpg.create_pool(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

async def track_is_issued(url, pool):
    async with pool.acquire() as conn:
        result = await conn.fetchrow("SELECT 1 FROM issued_tracks WHERE url = $1", url)
        return result is not None

async def mark_as_issued(url, pool):
    async with pool.acquire() as conn:
        await conn.execute("INSERT INTO issued_tracks (url, issued_at) VALUES ($1, NOW())", url)

async def clear_issued_tracks(pool):
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM issued_tracks")

async def download_file(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                return await response.read()
            else:
                raise Exception(f'Failed to download file: {response.status}')

async def update_audio_files():
    channel = bot.get_channel(CHANNEL_ID)
    if channel is None:
        print(f'Channel with ID {CHANNEL_ID} not found')
        return

    permissions = channel.permissions_for(channel.guild.me)
    if not permissions.read_message_history:
        print(f'Bot does not have permission to read message history in channel {CHANNEL_ID}')
        return

    bot.audio_files = []
    bot.links = []
    bot.message_dates = {}
    bot.message_authors = {}
    try:
        async for message in channel.history(limit=None):
            if not message.author.bot:
                message_date = message.created_at

                for attachment in message.attachments:
                    if attachment.content_type and attachment.content_type.startswith('audio'):
                        bot.audio_files.append(attachment.url)
                        bot.message_dates[attachment.url] = message_date
                        bot.message_authors[attachment.url] = message.author.name
                        print(f'Added audio attachment: {attachment.url}')

                for word in message.content.split():
                    if word.startswith('http://') or word.startswith('https://'):
                        bot.links.append(word)
                        bot.message_dates[word] = message_date
                        bot.message_authors[word] = message.author.name
                        print(f'Added link: {word}')

        print(f'Loaded {len(bot.audio_files)} audio files and {len(bot.links)} links.')
    except Exception as e:
        print(f'Error loading messages: {e}')

@tasks.loop(hours=1)
async def periodic_update():
    await update_audio_files()

@bot.event
async def on_ready():
    print(f'We have logged in as {bot.user}')
    await update_audio_files()
    bot.pool = await get_db_pool()
    periodic_update.start()

@bot.command(name='run')
async def run(ctx, date: str = None):
    """Command to select and send a random file or link starting from the specified date."""
    if not bot.audio_files and not bot.links:
        await ctx.send("Аудиофайлы и ссылки пока не загружены.")
        return

    # Get database connection pool
    pool = bot.pool

    try:
        if date:
            start_date = datetime.strptime(date, '%d.%m.%Y')
            start_date = start_date.replace(tzinfo=pytz.utc)
        else:
            start_date = datetime.min.replace(tzinfo=pytz.utc)

        filtered_audio_files = [url for url in bot.audio_files if bot.message_dates[url] >= start_date]
        filtered_links = [url for url in bot.links if bot.message_dates[url] >= start_date]

        if not filtered_audio_files and not filtered_links:
            await ctx.send("На указанную дату нет доступных аудиофайлов или ссылок.")
            return

        # Fetch all issued tracks
        async with pool.acquire() as conn:
            issued_audio_files = await conn.fetch("SELECT url FROM issued_tracks WHERE url = ANY($1::text[])",
                                                  filtered_audio_files)
            issued_links = await conn.fetch("SELECT url FROM issued_tracks WHERE url = ANY($1::text[])",
                                            filtered_links)

        issued_audio_files = set(record['url'] for record in issued_audio_files)
        issued_links = set(record['url'] for record in issued_links)

        not_issued_audio_files = [url for url in filtered_audio_files if url not in issued_audio_files]
        not_issued_links = [url for url in filtered_links if url not in issued_links]

        if not not_issued_audio_files and not not_issued_links:
            await ctx.send("Все треки и ссылки уже были выданы.")
            return

        if not_issued_audio_files and (not not_issued_links or random.choice([True, False])):
            response = random.choice(not_issued_audio_files)
            try:
                file_data = await download_file(response)
                filename = response.split('/')[-1].split('?')[0]
                sender_name = bot.message_authors[response]
                bot.last_message = await ctx.send(
                    f"{sender_name} отправил файл:",
                    file=discord.File(fp=io.BytesIO(file_data), filename=filename)
                )
                await mark_as_issued(response, pool)
            except Exception as e:
                await ctx.send(f"Error fetching the audio file: {e}")
        else:
            link = random.choice(not_issued_links)
            sender_name = bot.message_authors[link]
            bot.last_message = await ctx.send(f"{sender_name} отправил ссылку: {link}")
            await mark_as_issued(link, pool)

    except ValueError:
        await ctx.send("Укажите действительную дату в формате: DD.MM.YYYY.")

@bot.command(name='cool')
async def cool(ctx):
    """Command to forward the last sent message to another channel."""
    if not hasattr(bot, 'last_message') or bot.last_message is None:
        await ctx.send("Нет сообщения для пересылки.")
        return

    dest_channel = bot.get_channel(DEST_CHANNEL_ID)
    if dest_channel is None:
        await ctx.send("Целевой канал не найден.")
        return

    try:
        if bot.last_message.attachments:
            attachment = bot.last_message.attachments[0]
            await dest_channel.send(
                content=bot.last_message.content,
                file=await attachment.to_file()
            )
        else:
            await dest_channel.send(content=bot.last_message.content)
        await ctx.send("Сообщение успешно переслано.")
    except Exception as e:
        await ctx.send(f"Ошибка при пересылке сообщения: {e}")

@bot.command(name='clear_tracks')
@commands.has_permissions(administrator=True)
async def clear_tracks(ctx):
    """Command to clear all issued tracks from the database."""
    try:
        # Get database connection pool
        pool = bot.pool
        await clear_issued_tracks(pool)
        await ctx.send("Все выданные треки и ссылки были очищены.")
    except Exception as e:
        await ctx.send(f"Ошибка при очистке таблицы: {e}")

@bot.event
async def on_close():
    if hasattr(bot, 'pool'):
        await bot.pool.close()

bot.run(TOKEN)
