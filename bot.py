import discord
import random
import os
import pytz

'''
BTW, "DHL" is your greatest creation after "VNIZ"
'''
import aiohttp
import io
from discord.ext import commands, tasks
from dotenv import load_dotenv
from datetime import datetime

'''
Am happy 2 have deal with U
'''
load_dotenv()

TOKEN = os.getenv('DISCORD_TOKEN')
CHANNEL_ID = int(os.getenv('DISCORD_CHANNEL_ID'))

intents = discord.Intents.default()
intents.messages = True
intents.message_content = True

bot = commands.Bot(command_prefix='!', intents=intents)


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
    try:
        async for message in channel.history(limit=None):
            if not message.author.bot:
                message_date = message.created_at

                for attachment in message.attachments:
                    if attachment.content_type and attachment.content_type.startswith('audio'):
                        bot.audio_files.append(attachment.url)
                        bot.message_dates[attachment.url] = message_date
                        print(f'Added audio attachment: {attachment.url}')  # Debugging log

                for word in message.content.split():
                    if word.startswith('http://') or word.startswith('https://'):
                        bot.links.append(word)
                        bot.message_dates[word] = message_date
                        print(f'Added link: {word}')  # Debugging log

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
    periodic_update.start()


@bot.command(name='run')
async def run(ctx, date: str = None):
    """Command to select and send a random file or link starting from the specified date."""
    if not bot.audio_files and not bot.links:
        await ctx.send("Аудиофайлы и ссылки пока не загружены.")
        return

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

        if filtered_audio_files and (not filtered_links or random.choice([True, False])):
            response = random.choice(filtered_audio_files)
            try:
                file_data = await download_file(response)
                filename = response.split('/')[-1].split('?')[0]
                sender_name = ctx.author.name
                await ctx.send(f"{sender_name} отправил файл:", file=discord.File(fp=io.BytesIO(file_data), filename=filename))
            except Exception as e:
                await ctx.send(f"Error fetching the audio file: {e}")
        else:
            link = random.choice(filtered_links)
            sender_name = ctx.author.name
            await ctx.send(f"{sender_name} отправил ссылку: {link}")

    except ValueError:
        await ctx.send("Укажите действительную дату в формате: DD.MM.YYYY.")

bot.run(TOKEN)
