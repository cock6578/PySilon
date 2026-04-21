import asyncio
import os
import uuid
from typing import Dict

import discord
from discord.ext import commands
from dotenv import load_dotenv
from supabase import acreate_client

load_dotenv(".env.bot")

DISCORD_TOKEN = os.environ["DISCORD_TOKEN"]
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_ANON_KEY = os.environ["SUPABASE_ANON_KEY"]
BOT_SUPABASE_EMAIL = os.environ["BOT_SUPABASE_EMAIL"]
BOT_SUPABASE_PASSWORD = os.environ["BOT_SUPABASE_PASSWORD"]

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix=".", intents=intents)

supabase = None
response_channel = None
pending_requests: Dict[str, int] = {}


async def get_device_by_slot(slot_number: int) -> dict | None:
    result = await (
        supabase.table("device_slots")
        .select("slot_number, device_id, devices!inner(status, display_name)")
        .eq("slot_number", slot_number)
        .execute()
    )

    if not result.data:
        return None

    row = result.data[0]
    if row["devices"]["status"] != "active":
        return None

    return row


async def get_discord_channel(channel_id: int):
    channel = bot.get_channel(channel_id)
    if channel is not None:
        return channel

    try:
        return await bot.fetch_channel(channel_id)
    except Exception:
        return None


async def handle_cpu_result(payload: dict) -> None:
    data = payload.get("payload", {})
    request_id = data.get("request_id")
    channel_id = pending_requests.pop(request_id, None)

    if channel_id is None:
        return

    channel = await get_discord_channel(channel_id)
    if channel is None:
        return

    slot_number = data.get("slot_number")
    cpu_percent = data.get("cpu_percent")
    display_name = data.get("display_name") or "Unknown device"

    await channel.send(f"Client {slot_number} ({display_name}): {cpu_percent}%")


async def handle_command_error(payload: dict) -> None:
    data = payload.get("payload", {})
    request_id = data.get("request_id")
    channel_id = pending_requests.pop(request_id, None)

    if channel_id is None:
        return

    channel = await get_discord_channel(channel_id)
    if channel is None:
        return

    slot_number = data.get("slot_number")
    error = data.get("error") or "Unknown error"

    await channel.send(f"Client {slot_number} error: {error}")


async def setup_supabase() -> None:
    global supabase, response_channel

    if supabase is not None:
        return

    supabase = await acreate_client(SUPABASE_URL, SUPABASE_ANON_KEY)

    auth = await supabase.auth.sign_in_with_password(
        {
            "email": BOT_SUPABASE_EMAIL,
            "password": BOT_SUPABASE_PASSWORD,
        }
    )

    if auth.user is None or auth.session is None:
        raise RuntimeError("Bot sign-in failed")

    await supabase.realtime.set_auth(auth.session.access_token)

    response_channel = supabase.channel(
        "bot-responses",
        {"config": {"private": True}},
    )

    def on_cpu_result(payload: dict):
        asyncio.create_task(handle_cpu_result(payload))

    def on_command_error(payload: dict):
        asyncio.create_task(handle_command_error(payload))

    response_channel.on_broadcast(
        event="cpu_result",
        callback=on_cpu_result,
    )
    response_channel.on_broadcast(
        event="command_error",
        callback=on_command_error,
    )

    await response_channel.subscribe()


@bot.event
async def on_ready():
    await setup_supabase()
    print(f"Logged in as {bot.user}")


@bot.command()
async def cpu(ctx: commands.Context, slot_number: int):
    await setup_supabase()

    device = await get_device_by_slot(slot_number)
    if device is None:
        await ctx.send(f"Slot {slot_number} not found or not active.")
        return

    device_id = device["device_id"]
    request_id = str(uuid.uuid4())
    pending_requests[request_id] = ctx.channel.id

    command_channel = supabase.channel(
        f"device:{device_id}",
        {"config": {"private": True}},
    )

    try:
        await command_channel.subscribe()
        await command_channel.send_broadcast(
            "command",
            {
                "cmd": "cpu",
                "request_id": request_id,
            },
        )
    finally:
        try:
            await supabase.remove_channel(command_channel)
        except Exception:
            pass

    await ctx.send(f"Sent cpu command to slot {slot_number} ...")


if __name__ == "__main__":
    bot.run(DISCORD_TOKEN)