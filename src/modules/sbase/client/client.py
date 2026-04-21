import asyncio
import json
import os
import socket
import uuid
from pathlib import Path

import httpx
import psutil
from dotenv import load_dotenv
from supabase import acreate_client

load_dotenv(".env.client")

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_ANON_KEY = os.environ["SUPABASE_ANON_KEY"]
REGISTER_DEVICE_FUNCTION_URL = os.environ["REGISTER_DEVICE_FUNCTION_URL"]

STATE_DIR = Path(os.environ.get("STATE_DIR", "."))
STATE_DIR.mkdir(parents=True, exist_ok=True)

INSTALL_ID_FILE = STATE_DIR / "install_id.txt"
SESSION_FILE = STATE_DIR / "supabase_session.json"
DISPLAY_NAME = os.environ.get("DISPLAY_NAME", socket.gethostname())


def get_install_id() -> str:
    if INSTALL_ID_FILE.exists():
        return INSTALL_ID_FILE.read_text(encoding="utf-8").strip()

    install_id = str(uuid.uuid4())
    INSTALL_ID_FILE.write_text(install_id, encoding="utf-8")
    return install_id


def load_saved_session() -> dict | None:
    if not SESSION_FILE.exists():
        return None

    try:
        return json.loads(SESSION_FILE.read_text(encoding="utf-8"))
    except Exception:
        return None


def save_session(access_token: str, refresh_token: str) -> None:
    SESSION_FILE.write_text(
        json.dumps(
            {
                "access_token": access_token,
                "refresh_token": refresh_token,
            },
            indent=2,
        ),
        encoding="utf-8",
    )


async def authenticate(supabase):
    saved = load_saved_session()

    if saved and saved.get("access_token") and saved.get("refresh_token"):
        try:
            auth = await supabase.auth.set_session(
                saved["access_token"],
                saved["refresh_token"],
            )
            if auth.user and auth.session:
                save_session(auth.session.access_token, auth.session.refresh_token)
                print(f"[client] restored session for {auth.user.id}")
                return auth
        except Exception:
            pass

    auth = await supabase.auth.sign_in_anonymously({})
    if auth.user is None or auth.session is None:
        raise RuntimeError("Anonymous sign-in failed")

    save_session(auth.session.access_token, auth.session.refresh_token)
    print(f"[client] created anonymous user {auth.user.id}")
    return auth


async def register_device(install_id: str, auth_user_id: str) -> dict:
    async with httpx.AsyncClient(timeout=20) as client:
        response = await client.post(
            REGISTER_DEVICE_FUNCTION_URL,
            json={
                "install_id": install_id,
                "display_name": DISPLAY_NAME,
                "auth_user_id": auth_user_id,
            },
        )
        response.raise_for_status()
        return response.json()


async def main() -> None:
    install_id = get_install_id()
    supabase = await acreate_client(SUPABASE_URL, SUPABASE_ANON_KEY)

    auth = await authenticate(supabase)
    await supabase.realtime.set_auth(auth.session.access_token)

    registration = await register_device(install_id, auth.user.id)

    device_id = registration["device_id"]
    slot_number = registration["slot_number"]
    device_topic = registration["device_topic"]
    response_topic = registration["response_topic"]

    response_channel = supabase.channel(
        response_topic,
        {"config": {"private": True}},
    )
    await response_channel.subscribe()

    async def handle_command(payload: dict) -> None:
        data = payload.get("payload", {})
        cmd = data.get("cmd")
        request_id = data.get("request_id")

        if cmd == "cpu":
            cpu_percent = psutil.cpu_percent(interval=1.0)
            await response_channel.send_broadcast(
                "cpu_result",
                {
                    "request_id": request_id,
                    "device_id": device_id,
                    "slot_number": slot_number,
                    "display_name": DISPLAY_NAME,
                    "cpu_percent": cpu_percent,
                },
            )
            return

        await response_channel.send_broadcast(
            "command_error",
            {
                "request_id": request_id,
                "device_id": device_id,
                "slot_number": slot_number,
                "display_name": DISPLAY_NAME,
                "error": f"Unsupported command: {cmd}",
            },
        )

    command_channel = supabase.channel(
        device_topic,
        {"config": {"private": True}},
    )

    def on_command(payload: dict):
        asyncio.create_task(handle_command(payload))

    command_channel.on_broadcast(
        event="command",
        callback=on_command,
    )
    await command_channel.subscribe()

    try:
        while True:
            try:
                await supabase.rpc(
                    "touch_device_last_seen",
                    {"p_device_id": device_id},
                ).execute()
            except Exception as exc:
                print(f"[client] heartbeat failed: {exc}")

            await asyncio.sleep(30)
    finally:
        try:
            await supabase.remove_channel(command_channel)
        except Exception:
            pass

        try:
            await supabase.remove_channel(response_channel)
        except Exception:
            pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("[client] stopped")