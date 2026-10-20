"""How long does each streaming STT plugin take to notice a dead websocket?

A local server accepts the websocket upgrade, then never sends anything and never
answers pings -- a provider, or a NAT in between, that vanished without FIN/RST.
Each plugin is handed silence and we time how long it takes to raise.

Extends the four-plugin probe in livekit/agents#7900 to every streaming STT
plugin in the tree. No API keys and no network: everything talks to 127.0.0.1.

    uv run python halfopen_probe.py            # all plugins
    uv run python halfopen_probe.py deepgram   # just some
"""

from __future__ import annotations

import asyncio
import json
import sys
import time
import traceback

import aiohttp
from aiohttp import web

from livekit import rtc
from livekit.agents import APIConnectOptions

WATCH = 75.0  # give up after this; the fixed plugins answer in ~45s
PORT = 8765
HTTP = f"http://127.0.0.1:{PORT}"
SAMPLE_RATE = 16000
FRAME_MS = 20


async def silent_ws(request: web.Request) -> web.WebSocketResponse:
    """Accept the upgrade, then answer nothing at all -- pings included."""
    ws = web.WebSocketResponse(autoping=False)
    await ws.prepare(request)
    await asyncio.sleep(3600)
    return ws


async def session_post(request: web.Request) -> web.Response:
    """Some plugins POST for a session before connecting; hand back a local ws url."""
    return web.json_response(
        {
            "id": "probe",
            "url": f"ws://127.0.0.1:{PORT}/ws",
            "websocket_url": f"ws://127.0.0.1:{PORT}/ws",
            "session_id": "probe",
            "token": "probe",
        }
    )


# Each entry builds an STT pointed at the local server. A plugin with no way to
# redirect its endpoint is left out and reported as unmeasured rather than guessed.
def build(name: str, http: aiohttp.ClientSession):
    if name == "deepgram":
        from livekit.plugins import deepgram

        return deepgram.STT(api_key="x", base_url=f"{HTTP}/v1/listen", http_session=http)
    if name == "soniox":
        from livekit.plugins import soniox

        return soniox.STT(api_key="x", base_url=f"{HTTP}/ws", http_session=http)
    if name == "elevenlabs":
        from livekit.plugins import elevenlabs

        return elevenlabs.STT(api_key="x", base_url=f"{HTTP}/v1", http_session=http)
    if name == "openai":
        from livekit.plugins import openai

        inst = openai.STT(api_key="x", base_url=f"{HTTP}/v1", use_realtime=True)
        inst._session = http  # it otherwise reaches for the job's http context
        return inst
    if name == "inworld":
        from livekit.plugins import inworld

        return inworld.STT(api_key="x", base_url=f"{HTTP}", http_session=http)
    if name == "assemblyai":
        from livekit.plugins import assemblyai

        return assemblyai.STT(api_key="x", base_url=f"{HTTP}/v3/ws", http_session=http)
    if name == "telnyx":
        from livekit.plugins import telnyx

        return telnyx.STT(api_key="x", base_url=f"{HTTP}/ws", http_session=http)
    if name == "gladia":
        from livekit.plugins import gladia

        return gladia.STT(api_key="x", base_url=f"{HTTP}", http_session=http)
    if name == "sarvam":
        from livekit.plugins import sarvam

        return sarvam.STT(api_key="x", base_url=f"{HTTP}", http_session=http)
    if name == "smallestai":
        from livekit.plugins import smallestai

        return smallestai.STT(api_key="x", base_url=f"{HTTP}/ws", http_session=http)
    if name == "simplismart":
        from livekit.plugins import simplismart

        return simplismart.STT(api_key="x", base_url=f"{HTTP}", http_session=http)
    if name == "fireworksai":
        from livekit.plugins import fireworksai

        return fireworksai.STT(api_key="x", base_url=f"{HTTP}/v1", http_session=http)
    if name == "meta":
        raise LookupError("rejects a ws:// url; the constructor requires absolute wss://")
    if name == "baseten":
        from livekit.plugins import baseten

        return baseten.STT(api_key="x", model_endpoint=f"ws://127.0.0.1:{PORT}/v1/bridges/unmute/stt/deepgram/nova:3", http_session=http)
    if name == "gradium":
        from livekit.plugins import gradium

        return gradium.STT(
            api_key="x", model_endpoint=f"ws://127.0.0.1:{PORT}/ws", http_session=http
        )
    if name == "slng":
        from livekit.plugins import slng

        from livekit.plugins.slng.stt import STTConnectionConfig

        return slng.STT(
            api_key="x",
            connections=[
                STTConnectionConfig(model="deepgram/nova:3", endpoint=f"ws://127.0.0.1:{PORT}/ws")
            ],
        )
    raise LookupError(f"no local-endpoint override known for {name}")


ALL = [
    "assemblyai", "baseten", "deepgram", "elevenlabs", "fireworksai", "gladia", "gradium",
    "inworld", "meta", "openai", "sarvam", "simplismart", "slng", "smallestai", "soniox",
    "telnyx",
]
# microsoft-ai, nabrah and xai expose no endpoint override, so they cannot be
# pointed at a local server from the public constructor.
NO_OVERRIDE = ["microsoft-ai", "nabrah", "xai"]


def _frame() -> rtc.AudioFrame:
    samples = SAMPLE_RATE * FRAME_MS // 1000
    return rtc.AudioFrame(
        data=b"\x00\x00" * samples,
        sample_rate=SAMPLE_RATE,
        num_channels=1,
        samples_per_channel=samples,
    )


async def probe(name: str, http: aiohttp.ClientSession) -> dict:
    started = time.monotonic()
    try:
        stt = build(name, http)
    except LookupError as exc:
        return {"plugin": name, "result": "unmeasured", "detail": str(exc)}
    except Exception as exc:
        return {"plugin": name, "result": "unmeasured", "detail": f"construct: {exc!r}"}

    try:
        stream = stt.stream(conn_options=APIConnectOptions(max_retry=0, timeout=10))
    except Exception as exc:
        return {"plugin": name, "result": "unmeasured", "detail": f"stream(): {exc!r}"}

    async def feed() -> None:
        while True:
            stream.push_frame(_frame())
            await asyncio.sleep(FRAME_MS / 1000)

    feeder = asyncio.create_task(feed())

    async def drain() -> None:
        async for _ in stream:
            pass

    try:
        await asyncio.wait_for(drain(), timeout=WATCH)
        return {
            "plugin": name,
            "result": "ended_without_error",
            "seconds": round(time.monotonic() - started, 1),
        }
    except asyncio.TimeoutError:
        return {"plugin": name, "result": "never_noticed", "seconds": WATCH}
    except Exception as exc:
        return {
            "plugin": name,
            "result": "raised",
            "seconds": round(time.monotonic() - started, 1),
            "detail": f"{type(exc).__name__}: {str(exc)[:90]}",
        }
    finally:
        feeder.cancel()
        with __import__("contextlib").suppress(BaseException):
            await feeder
        with __import__("contextlib").suppress(BaseException):
            await stream.aclose()


async def main() -> None:
    wanted = sys.argv[1:] or ALL

    app = web.Application()
    app.router.add_get("/ws", silent_ws)
    app.router.add_get("/{tail:.*}", silent_ws)
    app.router.add_post("/{tail:.*}", session_post)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", PORT)
    await site.start()

    results = []
    async with aiohttp.ClientSession() as http:
        # one at a time: a shared event loop makes concurrent timings unreliable
        for name in wanted:
            r = await probe(name, http)
            results.append(r)
            print(
                f"  {r['plugin']:14} {r['result']:20} "
                f"{str(r.get('seconds', '')):>6}  {r.get('detail', '')[:70]}",
                flush=True,
            )

    for name in NO_OVERRIDE:
        if not wanted or name in ALL:
            results.append(
                {"plugin": name, "result": "unmeasured", "detail": "no endpoint override in the public constructor"}
            )

    await runner.cleanup()
    print("\n" + json.dumps(results, indent=2))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception:
        traceback.print_exc()
