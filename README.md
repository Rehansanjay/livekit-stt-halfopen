# How long does a LiveKit STT plugin take to notice a dead websocket?

If an STT websocket goes half-open — the provider, or a NAT in between, disappears without a FIN or an RST — nothing arrives and nothing fails. The read side parks on `ws.receive()` forever. The stream does not reconnect, `stt.FallbackAdapter` does not switch provider, and on a call the agent simply stops hearing the caller. No error is raised anywhere.

[livekit/agents#7900](https://github.com/livekit/agents/issues/7900) measured four plugins. This measures all nineteen streaming STT plugins in the tree.

A local server accepts the websocket upgrade and then sends nothing and answers no pings. Each plugin is handed silence and timed until it raises. No API keys and no network: everything talks to `127.0.0.1`.

Measured against `livekit/agents` at **`d4cc4a941`**, 20 October 2026, with a 75-second watch.

## Results

**Fails over** — raises a retryable error the base stream can act on:

| plugin | time to raise |
| --- | --- |
| smallestai | 46.4s |
| telnyx | 46.9s |
| deepgram | 47.0s |

**Detects it, then tells no one** — the heartbeat fires and nothing propagates:

| plugin | detects at | what happens next |
| --- | --- | --- |
| soniox | ~46s | logs the error frame, requests a reconnect, is connected again 3 ms later, repeats indefinitely |
| simplismart | ~50.6s | logs `connection lost`, never raises |

**Never notices** — still waiting when the watch expires at 75s:

| plugin | |
| --- | --- |
| assemblyai | open PR [#7547](https://github.com/livekit/agents/pull/7547) |
| baseten | 5s `wait_for` on receive, but the handler `continue`s |
| elevenlabs | open PR [#7913](https://github.com/livekit/agents/pull/7913) |
| fireworksai | 5s `wait_for` on receive, but the handler `continue`s |
| gladia | the streaming connect; its batch connect *is* bounded, by `ClientWSTimeout` |
| gradium | 5s `wait_for` on receive, but the handler `continue`s |
| inworld | open PR [#7913](https://github.com/livekit/agents/pull/7913) |
| openai | realtime STT; open PR [#7913](https://github.com/livekit/agents/pull/7913) |

Three measured, two half-measured, eight silent. Six could not be measured at all — see below.

### The mechanism lines up exactly

Every plugin that raised passes `heartbeat=` to `ws_connect`. Every plugin that never noticed does not. Both plugins that detect-and-stay-silent pass one. Nothing in the table is unexplained by that single parameter, which is `None` by default in aiohttp:

```python
# plugins that fail over
ws_connect(url, headers=headers, heartbeat=30.0)
```

An unanswered ping makes aiohttp close the socket, which arrives in the receive loop as `WSMsgType.ERROR`. Detection is necessary but not sufficient: the ERROR frame then has to be raised rather than logged, which is what separates the first group from the second.

### The five-second timeout that isn't one

baseten, fireworksai and gradium all do this:

```python
try:
    msg = await asyncio.wait_for(ws.receive(), timeout=5)
except asyncio.TimeoutError:
    if closing_ws:
        break
    continue      # round again, forever
```

That reads like a liveness bound and is a **polling** timeout. It fires every five seconds and the loop carries on, so a dead socket is never escalated. All three measured 75 seconds. I had originally written these three off as already safe on the strength of the `wait_for` alone, and the probe is what caught it — see [#7943](https://github.com/livekit/agents/pull/7943).

### Before and after

The three above, with `heartbeat=30.0` and a raising ERROR branch added ([#7943](https://github.com/livekit/agents/pull/7943)):

| plugin | before | after |
| --- | --- | --- |
| baseten | never (75s) | **48.1s** |
| fireworksai | never (75s) | **47.0s** |
| gradium | never (75s) | **47.0s** |

## Not measured, and why

Reported rather than guessed:

| plugin | reason | heartbeat on main |
| --- | --- | --- |
| meta | the constructor requires an absolute `wss://` url, so it cannot be pointed at a local plaintext server | yes |
| microsoft-ai | no endpoint override in the public constructor | yes |
| nabrah | no endpoint override in the public constructor | no |
| sarvam | the local session handshake answers 403 | mixed — `stt_streaming.py` passes one, `stt.py` does not |
| slng | endpoint validation rejected every local shape tried | no |
| xai | no endpoint override in the public constructor | yes |

The heartbeat column there is read from the source with a paren-balanced parse of each `ws_connect` call, not a text search — an earlier text-window version of this audit got three plugins wrong, including reporting a plugin as covered when the match was in its batch code path.

## Running it

```bash
# from a livekit/agents checkout, with the workspace installed
uv run python probe/halfopen_probe.py              # every plugin
uv run python probe/halfopen_probe.py deepgram     # just some
```

The probe needs a checkout of `livekit/agents` with the plugins importable, because it constructs each `STT` directly and points it at `127.0.0.1`. `probe/halfopen_probe.py` carries a per-plugin construction table: most plugins take a `base_url`, some a `model_endpoint`, one a `url`, and six take nothing that can be redirected.

### Reading the numbers

~46 seconds is `heartbeat=30.0` plus aiohttp's 15-second pong wait, plus connect. It is not a tuning choice anyone made per plugin — every plugin that passes a heartbeat passes `30.0`.

Whether 46 seconds is acceptable is a separate question from whether a plugin notices at all. [#7900](https://github.com/livekit/agents/issues/7900) argues it is a long time to be deaf on a phone call, and it is: the caller gets 46 seconds of an agent that cannot hear them before failover is even attempted. Shortening it would mean changing every plugin that already ships `30.0`, so it is a maintainers' decision rather than a bug.

## The open question

soniox's own log, against this server:

```
 5001ms  Soniox Speech-to-Text API connection established!
51278ms  Soniox STT WebSocket error frame: ServerTimeoutError('No PONG received after 15.0 seconds')
51279ms  Soniox STT WebSocket closed; requesting reconnect
51282ms  Soniox Speech-to-Text API connection established!
```

It detects the dead socket correctly and then reconnects into the same state, every ~46 seconds, indefinitely. `conn_options.max_retry=0` does not bound it, because that reconnect is the plugin's own loop rather than the base class's retry. A `FallbackAdapter` is never told, so the caller stays deaf for as long as the call lasts.

Against a provider that has genuinely vanished, the reconnect would fail and surface an error. This models the narrower case of a server that *accepts* connections and then goes silent — a load balancer in front of a dead backend, or a provider bug. Whether an unbounded internal reconnect should ever give up and report failure is a policy question, which is why it is recorded here rather than filed as a bug.

## Related

- [livekit/agents#7900](https://github.com/livekit/agents/issues/7900) — the original four-plugin measurement, by @DEVJHAWAR11, and the probe this one extends
- [#7913](https://github.com/livekit/agents/pull/7913) — openai, elevenlabs, inworld
- [#7914](https://github.com/livekit/agents/pull/7914) — gladia, nabrah, sarvam, slng
- [#7943](https://github.com/livekit/agents/pull/7943) — baseten, fireworksai, gradium
- [#5717](https://github.com/livekit/agents/issues/5717) — why a plain read timeout is the wrong instrument for STT, where a quiet caller is the normal state

Raw numbers: [`results/2026-10-20-origin-main.json`](results/2026-10-20-origin-main.json).
