# AGENTS.md

Hermes Agent plugin: makes an MQTT broker a gateway platform (`kind: platform`). Inbound
bus messages route by topic policy; replies publish back to the bus. Nothing here is
loaded outside Hermes; the tests drive the adapter directly.

## Shape

- `plugin.yaml` — manifest. `name: mqtt` is the install path and the enable key.
- `__init__.py` — `register(ctx)`; the only place the platform is declared.
- `adapter.py` — `MqttAdapter`: routing, templates, dedupe, backoff, the four modes, burst
  coalescing, `send()`.
- `mqtt_client.py` — socket + TLS session: CONNECT/SUBSCRIBE/PUBLISH/PING, keepalive, reconnect.
  `build_tls_context()` builds the verified client context; TLS fixtures live under
  `tests/fixtures/tls/` (throwaway, test-only keys).
- `mqtt_codec.py` — wire encoding/decoding and v5 properties; every packet-level test goes here.
- `tests/` — `unittest`, no third-party runner. `_bootstrap.py` registers the package under
  `mqtt` whatever the checkout directory is called.

## Commands

```bash
# tests — uv, not pip: Hermes's own dependencies resolve only through its uv.lock.
# Must pass on Linux, macOS and Windows, Python 3.14 (the version Hermes pins for;
# the plugin itself is 3.11-clean).
git clone --depth 1 https://github.com/NousResearch/hermes-agent.git hermes-src
uv sync --frozen --no-dev --project hermes-src
uv run --project hermes-src --no-sync python -m unittest discover -s tests

# already running Hermes from a checkout? skip the clone and use it directly
PYTHONPATH=/path/to/hermes:/path/to/hermes/.venv/lib/python3.14/site-packages \
  python -m unittest discover -s tests

# the catalog admission gate — must print "Validation passed."
hermes plugins validate .

# prove the loader actually takes it, not just the suite:
# install into a throwaway home, enable, then expect
# "OK: runtime discovery, manifest parsing, import, and registration passed"
HERMES_HOME=/tmp/mqtt-home hermes plugins install JasonSwindle/hermes-mqtt --ref <sha> --yes-deps
HERMES_HOME=/tmp/mqtt-home hermes plugins enable mqtt
HERMES_HOME=/tmp/mqtt-home hermes plugins doctor mqtt

# the check CI cannot run — needs a broker and credentials, not a test fixture
python scripts/e2e_live.py --dry-run                        # resolve, connect nothing
python scripts/e2e_live.py --dry-run --url mqtts://broker:8883  # TLS dry-run
python scripts/e2e_live.py --topic test.hello               # publish one real event
python scripts/e2e_live.py --verify-log /path/to/agent.log  # the delivered line, or exit 1
```

`hermes plugins validate` warns "private core imports" on
`tools.cronjob_tools.execute_job_for_event`. That is expected: it is the documented
event-trigger entry point and the bundled webhook adapter calls it the same way. Keep it
out of module scope and behind the `ImportError` branch in `_job_mode`; the plugin's other
imports stay on public gateway surfaces.

## Invariants

- **Standard library only.** No runtime dependency, ever: the plugin must survive an image
  update that replaces the venv. A `pip install` at import time is a bug.
- **Unmatched topics drop.** Never escalate an unrouted message to an agent turn.
- **Own-topic guard.** Anything under `publish_topic` is ignored inbound.
- **Protocol scope**: 3.1.1 (level 4) and 5.0 (level 5), QoS 0–1, TCP and TLS
  (`mqtts://`/`tls://`/`ssl://`). No will, no QoS 2, no session expiry, no topic aliases,
  no AUTH, no WebSocket (`ws://`/`wss://` is the planned follow-up release; see
  `docs/WEBSOCKET-PLAN.md`). Widening it is a design decision, not a patch.
- **TLS verifies by default.** `mqtts://` verifies the certificate chain and hostname
  (system trust, or `tls_ca_file` as the only trust anchor when set); `tls_insecure` is the
  only opt-out, must stay explicit and logged, and a failed handshake must never be retried
  as plaintext. Never log key material or key passwords.
- **TLS failure vs lost CONNACK.** A record-level TLS error (`MqttTlsError`) is a TLS
  failure: no protocol-level fallback. A peer that completes the handshake and then closes
  before CONNACK is ambiguous and stays ladder-eligible (`TlsConnackLost`); if every level
  ends that way it surfaces as `MqttTlsLost`/`mqtt_tls_failed` and remains retryable.
- **`trusted_inbound` is earned by the broker's authentication**, not by payload content;
  never copy it to a platform a person can send to.
- **Payload text is data, never a command.** Agent-mode events set
  `allow_gateway_control=False` and the platform registers `allow_update_command=False`;
  bus traffic must not be able to issue `/update`, `/new` or any gateway control command.
- **Bounded inbound.** A packet larger than `max_packet_bytes` (default 16 MiB) ends the
  session and reconnects; the cap is enforced from the remaining length before allocating.
  Network-acquired bytes never size an unbounded buffer.
- **Coalescing is opt-in; defaults never change.** `coalesce` (seconds; absent or `0` = off)
  and `coalesce_max` (default 25) are per-topic-policy keys under `extra.topics.<prefix>`;
  with `coalesce` absent the adapter keeps the per-message path byte for byte. Only `agent`
  and `job` bucket — `deliver`/`drop` never coalesce — buckets are keyed by the matched
  prefix and capped at 256 (the oldest is flushed, never dropped), dedupe runs before
  buffering, and every batch flushes on the window, at `coalesce_max`, on `disconnect()` and
  on the fatal reconnect path. `_counts.agent`/`_counts.job` then count actions (a batch of 5
  is +1); `coalesced_messages` counts the buffered messages.
- **Refusal is fatal, transience is not.** A refused SUBACK (0x80+), a permanent CONNACK
  refusal and an invalid subscribe filter are fatal and reported to the gateway; a missing
  SUBACK, a lost CONNACK or a dropped socket retries with backoff (and `auto` advances the
  protocol ladder on a lost/rejected CONNACK before giving up).
- **Malformed inbound is fatal.** Truncated frames, non-minimal lengths, invalid UTF-8 or
  U+0000 topics, invalid fixed-header flags, duplicate properties, session-present after
  Clean Start, QoS 2 deliveries and QoS 3 flag bits end the session; they are never routed.
- **Acks mean accepted.** QoS 1 is PUBACKed only after the message is queued. The keepalive
  runs in its own task (an outbound obligation) and no read may be cancelled mid-packet —
  a slow consumer must not desync framing or silence PINGREQ.

## Release

`plugin.yaml`'s `version` must equal the tag (`v1.0.0` ↔ `version: 1.0.0`). The release
workflow asserts it, runs the suite, and attaches the archive. Bump the version in the
same commit that is tagged, never after.
