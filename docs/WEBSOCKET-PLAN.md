# WebSocket transport (`ws://` / `wss://`) — planned follow-up release

**Status: not implemented.** This is the design/estimate note for the follow-up release;
the plugin currently supports TCP and TLS only (`tcp://`, `mqtts://`). See the TLS section
of the [README](../README.md) for what is available today.

## Why it is a separate release

MQTT over WebSockets requires a hand-rolled RFC 6455 client: the Hermes runtime ships no
WebSocket library, and the plugin's no-dependency invariant forbids adding one. TLS was a
small change because the stdlib `ssl` module does the protocol work; WebSockets adds the
HTTP/1.1 Upgrade handshake plus the frame state machine, a test-only WebSocket server for
the offline suite, and a larger interop surface. It is roughly **2× the TLS change**:
~1,000–1,400 additional lines including tests, 2–3 focused sessions, medium-high risk.

Scope it only for a concrete need: this plugin is a **server-side** MQTT client, so
WebSockets are not needed for browser compatibility — they matter when only HTTP(S) egress
is allowed (corporate proxy/firewall, managed platform) or the broker exposes a WS listener
only.

## Proposed design

1. **Schemes/URLs:** `ws://` (default port 80) and `wss://` (default 443, TLS via the
   existing `build_tls_context` verification path). MQTT-over-WS path is broker-specific
   and must come from the URL (e.g. `ws://host:8083/mqtt`, `wss://host/mqtt`); default
   path `/mqtt` if absent. Plain `80/443` defaults only when no port is given.
2. **Transport seam:** extract the byte-stream contract from `MqttSession` (currently
   `_reader`/`_writer` touched in ~8 places) into a small `Transport` interface:
   `read(n)`, `write(data)`, `close()`. `StreamTransport` wraps the current asyncio
   streams; `WebSocketTransport` wraps streams + a frame codec and exposes MQTT bytes.
   The MQTT pump, keepalive, codecs and adapter are otherwise unchanged.
3. **Handshake:** HTTP/1.1 `GET <path>` with `Host`, `Upgrade: websocket`,
   `Connection: Upgrade`, random `Sec-WebSocket-Key` (16 bytes, base64),
   `Sec-WebSocket-Version: 13`, `Sec-WebSocket-Protocol: mqtt`; validate `101`, the
   `Sec-WebSocket-Accept` SHA-1/base64 digest, and the negotiated subprotocol. Optional
   configured extra headers for brokers that require them (e.g. an `Authorization` token).
   Handshake failures are deterministic → fatal `mqtt_ws_failed` (like `mqtt_tls_failed`).
4. **Framing:** client-to-server frames must be masked with a fresh random 4-byte key;
   support 7/16/64-bit payload lengths; reassemble fragmented binary messages;
   handle interleaved control frames (ping → pong with echo, close handshake, reject text
   frames); enforce the existing `max_packet_bytes` cap on reassembled messages before
   allocation; send each outbound MQTT packet as one binary message (single frame) and
   close with code 1000 on graceful shutdown. No `permessage-deflate`.
5. **Keepalive:** MQTT PINGREQ continues inside WS frames; WS-level pings are handled by
   the transport. No extra WS ping timer unless a load balancer requires one.
6. **Config surface:** scheme in `url`; optional `ws_subprotocol` (default `mqtt`, some
   brokers want `mqttv3.1`), optional `ws_headers` map for proxy/token headers. `wss://`
   reuses `tls_ca_file`/`tls_cert_file`/`tls_key_file`/`tls_server_name`/`tls_insecure`.
   Env equivalents for the two new settings.
7. **Tests:** a stdlib WebSocket test server (handshake + server-side frame parsing,
   reusing `FakeBroker`'s MQTT fixture logic behind the WS transport) plus tests for the
   handshake, masking, fragmentation, interleaved control frames, close handshake,
   oversize frames, wrong accept hash/status, `ws` vs `wss` (reusing the TLS fixtures),
   path/subprotocol configuration, and adapter end-to-end routing over WS. Docs, manifest
   optional-env entries, and `scripts/e2e_live.py` flags (`--ws-path`, `--ws-subprotocol`,
   `--ws-header`).
8. **Version:** the release after 0.7.0, committed and tagged in the same release commit per
   [AGENTS.md](../AGENTS.md).

## Out of scope for that release

HTTP CONNECT proxy tunnels, HTTP/2, redirects, `permessage-deflate`, AWS IoT SigV4
query/header signing, browser/client-side use, and WS ping-based load-balancer keepalive
(add only if a concrete deployment needs it).

## Interop checklist (live broker, not CI)

Run the manual `scripts/e2e_live.py` flow against at least: Mosquitto
(`listener 8083` + `protocol websockets`), EMQX (`/mqtt`), HiveMQ (`/mqtt`), and one
cloud broker (AWS IoT `/mqtt`, Azure IoT) to confirm path/subprotocol/TLS behaviour before
tagging. CI can only cover the local stdlib WebSocket fixture.
