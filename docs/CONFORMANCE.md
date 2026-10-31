# MQTT conformance and scope

**Client:** `mqtt_client.py` (session, pump, keepalive) + `mqtt_codec.py` (wire format).
**Protocols:** MQTT 3.1.1 (protocol level 4) and MQTT 5.0 (level 5), negotiated per connection.
**Transport:** TCP, or TLS 1.2+ (`mqtts://`/`tls://`/`ssl://`, default port 8883). No WebSocket (planned, see [WEBSOCKET-PLAN.md](WEBSOCKET-PLAN.md)).

## Implemented

| Capability | 3.1.1 | 5.0 | Notes |
| --- | --- | --- | --- |
| CONNECT / CONNACK | yes | yes | v5 adds a property block; level `04` vs `05` |
| SUBSCRIBE / SUBACK | yes | yes | v5 options set Retain Handling 2 |
| PUBLISH / PUBACK QoS 0–1 | yes | yes | v5 reason codes are outcomes, not ignored bytes |
| PINGREQ / PINGRESP | yes | yes | PINGREQ is driven by our own outbound silence, in its own task; keepalive 0 disables it |
| DISCONNECT (client) | yes | yes | normal-disconnect packet on graceful close (no reason code) |
| DISCONNECT (server) | n/a | yes | reason code surfaced in the error |
| Reconnect | adapter-level | adapter-level | 2/5/15/30/60 s ±25% jitter |
| Protocol ladder | yes | yes | `auto` falls back on CONNACK `0x01`/`0x84`, a v3 CONNACK to a level-5 CONNECT, or a lost CONNACK |
| Duplicate suppression | adapter-level | adapter-level | `event_id` only, 1 h TTL, 4096 entries; no content fingerprint |
| Burst coalescing | yes | yes | per-policy window, bounded at 256 buckets, flushed on disconnect, opt-in |
| Password without username | rejected | accepted | [MQTT-3.1.2-18] applies to 3.1.1 only |
| Inbound packet cap | yes | yes | `max_packet_bytes` checked from the remaining length before allocation |
| Malformed PUBLISH | yes | yes | QoS bits 3, DUP at QoS 0, empty/wildcard/invalid-UTF-8 topic, unsupported topic alias, packet id 0 → session ends |
| SUBACK validation | yes | yes | 0x80+ is `SubscriptionRefused`; granted QoS above requested and multi-code packets are errors |
| CONNACK validation | yes | yes | reserved flags, session-present after clean start, trailing bytes, duplicate properties, Receive Maximum 0, Maximum Packet Size 0, Maximum QoS > 1, Retain Available > 1 |
| PUBACK validation | yes | yes | v5 reason codes checked against the table, packet id 0 and trailing bytes rejected |
| Fixed-header flags | yes | yes | reserved flag bits on control packets are malformed |
| QoS 2 inbound | yes | yes | a QoS 2 PUBLISH/flow packet on a QoS 0–1 subscription ends the session (never silently dropped) |
| UTF-8 / varints | yes | yes | strict UTF-8, no U+0000, minimal-length variable byte integers |
| TLS transport | yes | yes | stdlib `ssl`; server certificate + hostname verified by default; TLS 1.2+ |
| TLS configuration | yes | yes | custom CA (`tls_ca_file`), mTLS cert/key (+ key password), `tls_server_name` override, explicit `tls_insecure` opt-out |
| TLS failure handling | yes | yes | config/handshake failures raise `MqttTlsError` → adapter `mqtt_tls_failed`; record-level TLS errors do not use the MQTT level ladder and never downgrade. A peer that closes after the handshake but before CONNACK is ladder-eligible (`TlsConnackLost`); if every level ends that way, `MqttTlsLost` is reported and kept retryable so a rotated/rejected identity can recover |

### MQTT 5 properties honoured

| Property | Effect |
| --- | --- |
| Server Keep Alive `0x13` | overrides the client's keepalive |
| Receive Maximum `0x21` | caps unacknowledged QoS 1/2 publishes (semaphore) |
| Maximum Packet Size `0x27` | refuses an oversized packet before sending |
| Maximum QoS `0x24` | refuses a publish above the broker's ceiling |
| Retain Available `0x25` | refuses a retained publish when unsupported |
| Reason codes | SUBACK/PUBACK ≥ `0x80` raise; `0x10` logs "no matching subscribers" |

## Deliberately not implemented

Session expiry (every connection is Clean Start with expiry 0, so the broker queues nothing
while the consumer is disconnected), will messages, QoS 2, shared subscriptions owned by the
broker, inbound topic aliases (refused as malformed because the client never advertises a
Topic Alias Maximum), enhanced auth (AUTH), message expiry, user-property propagation into
the envelope, WebSocket transport (`ws://`/`wss://`), HTTP CONNECT proxy tunnels, TLS 1.0/1.1
(disabled by the stdlib default minimum of TLS 1.2).

A refused SUBACK (any reason ≥ `0x80`) is treated as adapter-fatal even though MQTT 5 calls
those per-packet outcomes: a broker refusing the one subscription this client will ever make
means reconnecting would loop on a bad filter or ACL. The only non-graceful close is a
normal-disconnect packet (v5 §4.13.1 SHOULD-level reason codes are not sent).

## Enforced limits

| Limit | Behaviour |
| --- | --- |
| QoS | only 0 and 1; anything else raises `ValueError` before a packet is sent |
| Keep Alive | 0–65535; 0 disables PINGREQ entirely; PINGREQ goes out at half the interval of outbound silence |
| Password | requires a username on 3.1.1; allowed without one on 5.0 |
| Publish topic | non-empty, no `+` or `#`; wildcards are filter-only |
| Subscribe filter | MQTT filter syntax validated before dialling (wildcards whole-level, `#` last) |
| Report render size | rendered text capped at `max_render_chars` (adapter default 64 KiB) with a truncation marker |
| Retained inbound | dropped unless `accept_retained: true`; v5 also asks the broker not to send them |
| Inbound packet size | `max_packet_bytes` (adapter default 16 MiB) — a larger packet ends the session before its body is buffered |
| Malformed PUBLISH | QoS bits 3, empty/wildcard topic, unsupported topic alias → `MqttError`, never routed |
| CONNACK limits | Receive Maximum 0, Maximum Packet Size 0, Maximum QoS > 1, Retain Available > 1 → `MqttError` |
| Truncated PUBLISH | ends the session with `MqttError`, never a partial delivery |
| Backpressure | QoS 1 is PUBACKed only after the message is queued; a full queue blocks the pump while the heartbeat task keeps the connection alive |
| TLS | certificate chain + hostname verification on by default (system trust or `tls_ca_file`); mTLS pair validated; `tls_insecure` is the only opt-out and logs a warning; a failed handshake is never retried as plaintext |
| TLS material | paths are configuration (env or YAML); key contents/key passwords are never logged |

## Verified by

`python -m unittest discover -s tests` — 222 tests: byte-level codec assertions for both
dialects, a fake broker that re-implements framing independently, wire end-to-end tests for
the router, coalescing tests for the per-prefix buckets, flush triggers and batch
rendering/truncation, TLS tests against a real loopback TLS listener using committed
throwaway fixtures (`tests/fixtures/tls/`), and mutation checks on the v5, hostile-input and
TLS guards. Not covered: a live broker, real public-CA chains, a broker-side TLS proxy, and
the features listed as out of scope.
