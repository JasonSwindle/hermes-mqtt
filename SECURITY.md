# Security policy

## Reporting a vulnerability

Use GitHub's private reporting:
**[Report a vulnerability](https://github.com/JasonSwindle/hermes-mqtt/security/advisories/new)**.
Please do not open a public issue for a security problem.

Include the affected version, the Hermes version, the broker in use, and the smallest
steps that show the problem. **Never send broker credentials** — if a secret appears in a
log line you want to quote, replace it with `***`.

Expect an acknowledgement within a few days; this is a single-maintainer, community project.

## Supported versions

Fixes land on the latest release. The manifest declares
`requires_hermes: ">=0.22.3"` — a floor, not a ceiling: the oldest host the plugin claims
to load on. Its declared seams were exercised on v0.22.5 and v0.23.1 (0.22.3 itself was
not independently exercised here). A newer Hermes is attempted rather than refused up
front; a missing optional seam (job mode, cross-platform delivery) is detected per event
and reported without taking the adapter down. A host below the floor is skipped by the
loader rather than failing at registration.

## Scope

In scope for this repository:

- **Credential handling.** The broker password is read from the environment through
  Hermes' platform env helper, never logged, and never written to `config.yaml`. Any path
  that puts it in a log line, an error message or a published payload is a vulnerability.
- **Trust boundary.** `trusted_inbound=True` is claimed because the broker authenticated
  the connection. Nothing in the envelope is trusted for authorization. A way to use a
  published message to bypass allowlists, pairing or a job's permissions is in scope.
  Payload text is data, never control: agent-mode events are built with
  `allow_gateway_control=False` and the platform registers `allow_update_command=False`, so
  a bus payload must not be able to issue `/update`, `/new` or any other gateway command.
- **The own-topic guard.** A widened subscription must not consume this adapter's own
  replies; a route that publishes into the consumed namespace is a flaw, not a config
  choice.
- **Hostile input.** A truncated packet, a property block that lies about its length, a
  PUBLISH larger than the broker's declared maximum, an oversized topic, invalid UTF-8, a
  non-minimal length, a duplicate singleton property, or a CONNACK that lies about Session
  Present: the session must reject or disconnect, never loop, hang, or grow memory without
  bound. Inbound packets above `max_packet_bytes` (default 16 MiB) are refused from the
  remaining length before allocation; malformed flags (QoS 3, DUP at QoS 0, reserved bits on
  a control packet), empty/wildcard/U+0000 topics, unsupported topic aliases and a QoS 2
  delivery on a QoS 0–1 subscription end the session instead of being routed. A retained
  packet is additionally dropped unless `accept_retained` is true.
- **Delivery correctness.** QoS 1 is PUBACKed only after the message is queued and the
  adapter's own queue is bounded; an ack must never promise a message the adapter discarded.
  A slow consumer must not silence the keepalive (it runs in its own task) or desync framing
  (no read is cancelled mid-packet).
- **Transport security.** `mqtts://` must verify the broker certificate chain and hostname
  by default; a path that silently disables verification, downgrades TLS to plaintext, or
  accepts a certificate without the user's explicit `tls_insecure` opt-in is a vulnerability.
  TLS config errors and failed handshakes are reported, never retried as plaintext.
- **Key material handling.** TLS CA/certificate/key paths come from configuration; key
  contents and `tls_key_password` must never appear in a log line, status field, error
  message or published payload.
- **No eval/exec/subprocess surface, and bounded file reads.** The plugin has no
  `eval`, `exec`, dynamic-import or subprocess surface. File access is limited to configured
  TLS material: the stdlib `ssl` context loads the CA/certificate/key paths at connect time,
  and a 4 KiB prefix of the client key is read to detect an encrypted key before OpenSSL can
  prompt. Contents are never logged, returned, persisted or published. Anything beyond that
  is in scope.

Out of scope here (report upstream instead):

- Bugs in Hermes Agent itself, including `hermes plugins validate` and the gateway plugin
  loader: [NousResearch/hermes-agent](https://github.com/NousResearch/hermes-agent/issues).
- Bugs in Mosquitto, EMQX, HiveMQ or any other broker.
- **Plaintext is an operator choice.** `mqtts://` is supported and verifies by default, but
  `tcp://` remains available for a trusted LAN/loopback listener. Credentials crossed over
  plaintext on an untrusted network are the operator's risk to close (use `mqtts://` or a
  broker-side TLS terminator).
- **WebSocket transport.** Not implemented yet (planned follow-up, `docs/WEBSOCKET-PLAN.md`);
  a request for `ws://`/`wss://` is a missing feature, not a vulnerability.
- The general risk that a Hermes plugin runs in-process with the agent's environment. That
  is a host design property; it is why this plugin adds no surface beyond one outbound
  broker connection.

## Design notes relevant to attackers

- Runtime imports: standard library plus `gateway.platforms.base`,
  `gateway.platforms.event`, `gateway.platforms.helpers` (the host's
  `MessageDeduplicator`), `gateway.platforms._shared` and `gateway.config` — public plugin
  surfaces. One guarded import of `tools.cronjob_tools.execute_job_for_event` for
  job mode, the same entry point the bundled webhook adapter uses.
- No `eval`, `exec`, dynamic imports or subprocesses. Direct file I/O is limited to TLS
  material configured by the operator: the stdlib `ssl` context loads CA/certificate/key
  paths, and `mqtt_client._key_header()` reads the first 4 KiB of the client key only to
  detect encryption. Nothing read is logged, returned or persisted.
- The TLS test fixtures under `tests/fixtures/tls/` are throwaway test-only keys committed
  so CI needs no `openssl`; they are never loaded by the plugin at runtime.
- Replies are published as JSON to `publish_topic`; the payload is built from the inbound
  envelope and the template, and the topic is configuration, never inbound data.
- The bus cannot drive gateway control: agent-mode events set `allow_gateway_control=False`
  and the platform registration sets `allow_update_command=False`.
- The plugin cannot update itself: a catalog install pins a full commit SHA.
