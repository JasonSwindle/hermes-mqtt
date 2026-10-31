#!/usr/bin/env python3
"""Publish real events to a live broker, through this plugin's own MQTT client.

This is the check CI cannot run: it needs a broker and credentials. It proves the deployed
plugin moves a message end to end — publish here, then look for the delivery line in the
gateway log (``[mqtt] delivered <topic> → <platform>:<target>``).

    python scripts/e2e_live.py --dry-run          # show the resolved broker, connect nothing
    python scripts/e2e_live.py --topic test.hello # publish one envelope
    python scripts/e2e_live.py --verify-log /opt/data/logs/agent.log

For the 0.7.0 coalescing probe, ``--burst`` publishes N distinct events on one wire topic
and ``--verify-log`` then checks that the batch produced one action, not N:

    python scripts/e2e_live.py --burst 5 --topic cloud.github.push \\
        --wire-topic events/cloud.github.push
    python scripts/e2e_live.py --verify-log /path/to/agent.log --burst 5 \\
        --topic cloud.github.push --coalesce-prefix cloud.

The burst must finish inside a 2 s window (the probe's premise): lower ``--interval`` when a
run reports it missed. The log check reads the last 4000 lines of the file; it requires
exactly one ``[mqtt] coalesced N messages on <prefix> → <mode>`` line and exactly one action
line (``[mqtt] agent turn for`` or ``[mqtt] fired cron job``). ``--coalesce-prefix`` defaults
to the first dotted segment of ``--topic`` (``cloud.github.push`` → ``cloud.``), or of the
wire topic with its subscription head stripped when only ``--wire-topic`` names the topic;
pin it explicitly when the gateway policy differs.

Broker settings resolve in this order: flags, then the environment, then ``/opt/data/.env``
(the file Hermes itself loads). Nothing is printed except the host, port and topic.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import re
import sys
import time
import types
import uuid
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

_PLUGIN_DIR = Path(__file__).resolve().parents[1]

# Import the client without running mqtt/__init__.py: that module pulls in the gateway, and
# this probe has to work on a box where Hermes is not importable.
_package = types.ModuleType("mqtt")
_package.__path__ = [str(_PLUGIN_DIR)]
sys.modules.setdefault("mqtt", _package)

from mqtt.mqtt_client import (  # noqa: E402
    MqttAuthError,
    MqttError,
    MqttSession,
    build_tls_context,
)

TLS_SCHEMES = ("mqtts", "ssl", "tls")
DEFAULT_TOPIC = "test.hello"
#: The checked log window: a burst probe must be the only run in it.
VERIFY_TAIL_LINES = 4000
#: "Within 2 s" is the probe's premise (spec section 5: publish 5 messages in 2 s).
BURST_WINDOW_SECONDS = 2.0
#: Contract log lines written by adapter.py 0.7.0 when a batch flushes / an action starts.
_COALESCED_LINE = re.compile(
    r"\[mqtt\] coalesced (?P<count>\d+) messages on (?P<prefix>.+?) → (?P<mode>[a-z]+)")
_ACTION_NEEDLES = {
    "agent": "[mqtt] agent turn for",
    "job": "[mqtt] fired cron job",
}


def _truthy(value: str) -> bool:
    return str(value or "").strip().lower() in ("1", "true", "yes", "on")


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected a positive integer, got {value!r}") from None
    if parsed < 1:
        raise argparse.ArgumentTypeError(f"expected a positive integer, got {parsed}")
    return parsed


def _seconds(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected seconds, got {value!r}") from None
    if not math.isfinite(parsed) or parsed < 0:
        raise argparse.ArgumentTypeError(f"expected seconds >= 0, got {value!r}")
    return parsed


def derive_coalesce_prefix(topic: str, wire_topic: str | None = None) -> str:
    """Best guess at the matched topic-policy prefix for the probe's topic.

    The envelope topic is the first match input; when the caller named only a wire topic
    (the adapter also matches a wire topic with its subscription head stripped:
    ``events/cloud.github.push`` → ``cloud.github.push``), fall back to that instead. A
    policy is usually configured at the top-level namespace (``cloud.``), so the returned
    prefix is the first dotted segment. It is a guess — ``--coalesce-prefix`` pins it.
    """
    name = str(topic or "").strip()
    if wire_topic and name in ("", DEFAULT_TOPIC):
        head, _sep, rest = str(wire_topic).strip().partition("/")
        name = rest or head
    if not name:
        return ""
    if name.endswith("."):
        return name
    head, dot, _rest = name.partition(".")
    return f"{head}." if dot else name


@dataclass
class Broker:
    host: str
    port: int
    user: str
    password: str
    protocol: str
    tls: bool = False
    tls_ca_file: str = ""
    tls_cert_file: str = ""
    tls_key_file: str = ""
    tls_key_password: str = ""
    tls_server_name: str = ""
    tls_insecure: bool = False


def dotenv(path: str = "/opt/data/.env") -> dict[str, str]:
    values: dict[str, str] = {}
    try:
        with open(path, encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                values[key.strip()] = value.strip().strip('"').strip("'")
    except OSError:
        pass
    return values


def resolve(args: argparse.Namespace) -> Broker:
    env = dotenv()

    def pick(flag: str | None, key: str) -> str:
        return flag or os.environ.get(key) or env.get(key) or ""

    raw = pick(args.url, "MQTT_HERMES_URL") or "tcp://localhost:1883"
    parsed = urlparse(raw if "://" in raw else f"tcp://{raw}")
    if parsed.scheme not in (*TLS_SCHEMES, "tcp", "mqtt"):
        raise ValueError(
            f"unsupported MQTT scheme {parsed.scheme!r} "
            "(expected mqtts://, tcp://, mqtt://, tls:// or ssl://)")
    if parsed.username or parsed.password:
        raise ValueError(
            "credentials in the MQTT url are not supported; use MQTT_HERMES_USER/_PASSWORD")
    tls = parsed.scheme in TLS_SCHEMES
    return Broker(
        host=parsed.hostname or "localhost",
        port=parsed.port or (8883 if tls else 1883),
        user=pick(args.user, "MQTT_HERMES_USER"),
        password=pick(args.password, "MQTT_HERMES_PASSWORD"),
        protocol=pick(args.protocol, "MQTT_HERMES_PROTOCOL") or "auto",
        tls=tls,
        tls_ca_file=pick(args.tls_ca, "MQTT_HERMES_TLS_CA_FILE"),
        tls_cert_file=pick(args.tls_cert, "MQTT_HERMES_TLS_CERT_FILE"),
        tls_key_file=pick(args.tls_key, "MQTT_HERMES_TLS_KEY_FILE"),
        tls_key_password=pick(args.tls_key_password, "MQTT_HERMES_TLS_KEY_PASSWORD"),
        tls_server_name=pick(args.tls_server_name, "MQTT_HERMES_TLS_SERVER_NAME"),
        tls_insecure=args.tls_insecure or _truthy(pick(None, "MQTT_HERMES_TLS_INSECURE")),
    )


def envelope(topic: str, note: str) -> bytes:
    return json.dumps(
        {
            "source": "e2e-live",
            "event_id": uuid.uuid4().hex,
            "occurred_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "topic": topic,
            "summary": note,
            "probe": True,
        }
    ).encode("utf-8")


async def publish(args: argparse.Namespace, broker: Broker) -> int:
    wire = args.wire_topic or f"events/{args.topic}"
    burst = args.burst is not None
    count = args.burst if burst else 1
    ssl_context = None
    server_hostname = None
    if broker.tls:
        ssl_context = build_tls_context(
            insecure=broker.tls_insecure,
            ca_file=broker.tls_ca_file,
            cert_file=broker.tls_cert_file,
            key_file=broker.tls_key_file,
            key_password=broker.tls_key_password or None,
        )
        server_hostname = broker.tls_server_name or broker.host
    session = await MqttSession.open(
        broker.host,
        broker.port,
        client_id=f"e2e-live-{uuid.uuid4().hex[:8]}",
        username=broker.user,
        password=broker.password,
        protocol=broker.protocol,
        ssl_context=ssl_context,
        server_hostname=server_hostname,
    )
    try:
        level = "5.0" if session.protocol == 5 else "3.1.1"
        transport = "TLS" if broker.tls else "TCP"
        if not burst:
            await session.publish(wire, envelope(args.topic, args.note), qos=1)
            print(f"published to {wire} as mqtt {level} ({transport}, "
                  f"{broker.host}:{broker.port})")
            return 0

        # One session, one wire topic, N distinct events: every envelope gets a fresh
        # event_id, so coalescing cannot dismiss the burst as duplicates.
        started = time.monotonic()
        for index in range(1, count + 1):
            note = f"{args.note} [{index}/{count}]"
            await session.publish(wire, envelope(args.topic, note), qos=1)
            if index != count:
                await asyncio.sleep(args.interval)
        elapsed = time.monotonic() - started
        print(f"burst: published {count} distinct envelopes to {wire} in {elapsed:.2f}s "
              f"(interval {args.interval:g}s, mqtt {level}, {transport}, "
              f"{broker.host}:{broker.port})")
        if elapsed > BURST_WINDOW_SECONDS:
            print(f"burst missed the {BURST_WINDOW_SECONDS:g}s probe window "
                  f"({elapsed:.2f}s): the adapter may have flushed between events — "
                  f"lower --interval", file=sys.stderr)
            return 5
        return 0
    finally:
        await session.aclose()


def _read_tail(path: str) -> list[str] | None:
    try:
        with open(path, encoding="utf-8", errors="replace") as handle:
            return handle.readlines()[-VERIFY_TAIL_LINES:]
    except OSError as exc:
        print(f"cannot read {path}: {exc}", file=sys.stderr)
        return None


def verify_log(path: str, topic: str) -> int:
    """Look for the delivery line the gateway writes; the second, independent channel."""
    lines = _read_tail(path)
    if lines is None:
        return 2
    needle = f"[mqtt] delivered {topic}"
    hits = [line.strip() for line in lines if needle in line]
    if not hits:
        print(f"no '{needle}' line in the last {VERIFY_TAIL_LINES} lines of {path}",
              file=sys.stderr)
        return 1
    print(hits[-1])
    return 0


def verify_burst_log(path: str, count: int, prefix: str, *, derived: bool) -> int:
    """Assert a coalesced burst produced exactly one batch and exactly one action.

    This is the acceptance check for the live probe: N events published on one coalesced
    topic must log one ``coalesced N messages on <prefix> → <mode>`` line and one action,
    which is the whole point of the feature (not one action per event).
    """
    lines = _read_tail(path)
    if lines is None:
        return 2

    batches: list[tuple[int, int, str, str, str]] = []  # (line index, N, prefix, mode, text)
    actions: list[tuple[int, str, str]] = []  # (line index, mode, text)
    for index, raw in enumerate(lines):
        line = raw.strip()
        match = _COALESCED_LINE.search(line)
        if match:
            batches.append((
                index, int(match.group("count")), match.group("prefix"),
                match.group("mode"), line,
            ))
            continue
        for mode, needle in _ACTION_NEEDLES.items():
            if needle in line:
                actions.append((index, mode, line))
                break

    expected = [batch for batch in batches if batch[1] == count and batch[2] == prefix]
    problems: list[str] = []
    if not expected:
        if batches:
            seen = "\n".join(f"  {batch[4]}" for batch in batches)
            problems.append(
                f"no '[mqtt] coalesced {count} messages on {prefix} → <mode>' line in the "
                f"last {VERIFY_TAIL_LINES} lines of {path}; coalesced lines found:\n{seen}")
        else:
            problems.append(
                f"no '[mqtt] coalesced ...' line in the last {VERIFY_TAIL_LINES} lines "
                f"of {path} — is the topic policy configured with coalesce?")
    elif len(expected) > 1:
        seen = "\n".join(f"  {batch[4]}" for batch in expected)
        problems.append(
            f"expected exactly one coalesced line for {count} messages on {prefix}, "
            f"found {len(expected)}:\n{seen}")

    if len(expected) == 1:
        batch_index, _n, _prefix, mode, _batch_line = expected[0]
        if mode not in _ACTION_NEEDLES:
            problems.append(
                f"coalesced line reports mode {mode!r}, but only agent/job coalesce")
        elif not actions:
            needles = " / ".join(_ACTION_NEEDLES.values())
            problems.append(
                f"no action line ({needles}) in the last {VERIFY_TAIL_LINES} lines of {path}")
        elif len(actions) > 1:
            seen = "\n".join(f"  {action[2]}" for action in actions)
            problems.append(f"expected exactly one action line, found {len(actions)}:\n{seen}")
        else:
            action_index, action_mode, _action_line = actions[0]
            if action_mode != mode:
                problems.append(
                    f"coalesced line says mode {mode!r}, but the action line is a "
                    f"{action_mode} action")
            elif action_index < batch_index:
                problems.append(
                    "the action line precedes the coalesced line in the checked tail — "
                    "the log mixes runs and cannot prove this batch")

    if problems:
        for problem in problems:
            print(f"verify-log: {problem}", file=sys.stderr)
        return 1

    _batch_index, _n, _prefix, mode, batch_line = expected[0]
    _action_index, _action_mode, action_line = actions[0]
    print(batch_line)
    print(action_line)
    print(f"verified: {count} events → 1 {mode} action in the last "
          f"{VERIFY_TAIL_LINES} lines of {path}")
    if derived:
        print(f"prefix: {prefix!r} derived from --topic/--wire-topic; "
              f"pass --coalesce-prefix to pin it")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Publish real events to a live broker through this plugin's MQTT client."
    )
    parser.add_argument("--url", help="broker URL; default MQTT_HERMES_URL")
    parser.add_argument("--user", help="broker username; default MQTT_HERMES_USER")
    parser.add_argument("--password", help="prefer MQTT_HERMES_PASSWORD over this flag")
    parser.add_argument("--protocol", help="auto | 5 | 3.1.1 (default auto)")
    parser.add_argument("--tls-ca", help="CA bundle (PEM) for a private broker CA; default MQTT_HERMES_TLS_CA_FILE")
    parser.add_argument("--tls-cert", help="client certificate (PEM) for mutual TLS; default MQTT_HERMES_TLS_CERT_FILE")
    parser.add_argument("--tls-key", help="client private key (PEM) for mutual TLS; default MQTT_HERMES_TLS_KEY_FILE")
    parser.add_argument("--tls-key-password", help="client key password; default MQTT_HERMES_TLS_KEY_PASSWORD")
    parser.add_argument("--tls-server-name", help="certificate name / SNI override; default MQTT_HERMES_TLS_SERVER_NAME")
    parser.add_argument("--tls-insecure", action="store_true",
                        help="UNSAFE: disable certificate verification (default MQTT_HERMES_TLS_INSECURE)")
    parser.add_argument("--topic", default=DEFAULT_TOPIC,
                        help=f"envelope topic (default {DEFAULT_TOPIC})")
    parser.add_argument("--wire-topic", help="wire topic (default events/<topic>)")
    parser.add_argument("--note", default="e2e-live probe", help="summary text in the envelope")
    parser.add_argument("--burst", type=_positive_int, metavar="N",
                        help="publish N distinct envelopes to one wire topic (coalescing probe)")
    parser.add_argument("--interval", type=_seconds, default=0.2, metavar="S",
                        help="seconds between burst publishes (default 0.2; only with --burst)")
    parser.add_argument("--coalesce-prefix", metavar="PREFIX",
                        help="matched topic-policy prefix a --burst log line must report "
                             "(default: first dotted segment of --topic/--wire-topic)")
    parser.add_argument("--dry-run", action="store_true", help="print the resolved broker, connect nothing")
    parser.add_argument("--verify-log", metavar="PATH",
                        help="check the gateway log for the delivery line, or for the "
                             "one coalesced batch when --burst is given")
    args = parser.parse_args()

    if args.coalesce_prefix is not None and not (args.verify_log and args.burst is not None):
        parser.error("--coalesce-prefix is only meaningful with --verify-log PATH and --burst N")
    if args.burst is not None and not args.verify_log:
        slowest = (args.burst - 1) * args.interval
        if slowest > BURST_WINDOW_SECONDS:
            parser.error(
                f"--burst {args.burst} with --interval {args.interval:g}s needs at least "
                f"{slowest:g}s and cannot fit the {BURST_WINDOW_SECONDS:g}s probe window; "
                f"lower --interval")

    try:
        broker = resolve(args)
    except ValueError as exc:
        print(f"broker url: {exc}", file=sys.stderr)
        return 2

    if args.verify_log:
        if args.burst is None:
            return verify_log(args.verify_log, args.topic)
        prefix = args.coalesce_prefix
        derived = prefix is None
        if derived:
            prefix = derive_coalesce_prefix(args.topic, args.wire_topic)
        return verify_burst_log(args.verify_log, args.burst, prefix, derived=derived)

    if args.dry_run:
        scheme = "mqtts" if broker.tls else "tcp"
        print(f"broker {scheme}://{broker.host}:{broker.port} "
              f"user={broker.user or '(anonymous)'} protocol={broker.protocol}")
        if broker.tls:
            print(f"tls ca={broker.tls_ca_file or 'system trust store'} "
                  f"client_cert={'yes' if broker.tls_cert_file else 'no'} "
                  f"insecure={broker.tls_insecure}")
        wire = args.wire_topic or f"events/{args.topic}"
        if args.burst is None:
            print(f"would publish to {wire}")
        else:
            print(f"would publish {args.burst} distinct envelopes to {wire} every "
                  f"{args.interval:g}s (coalesced-burst probe, inside "
                  f"{BURST_WINDOW_SECONDS:g}s)")
        return 0

    try:
        return asyncio.run(publish(args, broker))
    except ValueError as exc:  # TLS material/config
        print(f"TLS configuration: {exc}", file=sys.stderr)
        return 2
    except MqttAuthError as exc:
        print(f"broker refused the credentials: {exc}", file=sys.stderr)
        return 3
    except (MqttError, OSError) as exc:
        print(f"publish failed: {exc}", file=sys.stderr)
        return 4


if __name__ == "__main__":
    raise SystemExit(main())
