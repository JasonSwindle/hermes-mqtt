"""MQTT platform adapter — the event bus as a first-class Hermes platform.

Inbound: one MQTT session subscribes to ``events/#`` and routes each message by the
per-topic policy in ``platforms.mqtt.extra.topics``. Four modes exist because an
event bus at sensor volume must not turn into an agent turn per message:

===========  ==================================================================  =========
mode         what happens                                                        cost
===========  ==================================================================  =========
``deliver``  render the envelope into text and push it to another platform        $0
``agent``    start a real agent turn with the envelope as the prompt              a turn
``job``      fire an existing cron job, envelope as transient run context         $0
``drop``     count it, log once per topic                                         $0
===========  ==================================================================  =========

Anything unmatched is **dropped**, never escalated to a turn: silence is the safe
failure for an unattended bus.

Outbound: ``send()`` publishes a JSON envelope to ``publish_topic`` (default
``hermes/out``), deliberately outside ``events/#`` so a reply can never be
re-consumed by this adapter's own subscription.

Identity: the broker authenticates the *connection*, so the publisher is the
trusted ``events`` bus, not a person. ``trusted_inbound`` says exactly that, and
nothing in the payload is trusted for authorization.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import random
import socket
import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Optional
from urllib.parse import urlparse

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, SendResult
from gateway.platforms.event import MessageEvent, MessageType
from gateway.platforms.helpers import MessageDeduplicator
from gateway.platforms._shared import get_scoped_secret as _get_scoped_secret

from .mqtt_client import (
    MqttAuthError,
    MqttError,
    MqttFatal,
    MqttSession,
    MqttTlsError,
    MqttTlsLost,
    ProtocolUnsupported,
    SubscriptionRefused,
    build_tls_context,
)
from .mqtt_codec import normalize_protocol

logger = logging.getLogger(__name__)


def _parse_send_target(ref: str):
    """Chat-id parser for `send_message`: strip the ref, or None when it is blank.

    Local copy of the gateway's one-line rule so the plugin depends on the public
    platform surface only (catalog rule 9: no private core imports).
    """
    stripped = ref.strip()
    return (stripped, None) if stripped else None


PLATFORM_NAME = "mqtt"
MAX_MESSAGE_LENGTH = 262_144  # MQTT has no practical payload cap; keep replies in one message
DEFAULT_SUBSCRIBE = "events/#"
DEFAULT_PUBLISH = "hermes/out"
DEFAULT_DELIVER_TEMPLATE = "{topic}: {summary}"
DEFAULT_AGENT_TEMPLATE = "MQTT event on {mqtt_topic} from {source} at {occurred_at}:\n{json}"
DEFAULT_JOB_TEMPLATE = "{topic}: {summary}"
MODES = ("deliver", "agent", "job", "drop")
DEDUP_TTL_SECONDS = 3600
DEDUP_MAX_ENTRIES = 4096
RECONNECT_BACKOFF = (2, 5, 15, 30, 60)
#: Inbound packets larger than this are refused before a byte of body is buffered. MQTT
#: itself allows up to 256 MiB, but a house event bus never needs that and a broken
#: broker's remaining length must not turn into an allocation request.
DEFAULT_MAX_PACKET_BYTES = 16 * 1024 * 1024
#: Rendered prompts/deliveries are capped: a bus payload is data, not a licence to spend
#: a 16 MiB prompt (or trip a platform's message limit) on one event.
DEFAULT_MAX_RENDER_CHARS = 65_536
#: Burst coalescing is opt-in per topic policy. A bucket is one pending burst for one
#: matched prefix; the map is bounded so a wide/fast topic space cannot grow it forever
#: (on overflow the oldest bucket is flushed early, never dropped).
MAX_COALESCE_BUCKETS = 256
DEFAULT_COALESCE_MAX = 25
MAX_COALESCE_WINDOW = 3600.0
MAX_COALESCE_BATCH = 1000
COALESCE_MODES = ("agent", "job")
MAX_LOGGED_DROPS = 1024
MAX_DEDUPE_KEY_CHARS = 256
TLS_SCHEMES = ("mqtts", "ssl", "tls")
PLAINTEXT_SCHEMES = ("tcp", "mqtt")
DEFAULT_PORT = 1883
DEFAULT_TLS_PORT = 8883


@dataclass(frozen=True)
class BrokerUrl:
    """A parsed broker endpoint; ``tls`` is decided by the URL scheme alone."""

    host: str
    port: int
    tls: bool = False


def _bounded_event_id(event_id: str) -> str:
    """Hash an oversized ``event_id`` so the dedupe cache never stores payload-sized keys."""
    if len(event_id) <= MAX_DEDUPE_KEY_CHARS:
        return event_id
    return "id:" + hashlib.blake2b(event_id.encode("utf-8"), digest_size=16).hexdigest()


_TRUE_FLAGS = frozenset({"1", "true", "yes", "on"})
_FALSE_FLAGS = frozenset({"", "0", "false", "no", "off"})


def _parse_flag(value: Any, *, name: str) -> bool:
    """Strict boolean for security-relevant flags: bool or a well-known string only.

    ``_as_bool`` is deliberately permissive for display-style switches, but a value like
    YAML ``2`` or a mapping must fail closed instead of silently becoming True — especially
    for ``tls_insecure``.
    """
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, str):
        text = value.strip().lower()
        if text in _TRUE_FLAGS:
            return True
        if text in _FALSE_FLAGS:
            return False
    raise ValueError(f"{name} must be a boolean (got {value!r})")


def _as_bool(value: Any, default: bool = False) -> bool:
    """Parse a config flag the way YAML/env can hand it over (``"false"`` is False)."""
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes", "on")
    return bool(value)


def is_valid_topic_filter(topic_filter: str) -> bool:
    """True for a syntactically valid MQTT subscribe filter.

    Wildcards are filter-only and must occupy a whole level; ``#`` must be the final
    level. A malformed filter makes a broker drop the connection, which would
    otherwise look like a transient failure and retry forever.
    """
    if not topic_filter or "\x00" in topic_filter or len(topic_filter) > 65535:
        return False
    levels = topic_filter.split("/")
    for index, level in enumerate(levels):
        if "#" in level and (level != "#" or index != len(levels) - 1):
            return False
        if "+" in level and level != "+":
            return False
    return True



# --- pure helpers (unit-tested without a broker) ---------------------------------


def parse_url(url: str) -> BrokerUrl:
    """``mqtts://host[:8883]`` / ``tcp://host[:1883]`` → :class:`BrokerUrl`.

    The scheme is the only TLS switch (``tls://`` and ``ssl://`` are aliases for
    ``mqtts://``). There is deliberately no way to enable TLS on a ``tcp://`` URL, and
    the client never downgrades a failed TLS handshake to plaintext. Credentials are
    never accepted in the URL — use the environment.
    """
    raw = (url or "").strip()
    if not raw:
        raise ValueError("empty MQTT url")
    if "://" not in raw:
        raw = f"tcp://{raw}"
    parsed = urlparse(raw)
    if parsed.username or parsed.password:
        raise ValueError(
            "credentials in the MQTT url are not supported; use MQTT_HERMES_USER/_PASSWORD")
    if parsed.scheme in TLS_SCHEMES:
        tls, default_port = True, DEFAULT_TLS_PORT
    elif parsed.scheme in PLAINTEXT_SCHEMES:
        tls, default_port = False, DEFAULT_PORT
    else:
        raise ValueError(
            f"unsupported MQTT scheme {parsed.scheme!r} "
            "(expected mqtts://, tcp://, mqtt://, tls:// or ssl://)")
    if not parsed.hostname:
        raise ValueError(f"no host in MQTT url {url!r}")
    return BrokerUrl(host=parsed.hostname, port=parsed.port or default_port, tls=tls)


def coerce_policy(value: Any) -> dict:
    """Accept ``"agent"``, ``{mode: agent}`` or ``{mode: deliver, ...}``; unknown modes drop."""
    if isinstance(value, str):
        value = {"mode": value}
    if not isinstance(value, dict):
        return {"mode": "drop"}
    policy = dict(value)
    mode = str(policy.get("mode") or "drop").strip().lower()
    if mode not in MODES:
        logger.warning("[mqtt] unknown mode %r in topic policy — dropping instead", mode)
        mode = "drop"
    policy["mode"] = mode
    return policy


def resolve_policy(topic: str, table: dict, default: dict) -> tuple[dict, str]:
    """Longest matching prefix wins; returns ``(policy, matched_prefix)``."""
    best, best_prefix = None, ""
    for prefix, value in (table or {}).items():
        name = str(prefix)
        if topic == name or topic.startswith(name):
            if len(name) >= len(best_prefix):
                best, best_prefix = coerce_policy(value), name
    if best is None:
        return coerce_policy(default), ""
    return best, best_prefix


class _TemplateValues(dict):
    """Unknown placeholders render as empty text instead of raising mid-event."""

    def __missing__(self, key: str) -> str:  # noqa: D105
        return ""


def render(template: str, values: dict) -> str:
    try:
        return str(template).format_map(_TemplateValues(values)).strip()
    except Exception:  # malformed template (stray brace) — never lose the event over it
        return str(values.get("summary") or values.get("json") or "").strip()


# --- burst coalescing config (parsed once at construction; never raises) -----------


def _coalesce_seconds(value: Any) -> tuple[float, str]:
    """Parse a ``coalesce`` window. Returns ``(seconds, warning)``; 0 = coalescing off.

    Config is data: a typo must clamp and warn, never take the adapter down at startup.
    """
    if value is None or (isinstance(value, str) and not value.strip()):
        return 0.0, ""
    if isinstance(value, bool):
        return 0.0, f"coalesce must be a number of seconds, got {value!r}; coalescing disabled"
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return 0.0, f"coalesce must be a number of seconds, got {value!r}; coalescing disabled"
    if not math.isfinite(seconds):
        return 0.0, f"coalesce must be a finite number of seconds, got {value!r}; coalescing disabled"
    if seconds <= 0:
        # 0 is the documented "off" value; a negative is a config mistake but the safe
        # reading is the same, so clamp it and say so once.
        if seconds < 0:
            return 0.0, f"coalesce {value!r} is below 0; using 0 (coalescing disabled)"
        return 0.0, ""
    if seconds > MAX_COALESCE_WINDOW:
        return MAX_COALESCE_WINDOW, f"coalesce {value!r} exceeds {MAX_COALESCE_WINDOW:g}s; clamped"
    return seconds, ""


def _coalesce_batch_limit(value: Any) -> tuple[int, str]:
    """Parse ``coalesce_max``: default 25, clamped to 2..1000. Returns ``(limit, warning)``."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return DEFAULT_COALESCE_MAX, ""
    if isinstance(value, bool):
        return DEFAULT_COALESCE_MAX, (
            f"coalesce_max must be an integer >= 2, got {value!r}; "
            f"using {DEFAULT_COALESCE_MAX}")
    try:
        limit = int(value)
    except (TypeError, ValueError):
        return DEFAULT_COALESCE_MAX, (
            f"coalesce_max must be an integer >= 2, got {value!r}; "
            f"using {DEFAULT_COALESCE_MAX}")
    if limit < 2:
        return 2, f"coalesce_max {value!r} is below 2; clamped to 2"
    if limit > MAX_COALESCE_BATCH:
        return MAX_COALESCE_BATCH, f"coalesce_max {value!r} exceeds {MAX_COALESCE_BATCH}; clamped"
    return limit, ""


def _coalesce_table(topics: Any, default: Any) -> dict[str, tuple[float, int]]:
    """Build ``prefix -> (window, max_events)`` for policies that opted into coalescing.

    Built once at construction so an invalid value logs exactly one warning when the
    adapter is created (and never on the hot path). A default policy has no prefix and is
    keyed under ``""`` — it still batches every topic that falls through to it, because
    they all share that one policy.
    """
    table: dict[str, tuple[float, int]] = {}
    entries: list[tuple[str, Any]] = []
    if isinstance(topics, dict):
        entries.extend((str(prefix), value) for prefix, value in topics.items())
    if default is not None:
        entries.append(("", default))
    for prefix, raw in entries:
        if isinstance(raw, str):
            policy: dict = {"mode": raw}
        elif isinstance(raw, dict):
            policy = raw
        else:
            continue
        mode = str(policy.get("mode") or "drop").strip().lower()
        if mode not in COALESCE_MODES:  # deliver/drop keep their per-message shape
            continue
        window, window_warning = _coalesce_seconds(policy.get("coalesce"))
        if window <= 0:
            # coalesce_max is ignored when coalescing is off: do not validate or warn on it.
            if window_warning:
                logger.warning("[mqtt] topic policy %r: %s", prefix or "default", window_warning)
            continue
        max_events, max_warning = _coalesce_batch_limit(policy.get("coalesce_max"))
        for warning in (window_warning, max_warning):
            if warning:
                logger.warning("[mqtt] topic policy %r: %s", prefix or "default", warning)
        table[prefix] = (window, max_events)
    return table


def envelope(*, mqtt_topic: str, payload: bytes) -> dict:
    """Normalise a bus payload into the fields the router and templates use.

    The bus guarantees ``source``/``event_id``/``occurred_at``/``topic`` and passes the
    producer's body through untouched; everything else is best-effort. A non-JSON body
    is still routable — it becomes ``text``.
    """
    data: dict = {}
    try:
        decoded = json.loads(payload.decode("utf-8"))
        if isinstance(decoded, dict):
            data = decoded
        else:
            data = {"body": decoded}
    except (UnicodeDecodeError, json.JSONDecodeError):
        data = {"text": payload.decode("utf-8", "replace")}
    body = {k: v for k, v in data.items() if k not in ("event_id", "occurred_at", "source", "topic")}
    try:
        compact = json.dumps(body, ensure_ascii=False, separators=(",", ":"))
    except (TypeError, ValueError):
        compact = repr(body)
    summary = data.get("summary")
    if summary is None:
        text = data.get("text") or data.get("message")
        if not (isinstance(text, str) and text.strip()):
            # A JSON scalar payload (e.g. ``"hello"``) is wrapped in ``body``; use it
            # directly rather than making the caller read ``{"body":"hello"}``.
            text = data.get("body") if isinstance(data.get("body"), str) else None
        summary = text if isinstance(text, str) and text.strip() else compact[:500]
    return {
        "mqtt_topic": mqtt_topic,
        "topic": str(data.get("topic") or mqtt_topic),
        "source": str(data.get("source") or "unknown"),
        "event_id": str(data.get("event_id") or ""),
        "occurred_at": str(data.get("occurred_at") or ""),
        "summary": str(summary).strip(),
        "json": compact,
        "data": body,
    }


class _Dedup(MessageDeduplicator):
    """Bounded TTL set keyed by ``event_id``; MQTT is at-least-once and the bus retries.

    Subclassing the host helper is deliberate: the gateway rebuilds an adapter after a
    fatal reconnect and carries inbound dedupe via
    ``gateway.platforms.helpers.carry_inbound_dedup``, which only adopts
    ``MessageDeduplicator`` instances. A hand-rolled cache would start empty and could
    re-deliver events the previous instance already answered.
    """

    def __init__(self, ttl: int = DEDUP_TTL_SECONDS, max_entries: int = DEDUP_MAX_ENTRIES) -> None:
        super().__init__(max_size=max_entries, ttl_seconds=ttl)

    def seen(self, key: str) -> bool:
        """True if already handled; records the key otherwise."""
        return self.is_duplicate(key)


@dataclass
class _CoalesceBucket:
    """One pending burst: every buffered event for a single matched policy prefix."""

    prefix: str
    mode: str
    policy: dict
    window: float
    max_events: int
    values: list[dict] = field(default_factory=list)
    timer: Optional[asyncio.TimerHandle] = None


# --- adapter ---------------------------------------------------------------------


class MqttAdapter(BasePlatformAdapter):
    """Consumes the house MQTT bus; publishes replies to its own outbound topic."""

    MAX_MESSAGE_LENGTH = MAX_MESSAGE_LENGTH
    supports_code_blocks = False
    #: ``send()`` takes the full payload: MQTT bounds a packet by the broker/client caps,
    #: not by a chat window, so the gateway's cron truncation must not apply.
    splits_long_messages = True
    #: Nobody is on the other end of a topic to answer "what next?" — resume is meaningless.
    interactive_resume = False

    def __init__(self, config: PlatformConfig, ctx: Any = None) -> None:
        super().__init__(config=config, platform=_platform())
        extra = dict(config.extra or {})
        self._ctx = ctx
        self._url = str(extra.get("url") or _get_scoped_secret("MQTT_HERMES_URL", "") or "")
        self._host, self._port, self._tls = "", DEFAULT_PORT, False
        if self._url:
            try:
                broker = parse_url(self._url)
            except ValueError as exc:
                logger.error("[mqtt] bad url: %s", exc)
            else:
                self._host, self._port, self._tls = broker.host, broker.port, broker.tls
        self._username = str(extra.get("username") or _get_scoped_secret("MQTT_HERMES_USER", "") or "hermes")
        self._client_id = str(extra.get("client_id") or f"hermes-{socket.gethostname()[:23]}")
        self._subscribe = str(extra.get("subscribe") or DEFAULT_SUBSCRIBE)
        self._protocol = normalize_protocol(extra.get("protocol"))
        self._tls_ca_file = self._tls_setting("tls_ca_file", "MQTT_HERMES_TLS_CA_FILE")
        self._tls_cert_file = self._tls_setting("tls_cert_file", "MQTT_HERMES_TLS_CERT_FILE")
        self._tls_key_file = self._tls_setting("tls_key_file", "MQTT_HERMES_TLS_KEY_FILE")
        self._tls_key_password = self._tls_setting(
            "tls_key_password", "MQTT_HERMES_TLS_KEY_PASSWORD")
        self._tls_server_name = self._tls_setting(
            "tls_server_name", "MQTT_HERMES_TLS_SERVER_NAME")
        self._tls_insecure = False
        self._tls_config_error = ""
        try:
            self._tls_insecure = _parse_flag(
                extra["tls_insecure"] if "tls_insecure" in extra
                else _get_scoped_secret("MQTT_HERMES_TLS_INSECURE", ""),
                name="tls_insecure",
            )
        except ValueError as exc:
            self._tls_config_error = str(exc)
        self._warned_insecure = False
        if not self._tls and any((
            self._tls_ca_file, self._tls_cert_file, self._tls_key_file,
            self._tls_server_name, self._tls_insecure, self._tls_config_error,
        )):
            logger.warning(
                "[mqtt] TLS settings are configured but the url is not mqtts:// — ignoring them")
        self._accept_retained = _as_bool(extra.get("accept_retained", False))
        self._publish_topic = str(extra.get("publish_topic") or DEFAULT_PUBLISH)
        self._topics = extra.get("topics") or {}
        self._default = extra.get("default") or {"mode": "drop"}
        # Parsed once here so a bad value clamps + warns at construction, never on an event.
        self._coalesce_table = _coalesce_table(self._topics, self._default)
        self._coalesce_buckets: "OrderedDict[str, _CoalesceBucket]" = OrderedDict()
        self._concurrency = max(1, int(extra.get("max_concurrent") or 4))
        try:
            max_packet = int(extra.get("max_packet_bytes") or DEFAULT_MAX_PACKET_BYTES)
        except (TypeError, ValueError):
            max_packet = DEFAULT_MAX_PACKET_BYTES
        self._max_packet_bytes = max_packet if max_packet > 0 else DEFAULT_MAX_PACKET_BYTES
        try:
            max_render = int(extra.get("max_render_chars") or DEFAULT_MAX_RENDER_CHARS)
        except (TypeError, ValueError):
            max_render = DEFAULT_MAX_RENDER_CHARS
        self._max_render_chars = max_render if max_render > 0 else DEFAULT_MAX_RENDER_CHARS
        self._dedup = _Dedup()
        self._session: Optional[MqttSession] = None
        self._supervisor: Optional[asyncio.Task] = None
        self._dispatcher: Optional[asyncio.Task] = None
        self._inbox: asyncio.Queue = asyncio.Queue(maxsize=1000)
        self._fatal = False
        self._logged_drops: set[str] = set()
        self._counts = {mode: 0 for mode in MODES}
        self._counts["errors"] = 0
        self._counts["retained_skipped"] = 0
        self._counts["coalesced_messages"] = 0

    # -- config -------------------------------------------------------------

    @property
    def _password(self) -> str:
        return str((self.config.extra or {}).get("password") or _get_scoped_secret("MQTT_HERMES_PASSWORD", "") or "")

    def _tls_setting(self, key: str, env: str) -> str:
        """One TLS setting: explicit extra value, else the profile's env, never a log."""
        extra = self.config.extra or {}
        value = extra.get(key)
        if value is None or str(value).strip() == "":
            value = _get_scoped_secret(env, "")
        return str(value or "").strip()

    def _subscription_prefix(self) -> str:
        """The literal, wildcard-free head of the configured filter (``events/#`` → ``events``)."""
        cut = len(self._subscribe)
        for mark in ("+", "#"):
            index = self._subscribe.find(mark)
            if index != -1:
                cut = min(cut, index)
        return self._subscribe[:cut].rstrip("/")

    def _is_own_topic(self, topic: str) -> bool:
        """Guard against consuming our own replies if someone widens the subscription.

        Only the publish topic and its *descendants* are ours. A sibling such as
        ``hermes/state`` is not, even though it shares a parent segment: dropping it
        would silently swallow unrelated bus traffic.
        """
        own = self._publish_topic.rstrip("/")
        return topic == own or topic.startswith(f"{own}/")

    def _policy_for(self, values: dict) -> tuple[dict, str]:
        """Match on the envelope topic and on the wire topic; longest prefix wins."""
        candidates = [values["topic"]]
        wire = values["mqtt_topic"]
        if wire not in candidates:
            candidates.append(wire)
            prefix = self._subscription_prefix()
            if prefix and wire.startswith(f"{prefix}/"):
                trimmed = wire[len(prefix) + 1:]
                if trimmed not in candidates:
                    candidates.append(trimmed)
        best, best_prefix = {"mode": "drop"}, ""
        for candidate in candidates:
            policy, prefix = resolve_policy(candidate, self._topics, self._default)
            if prefix and len(prefix) > len(best_prefix):
                best, best_prefix = policy, prefix
        if not best_prefix:  # no topic matched → the configured default, not a guess
            best = coerce_policy(self._default)
        return best, best_prefix

    # -- lifecycle ----------------------------------------------------------

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        # A second connect must not spawn a second consumer pair: both loops would drain the
        # same inbox and one inbound event could be routed twice. The host only calls connect
        # again after a fatal path has torn the adapter down, so a live supervisor is a no-op.
        if self._running and self._supervisor is not None and not self._supervisor.done():
            logger.warning("[mqtt] connect() called while already connected — ignoring")
            return True
        await self._stop_tasks()
        self._fatal = False  # a new attempt gets a fresh verdict
        if not self._host:
            logger.error("[mqtt] no url configured (platforms.mqtt.extra.url or MQTT_HERMES_URL)")
            self._set_fatal_error("mqtt_no_url", "MQTT has no broker url configured.", retryable=False)
            return False
        if not self._password:
            logger.error("[mqtt] MQTT_HERMES_PASSWORD is empty — the broker will refuse us")
            self._set_fatal_error(
                "mqtt_no_password",
                "MQTT_HERMES_PASSWORD is empty; add the broker password for user hermes to .env.",
                retryable=False,
            )
            return False
        # The host passes is_reconnect=True when re-granting a platform that failed
        # before; TLS/auth failures in that path must stay retryable so a renewed cert
        # or fixed credential can recover, while a first connect reports them as fatal.
        session = await self._open(retrying=is_reconnect)
        if session is None:
            return False
        self._dispatcher = asyncio.create_task(self._dispatch_loop(), name="mqtt-dispatch")
        self._supervisor = asyncio.create_task(self._supervise(session), name="mqtt-supervise")
        return True

    async def _stop_tasks(self) -> None:
        """Cancel the consumer pair and close the session, without touching connection flags."""
        for task in (self._supervisor, self._dispatcher):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
        self._supervisor = self._dispatcher = None
        while True:  # drop anything the previous consumer had not routed yet
            try:
                self._inbox.get_nowait()
            except asyncio.QueueEmpty:
                break
        if self._session is not None:
            await self._session.aclose()
            self._session = None

    async def _open(self, *, retrying: bool = False) -> Optional[MqttSession]:
        if not is_valid_topic_filter(self._subscribe):
            self._fatal = True
            logger.error("[mqtt] invalid subscribe filter %r — fix platforms.mqtt.extra.subscribe", self._subscribe)
            self._set_fatal_error(
                "mqtt_bad_subscribe_filter",
                f"invalid MQTT topic filter {self._subscribe!r}",
                retryable=False,
            )
            return None
        ssl_context = None
        server_hostname = None
        if self._tls:
            if self._tls_config_error:
                self._fatal = True
                logger.error("[mqtt] TLS configuration rejected: %s", self._tls_config_error)
                self._set_fatal_error(
                    "mqtt_tls_failed", f"TLS configuration: {self._tls_config_error}",
                    retryable=retrying)
                return None
            try:
                ssl_context = build_tls_context(
                    insecure=self._tls_insecure,
                    ca_file=self._tls_ca_file,
                    cert_file=self._tls_cert_file,
                    key_file=self._tls_key_file,
                    key_password=self._tls_key_password or None,
                )
            except ValueError as exc:
                # Deterministic config problem: report it, never retry a broken chain.
                self._fatal = True
                logger.error("[mqtt] TLS configuration rejected: %s", exc)
                self._set_fatal_error(
                    "mqtt_tls_failed", f"TLS configuration: {exc}", retryable=retrying)
                return None
            server_hostname = self._tls_server_name or self._host
            if self._tls_insecure and not self._warned_insecure:
                self._warned_insecure = True
                logger.warning(
                    "[mqtt] TLS certificate verification is DISABLED (tls_insecure) — "
                    "only use this on a transport you already trust")
        try:
            session = await MqttSession.open(
                self._host, self._port,
                client_id=self._client_id, username=self._username, password=self._password,
                protocol=self._protocol, max_inbound_packet=self._max_packet_bytes,
                ssl_context=ssl_context, server_hostname=server_hostname,
            )
        except MqttTlsError as exc:
            self._fatal = True
            logger.error("[mqtt] TLS connection to %s:%s failed: %s", self._host, self._port, exc)
            # Initial connect: report and stop. Reconnect: keep it retryable so a renewed
            # certificate / late-mounted secret can recover without a gateway restart.
            # A lost CONNACK over TLS is ambiguous (rejected identity vs a flaky peer), so
            # it stays retryable even on the first connect; record-level TLS errors don't.
            ambiguous = isinstance(exc, MqttTlsLost)
            self._set_fatal_error(
                "mqtt_tls_failed", str(exc), retryable=retrying or ambiguous)
            return None
        except MqttAuthError as exc:
            self._fatal = True
            logger.error("[mqtt] authentication refused: %s", exc)
            # Initial connect: report and stop. Reconnect: keep it retryable so the
            # gateway watches for the credential being fixed instead of giving up.
            self._set_fatal_error("mqtt_auth_failed", str(exc), retryable=retrying)
            return None
        except (MqttFatal, ProtocolUnsupported) as exc:
            self._fatal = True
            logger.error("[mqtt] broker refused the connection: %s", exc)
            self._set_fatal_error("mqtt_connect_refused", str(exc), retryable=False)
            return None
        except (OSError, MqttError, asyncio.TimeoutError) as exc:
            logger.warning("[mqtt] connect to %s:%s failed: %s", self._host, self._port, exc)
            return None
        try:
            granted = await session.subscribe(self._subscribe, qos=1)
        except asyncio.CancelledError:
            await session.aclose()
            raise
        except SubscriptionRefused as exc:
            # A bad filter or an ACL refusal is deterministic — reconnecting cannot fix it.
            self._fatal = True
            logger.error("[mqtt] broker refused subscription %r: %s", self._subscribe, exc)
            self._set_fatal_error("mqtt_subscribe_refused", str(exc), retryable=False)
            await session.aclose()
            return None
        except (MqttError, OSError, asyncio.TimeoutError) as exc:
            # A missing/slow SUBACK or a socket drop is transient: retry instead of going deaf.
            logger.warning("[mqtt] subscribe %r failed transiently: %s — will retry", self._subscribe, exc)
            await session.aclose()
            return None
        except Exception as exc:  # noqa: BLE001 - a malformed configured filter cannot be retried
            self._fatal = True
            logger.error("[mqtt] subscribe %r failed (config): %s", self._subscribe, exc)
            self._set_fatal_error("mqtt_subscribe_failed", str(exc), retryable=False)
            await session.aclose()
            return None
        self._session = session
        self._mark_connected()
        transport = "TCP"
        if self._tls:
            transport = "TLS (verification disabled)" if self._tls_insecure else "TLS"
        logger.info(
            "[mqtt] connected to %s:%s as %s over %s, subscribed %s (qos %s), publishing to %s (mqtt %s)",
            self._host, self._port, self._username, transport, self._subscribe, granted,
            self._publish_topic, "5.0" if session.protocol == 5 else "3.1.1",
        )
        return session

    async def _supervise(self, session: MqttSession) -> None:
        """Own the connection for the adapter's lifetime: consume, then reconnect with backoff."""
        attempt = 0
        while self._running and not self._fatal:
            started = time.monotonic()
            try:
                async for topic, payload, retained in session.messages():
                    if not self._running:
                        break
                    try:
                        self._inbox.put_nowait((topic, payload, retained))
                    except asyncio.QueueFull:
                        self._counts["errors"] += 1
                        logger.warning("[mqtt] inbound queue full — dropped one message on %s", topic)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - any read failure means reconnect
                if self._running:
                    logger.warning("[mqtt] session dropped: %s", exc)
            finally:
                await session.aclose()
                self._session = None
            if not self._running or self._fatal:
                break
            # Deliberately NOT self._mark_disconnected(): the base implementation also sets
            # self._running = False, the same flag this supervisor loops on, so calling it would end
            # the reconnect loop after the first drop and leave the adapter dead until a gateway
            # restart. Publish the status without touching the flag.
            self._write_runtime_status_safe(
                "disconnected", platform_state="disconnected", error_code=None, error_message=None)
            if time.monotonic() - started >= 60:
                attempt = 0
            session = await self._await_retry(attempt)
            attempt += 1  # next drop in this streak starts one step further along
            if session is None:
                break
        if self._fatal:
            # A fatal reconnect would otherwise end the consumer silently while the gateway
            # still believes the platform is up. Flush pending bursts first (one action per
            # burst is the point of coalescing), then report the verdict.
            await self._flush_all(reason="fatal")
            logger.error("[mqtt] consumer stopped after a fatal reconnect error")
            await self._notify_fatal_error()

    async def _await_retry(self, attempt: int) -> Optional[MqttSession]:
        """Backoff between failed reconnect attempts, looped by the supervisor."""
        while self._running and not self._fatal:
            delay = RECONNECT_BACKOFF[min(attempt, len(RECONNECT_BACKOFF) - 1)]
            delay *= 1 + random.random() * 0.25
            attempt += 1
            logger.info("[mqtt] reconnecting in %.1fs", delay)
            await asyncio.sleep(delay)
            if (session := await self._open(retrying=True)) is not None:
                return session
        return None

    async def disconnect(self) -> None:
        self._running = False
        # Flush before anything reports the platform down: an operator reading
        # "disconnected" must already have the burst that was pending when we stopped.
        await self._flush_all(reason="disconnect")
        self._mark_disconnected()
        await self._stop_tasks()
        # A handler already in flight when the dispatcher was stopped can still append a
        # bucket after the first drain; drain once more so shutdown never strands a batch.
        await self._flush_all(reason="shutdown")
        for task in list(self._background_tasks):
            if task.done():  # timer/flush tasks that finished; never report them as live
                self._background_tasks.discard(task)
        # Deliberately keep ``_dedup``: the host can rebuild this adapter after a fatal
        # reconnect and carries the live IDs over; the TTL bounds staleness.
        logger.info("[mqtt] disconnected (modes seen: %s)", self._counts)

    # -- inbound ------------------------------------------------------------

    async def _dispatch_loop(self) -> None:
        """Drain the inbox with bounded concurrency so one slow turn cannot stall the socket."""
        slots = asyncio.Semaphore(self._concurrency)
        while True:
            topic, payload, retained = await self._inbox.get()
            await slots.acquire()

            async def run(topic=topic, payload=payload, retained=retained) -> None:
                try:
                    await self._handle(topic, payload, retained)
                except asyncio.CancelledError:
                    raise
                except Exception:  # noqa: BLE001 - a poison message must not kill the consumer
                    self._counts["errors"] += 1
                    logger.exception("[mqtt] handling %s failed", topic)
                finally:
                    slots.release()

            task = asyncio.create_task(run())
            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)

    async def _handle(self, mqtt_topic: str, payload: bytes, retained: bool = False) -> None:
        if self._is_own_topic(mqtt_topic):
            logger.debug("[mqtt] ignoring own outbound topic %s", mqtt_topic)
            return
        if retained and not self._accept_retained:
            self._counts["retained_skipped"] += 1
            logger.info("[mqtt] ignoring retained message on %s (accept_retained=false)", mqtt_topic)
            return
        values = envelope(mqtt_topic=mqtt_topic, payload=payload)
        policy, prefix = self._policy_for(values)
        mode = policy["mode"]

        event_id = values["event_id"]
        if event_id and self._dedup.seen(_bounded_event_id(event_id)):
            # Content fingerprints would drop legitimate repeats (a periodic sensor
            # publishing the same body); only a producer-supplied id is a safe key.
            logger.debug("[mqtt] duplicate event %s on %s — skipped", event_id, mqtt_topic)
            return

        # Coalescing is opt-in per policy and only for the two expensive modes. A duplicate
        # was already filtered above, so it can never inflate a batch or its counter.
        settings = self._coalesce_table.get(prefix) if mode in COALESCE_MODES else None
        if settings is not None:
            window, max_events = settings
            self._counts["coalesced_messages"] += 1
            await self._buffer_event(
                prefix=prefix, mode=mode, policy=policy,
                window=window, max_events=max_events, values=values,
            )
            return

        self._counts[mode] = self._counts.get(mode, 0) + 1
        if mode == "drop":
            key = prefix or mqtt_topic
            if key not in self._logged_drops:  # once per topic, not once per message
                if len(self._logged_drops) >= MAX_LOGGED_DROPS:
                    # A wide topic space must not grow this set forever; clearing re-logs
                    # the first occurrences after the cap is hit.
                    self._logged_drops.clear()
                self._logged_drops.add(key)
                logger.info(
                    "[mqtt] dropping %s (matched %s, mode=drop) — configure platforms.mqtt.extra.topics to act on it",
                    mqtt_topic, prefix or "no prefix",
                )
            return
        if mode == "deliver":
            await self._deliver_mode(values, policy)
        elif mode == "agent":
            await self._agent_mode(values, mqtt_topic, policy, prefix)
        elif mode == "job":
            await self._job_mode(values, mqtt_topic, policy)

    def _truncate(self, text: str) -> str:
        """Cap rendered text so one oversized payload cannot blow up a turn or a send."""
        limit = self._max_render_chars
        if len(text) <= limit:
            return text
        return text[:limit] + f"\n…[truncated {len(text) - limit} chars]"

    def _template_for(self, policy: dict, mode: str) -> str:
        """The existing per-mode default template, or the policy's own ``template``."""
        return policy.get("template") or (
            DEFAULT_AGENT_TEMPLATE if mode == "agent"
            else DEFAULT_JOB_TEMPLATE if mode == "job"
            else DEFAULT_DELIVER_TEMPLATE
        )

    def _render(self, values: dict, policy: dict, mode: str) -> str:
        return self._truncate(render(self._template_for(policy, mode), values))

    # -- coalescing ---------------------------------------------------------

    @staticmethod
    def _window_label(window: float) -> str:
        return f"{window:g}"

    def _render_event(self, values: dict, policy: dict, mode: str) -> str:
        """One event's existing template render (the same text single-event mode sends)."""
        return render(self._template_for(policy, mode), values)

    def _render_batch(
        self, prefix: str, window: float, policy: dict, mode: str, values_list: list[dict],
    ) -> str:
        """Render one action's text for a burst, capped at ``max_render_chars``.

        The header is literal and every event keeps its existing per-mode rendering. When
        the whole batch does not fit, complete event blocks are kept in arrival order and
        the remaining count is named — a truncated batch still acts instead of losing it.
        """
        label = prefix or "default"
        header = f"{len(values_list)} events on {label} in {self._window_label(window)}s"
        blocks = [self._render_event(values, policy, mode) for values in values_list]
        limit = self._max_render_chars
        full = header + "\n" + "\n\n".join(blocks)
        if len(full) <= limit:
            return full
        shown = 0
        body = header[:limit]
        for block in blocks:
            candidate = body + ("\n" if shown == 0 else "\n\n") + block
            if len(candidate) > limit:
                break
            body = candidate
            shown += 1
        return body + f"\n… truncated: {len(blocks) - shown} more events not shown"

    async def _buffer_event(
        self, *, prefix: str, mode: str, policy: dict,
        window: float, max_events: int, values: dict,
    ) -> None:
        """Append to the prefix's bucket; flush now on bucket overflow or a full batch."""
        bucket = self._coalesce_buckets.get(prefix)
        if bucket is None:
            if len(self._coalesce_buckets) >= MAX_COALESCE_BUCKETS:
                # Bounded buffer: never drop silently — flush the oldest burst early.
                _, oldest = next(iter(self._coalesce_buckets.items()))
                logger.warning(
                    "[mqtt] coalesce bucket limit (%d) reached — flushing oldest bucket on %s early",
                    MAX_COALESCE_BUCKETS, oldest.prefix or "default")
                await self._flush_bucket(oldest, reason="overflow")
            bucket = _CoalesceBucket(
                prefix=prefix, mode=mode, policy=policy,
                window=window, max_events=max_events)
            self._coalesce_buckets[prefix] = bucket
            loop = asyncio.get_running_loop()
            bucket.timer = loop.call_later(
                window, self._schedule_coalesce_flush, prefix)
        bucket.values.append(values)
        if len(bucket.values) >= bucket.max_events:
            await self._flush_bucket(bucket, reason="full")

    def _schedule_coalesce_flush(self, prefix: str) -> None:
        """Window timer fired: dispatch the bucket in a tracked task (never inline)."""
        bucket = self._coalesce_buckets.get(prefix)
        if bucket is None or not bucket.values:
            return
        task = asyncio.create_task(
            self._flush_bucket(bucket, reason="window"),
            name=f"mqtt-coalesce-{prefix or 'default'}")
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    async def _flush_bucket(
        self, bucket: _CoalesceBucket, *, reason: str = "window",
    ) -> None:
        """Dispatch one burst as one action; failures count once per batch, never per event."""
        if bucket.timer is not None:
            bucket.timer.cancel()
            bucket.timer = None
        if self._coalesce_buckets.get(bucket.prefix) is bucket:
            del self._coalesce_buckets[bucket.prefix]
        values = bucket.values
        bucket.values = []
        if not values:  # already flushed (timer raced a full/overflow flush)
            return
        count, mode, prefix = len(values), bucket.mode, bucket.prefix
        self._counts[mode] = self._counts.get(mode, 0) + 1
        logger.info(
            "[mqtt] coalesced %d messages on %s → %s", count, prefix or "default", mode)
        text = self._render_batch(prefix, bucket.window, bucket.policy, mode, values)
        batch_values = dict(values[0])
        batch_values["_coalesced_text"] = text
        try:
            if mode == "agent":
                await self._agent_mode(
                    batch_values, values[0]["mqtt_topic"], bucket.policy, prefix)
            else:
                dispatched = await self._job_mode(
                    batch_values, values[0]["mqtt_topic"], bucket.policy)
                if dispatched is False:
                    # The sink already counted the error; name the batch size in the log.
                    logger.warning(
                        "[mqtt] coalesced batch of %d messages on %s was refused by the sink",
                        count, prefix or "default")
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - one bad batch must not kill the pump
            self._counts["errors"] += 1
            logger.exception(
                "[mqtt] coalesced batch of %d messages on %s failed",
                count, prefix or "default")

    async def _flush_all(self, *, reason: str) -> None:
        """Flush every pending bucket once, bounded by min(2 s, the longest configured window)."""
        buckets = list(self._coalesce_buckets.values())
        self._coalesce_buckets.clear()
        if not buckets:
            return
        for bucket in buckets:
            if bucket.timer is not None:
                bucket.timer.cancel()
                bucket.timer = None
        cap = min(2.0, max(bucket.window for bucket in buckets))
        tasks: dict[asyncio.Task, int] = {}
        for bucket in buckets:
            if not bucket.values:
                continue
            task = asyncio.create_task(
                self._flush_bucket(bucket, reason=reason),
                name=f"mqtt-coalesce-flush-{bucket.prefix or 'default'}")
            tasks[task] = len(bucket.values)
        if not tasks:
            return
        _, pending = await asyncio.wait(set(tasks), timeout=cap)
        if pending:
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            for task in pending:
                self._counts["errors"] += 1
                logger.warning(
                    "[mqtt] coalesced batch of %d messages did not dispatch within %.1fs "
                    "on %s — batch dropped",
                    tasks[task], cap, reason)

    async def _deliver_mode(self, values: dict, policy: dict) -> None:
        text = self._render(values, policy, "deliver")
        if not text:
            self._counts["errors"] += 1
            logger.warning("[mqtt] deliver policy for %s rendered empty text", values["topic"])
            return
        platform_name = str(policy.get("deliver") or policy.get("platform") or "").strip()
        if not platform_name:
            self._counts["errors"] += 1
            logger.error(
                "[mqtt] deliver policy for %s has no deliver: platform — name a live gateway "
                "platform (telegram, discord, …) or use another mode",
                values["topic"],
            )
            return
        target = {
            "platform": platform_name,
            "chat_id": policy.get("chat_id") or policy.get("deliver_chat_id") or "",
            "profile": policy.get("profile"),
            "thread_id": policy.get("thread_id") or policy.get("message_thread_id"),
        }
        result = await self._deliver(text, target)
        if result.success:
            logger.info("[mqtt] delivered %s → %s:%s", values["topic"], target["platform"], target["chat_id"] or "home")
        else:
            self._counts["errors"] += 1
            logger.warning("[mqtt] delivery of %s failed: %s", values["topic"], result.error)

    async def _agent_mode(
        self, values: dict, mqtt_topic: str, policy: dict, prefix: str = "",
    ) -> None:
        # A coalesced flush pre-renders the whole burst; a single event renders as before.
        text = values.get("_coalesced_text") or self._render(values, policy, "agent")
        if not text:
            return
        # Session identity is configuration, never payload: otherwise a single wildcard
        # policy lets a producer create unbounded sessions by varying the topic.
        chat_id = str(policy.get("chat_id") or prefix or "mqtt")
        source = self.build_source(
            chat_id=chat_id, chat_name=chat_id, chat_type="dm",
            user_id="mqtt", user_name="event-bus", message_id=values["event_id"] or None,
        )
        raw = {key: value for key, value in values.items() if key != "_coalesced_text"}
        event = MessageEvent(
            text=text, message_type=MessageType.TEXT, source=source,
            raw_message=raw, message_id=values["event_id"] or uuid.uuid4().hex,
            timestamp=_timestamp(values["occurred_at"]),
            # Bus payload text is data, never a gateway command: even when a custom template
            # renders it at position 0, "/update" and friends must stay conversational.
            allow_gateway_control=False,
        )
        logger.info("[mqtt] agent turn for %s (chat %s)", values["topic"], chat_id)
        await self.handle_message(event)

    async def _job_mode(self, values: dict, mqtt_topic: str, policy: dict) -> bool:
        job_id = str(policy.get("job_id") or policy.get("job") or "").strip()
        if not job_id:
            self._counts["errors"] += 1
            logger.error("[mqtt] job policy for %s has no job_id", values["topic"])
            return False
        # A coalesced flush passes the pre-rendered batch as the transient run context.
        context = (
            values.get("_coalesced_text")
            or self._render(values, policy, "job")
            or self._truncate(values["json"])
        )
        try:
            from tools.cronjob_tools import execute_job_for_event
        except ImportError:  # the seam moved upstream; fail loudly instead of crashing the pump
            self._counts["errors"] += 1
            logger.error(
                "[mqtt] job policy for %s: tools.cronjob_tools.execute_job_for_event is missing "
                "in this Hermes build — job mode unavailable",
                values["topic"],
            )
            return False
        result = await asyncio.to_thread(execute_job_for_event, job_id, context)
        if result.get("success"):
            logger.info("[mqtt] fired cron job %s for %s", job_id, values["topic"])
            return True
        self._counts["errors"] += 1
        logger.warning(
            "[mqtt] cron job %s for %s did not run (claimed=%s): %s",
            job_id, values["topic"], result.get("claimed"), result.get("error") or "see cron log",
        )
        return False

    async def _deliver(self, text: str, target: dict) -> SendResult:
        """Push text to another platform through ITS live adapter (the webhook route's leg)."""
        platform_name = str(target.get("platform") or "").strip()
        if not platform_name:
            return SendResult(success=False, error="no deliver platform configured")
        if platform_name == "local":
            return SendResult(
                success=False,
                error=(
                    "platform 'local' is not a gateway adapter; deliver: must name a connected "
                    "messaging platform (telegram, discord, …)"
                ),
            )
        try:
            target_platform = Platform(platform_name)
        except ValueError:
            return SendResult(success=False, error=f"unknown platform {platform_name!r}")
        runner = getattr(self, "gateway_runner", None)
        if runner is None:
            return SendResult(success=False, error="no gateway runner for cross-platform delivery")
        profile = target.get("profile") or None
        finder = getattr(runner, "_authorization_adapter", None)
        adapter = finder(target_platform, profile) if finder else None
        if adapter is None:
            return SendResult(success=False, error=f"platform {platform_name!r} is not connected")
        chat_id = str(target.get("chat_id") or "")
        if not chat_id:
            # The routed adapter's own config is the profile-scoped home channel; the runner's
            # config is the DEFAULT profile's, so it is only a fallback for an unprofiled policy.
            home = getattr(getattr(adapter, "config", None), "home_channel", None)
            if home is None and not profile:
                try:
                    home = runner.config.get_home_channel(target_platform)
                except Exception:
                    home = None
            if home is None and profile:
                return SendResult(
                    success=False,
                    error=f"profile {profile!r} has no home channel for {platform_name}; set chat_id",
                )
            chat_id = str(getattr(home, "chat_id", "") or "")
        if not chat_id:
            return SendResult(success=False, error=f"no chat_id or home channel for {platform_name}")
        thread_id = str(target.get("thread_id") or "") or None
        metadata = {"thread_id": thread_id} if thread_id else None
        return await adapter.send(chat_id, text, metadata=metadata)

    # -- outbound -----------------------------------------------------------

    async def send(
        self, chat_id: str, content: str, reply_to: Optional[str] = None, metadata: Optional[dict[str, Any]] = None,
    ) -> SendResult:
        """Publish a reply envelope to ``publish_topic``; ``chat_id`` rides along as ``reply_to``."""
        session = self._session
        if session is None:
            return SendResult(success=False, error="MQTT session is not connected", retryable=True)
        topic = str((metadata or {}).get("publish_topic") or self._publish_topic)
        envelope: dict[str, Any] = {
            "event_id": uuid.uuid4().hex,
            "occurred_at": datetime.now(UTC).isoformat(),
            "source": "hermes",
            "topic": topic,
            "reply_to": chat_id,
            "text": content,
        }
        if reply_to:
            envelope["in_reply_to"] = str(reply_to)
        body = json.dumps(envelope, ensure_ascii=False).encode("utf-8")
        try:
            packet_id = await session.publish(topic, body, qos=1)
        except Exception as exc:  # noqa: BLE001 - surfaced to the caller as a SendResult
            return SendResult(success=False, error=f"MQTT publish failed: {exc}", retryable=True)
        return SendResult(success=True, message_id=f"{topic}:{packet_id}")

    async def get_chat_info(self, chat_id: str) -> dict[str, Any]:
        return {"name": chat_id, "type": "dm"}


def _timestamp(raw: str) -> datetime:
    try:
        value = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return datetime.now(UTC)
    # Producers often omit the offset; a naive timestamp compares wrongly against the
    # gateway's aware ones. Treat it as UTC rather than passing the ambiguity through.
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


# --- registration ----------------------------------------------------------------


def _platform() -> Platform:
    """``Platform('mqtt')`` — the registry member, or the same pseudo-member built by hand.

    In the gateway the plugin has registered before any adapter is constructed. Unit tests and
    standalone imports have no registry yet, and ``_add_pseudo_member`` writes the same cache
    entry ``_missing_`` would, so identity still holds once registration does happen.
    """
    try:
        return Platform(PLATFORM_NAME)
    except ValueError:
        return Platform._add_pseudo_member(PLATFORM_NAME)


def _validate_config(config: PlatformConfig) -> bool:
    extra = getattr(config, "extra", {}) or {}
    url = str(extra.get("url") or _get_scoped_secret("MQTT_HERMES_URL", "") or "")
    if not url:
        return False
    try:
        broker = parse_url(url)
    except ValueError:
        return False
    if broker.tls:
        cert_file = str(
            extra.get("tls_cert_file") or _get_scoped_secret("MQTT_HERMES_TLS_CERT_FILE", "") or ""
        ).strip()
        key_file = str(
            extra.get("tls_key_file") or _get_scoped_secret("MQTT_HERMES_TLS_KEY_FILE", "") or ""
        ).strip()
        if bool(cert_file) != bool(key_file):
            return False  # mutual TLS needs both halves of the identity
        insecure = extra.get("tls_insecure")
        if insecure is None or (isinstance(insecure, str) and not insecure.strip()):
            insecure = _get_scoped_secret("MQTT_HERMES_TLS_INSECURE", "")
        try:
            _parse_flag(insecure, name="tls_insecure")
        except ValueError:
            return False  # fail closed: a typo must never silently disable verification
    publish_topic = str(extra.get("publish_topic") or DEFAULT_PUBLISH)
    if not publish_topic or any(mark in publish_topic for mark in "+#"):
        # A publish topic is a concrete topic; wildcards are filter-only and would make
        # every send() fail at runtime.
        return False
    for key, default in (("max_packet_bytes", DEFAULT_MAX_PACKET_BYTES),
                         ("max_render_chars", DEFAULT_MAX_RENDER_CHARS)):
        try:
            int(extra.get(key) or default)
        except (TypeError, ValueError):
            return False
    subscribe = str(extra.get("subscribe") or DEFAULT_SUBSCRIBE)
    if not is_valid_topic_filter(subscribe):
        return False
    return True


def _is_connected(config: PlatformConfig) -> bool:
    if not _validate_config(config):
        return False
    extra = getattr(config, "extra", {}) or {}
    return bool(extra.get("password") or _get_scoped_secret("MQTT_HERMES_PASSWORD", ""))


def _env_enablement() -> Optional[dict]:
    """Seed ``extra`` from the profile env so an env-only setup still shows in ``gateway status``."""
    url = _get_scoped_secret("MQTT_HERMES_URL", "").strip()
    if not url:
        return None
    seed: dict = {"url": url}
    if user := _get_scoped_secret("MQTT_HERMES_USER", "").strip():
        seed["username"] = user
    if subscribe := _get_scoped_secret("MQTT_SUBSCRIBE", "").strip():
        seed["subscribe"] = subscribe
    if protocol := _get_scoped_secret("MQTT_HERMES_PROTOCOL", "").strip():
        seed["protocol"] = protocol
    if publish := _get_scoped_secret("MQTT_PUBLISH_TOPIC", "").strip():
        seed["publish_topic"] = publish
    # The key password is deliberately not seeded into extra: it is not needed for the
    # status display and stays a runtime scoped secret.
    for key, env in (
        ("tls_ca_file", "MQTT_HERMES_TLS_CA_FILE"),
        ("tls_cert_file", "MQTT_HERMES_TLS_CERT_FILE"),
        ("tls_key_file", "MQTT_HERMES_TLS_KEY_FILE"),
        ("tls_server_name", "MQTT_HERMES_TLS_SERVER_NAME"),
        ("tls_insecure", "MQTT_HERMES_TLS_INSECURE"),
    ):
        if value := _get_scoped_secret(env, "").strip():
            seed[key] = value
    return seed


def register(ctx) -> None:
    """Plugin entry point — called by the Hermes plugin system at startup."""
    ctx.register_platform(
        name=PLATFORM_NAME,
        label="MQTT",
        adapter_factory=lambda cfg: MqttAdapter(cfg, ctx=ctx),
        check_fn=lambda: True,  # stdlib only — nothing to import, nothing to install
        validate_config=_validate_config,
        is_connected=_is_connected,
        required_env=["MQTT_HERMES_URL", "MQTT_HERMES_PASSWORD"],
        install_hint="",
        env_enablement_fn=_env_enablement,
        trusted_inbound=True,  # the authenticated bus publishes; no human sender to allowlist
        max_message_length=MAX_MESSAGE_LENGTH,
        emoji="📡",
        pii_safe=True,
        # An event bus is not an operator console: a publisher must not be able to trigger
        # /update (a gateway update/restart) by putting that text in a payload.
        allow_update_command=False,
        target_ref_syntax_fn=_parse_send_target,
        dashboard_description="Consume the house MQTT event bus as a platform, with per-topic deliver / agent / job / drop modes.",
        platform_hint=(
            "This conversation is fed by the house MQTT event bus, not by a person typing. "
            "Each inbound message is a JSON event envelope; `summary` is the human-readable line "
            "and `json` is the producer payload. Nobody is watching the topic you reply to — a "
            "reply is published to the bus's outbound topic, so keep it self-contained."
        ),
    )
