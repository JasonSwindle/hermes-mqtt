"""Minimal MQTT client (3.1.1 and 5.0) for the Hermes event-bus consumer. Standard library only.

Why hand-rolled: the Hermes runtime venv ships no MQTT library, has no ``pip``
module, is root-owned and lives inside the image, so a third-party client would
have to be vendored or re-installed after every image update. The bus needs a
fixed, small subset — CONNECT/CONNACK, SUBSCRIBE/SUBACK, PUBLISH/PUBACK at QoS
0-1, PINGREQ/PINGRESP — over TCP or TLS (``mqtts://``), with no will, no QoS 2 and
no retained-message bookkeeping beyond passing the flag through to the caller.

Everything the adapter touches is one class: :class:`MqttSession`.
"""

from __future__ import annotations

import asyncio
import logging
import ssl
import struct
import time
from typing import AsyncIterator, Optional

from .mqtt_codec import (
    P_MAXIMUM_PACKET_SIZE,
    P_MAXIMUM_QOS,
    P_RECEIVE_MAXIMUM,
    P_RETAIN_AVAILABLE,
    P_SERVER_KEEP_ALIVE,
    PROTOCOL_311,
    PROTOCOL_5,
    CodecError,
    decode_ack,
    decode_connack,
    decode_publish,
    decode_suback,
    encode_connect,
    encode_publish,
    encode_subscribe,
    levels_for,
    normalize_protocol,
)

logger = logging.getLogger(__name__)

CONNECT, CONNACK, PUBLISH, PUBACK, PUBREC, PUBREL, PUBCOMP = 1, 2, 3, 4, 5, 6, 7
SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK = 8, 9, 10, 11
PINGREQ, PINGRESP, DISCONNECT = 12, 13, 14

#: v3.1.1 CONNACK codes that no amount of retrying can fix.
FATAL_CONNACK = {
    2: "client identifier rejected",
}
AUTH_CONNACK = frozenset({4, 5})  # v3: bad username/password, not authorised

#: MQTT 5 CONNACK reasons that no amount of retrying can fix (client/config/broker
#: policy). 0x88 server unavailable, 0x89 busy, 0x97 quota and 0x9F rate-limit stay
#: retryable; 0x84 is handled as a level refusal so the auto ladder can fall back.
FATAL_CONNACK_V5 = {
    0x80: "unspecified error",
    0x81: "malformed packet",
    0x82: "protocol error",
    0x83: "implementation specific error",
    0x85: "client identifier not valid",
    0x8A: "banned",
    0x8C: "bad authentication method",
    0x90: "topic name invalid",
    0x95: "packet too large",
    0x99: "payload format invalid",
    0x9A: "retain not supported",
    0x9B: "QoS not supported",
    0x9C: "use another server",
    0x9D: "server moved",
}
AUTH_CONNACK_V5 = frozenset({0x86, 0x87})  # bad user name/password, not authorised


class MqttError(Exception):
    """Connection-level failure. The caller reconnects."""


class ConnackLost(MqttError):
    """The connection ended before a CONNACK arrived; the level may be wrong."""


class MqttAuthError(MqttError):
    """The broker refused the credential — retrying cannot help."""


class MqttFatal(MqttError):
    """A permanent CONNACK refusal (bad client id, banned, unsupported, …)."""


class MqttTlsError(MqttFatal):
    """TLS material or record-level handshake failure.

    Retrying the same configuration cannot help; the ambiguous lost-CONNACK subclass
    (:class:`MqttTlsLost`) is deliberately kept retryable by the adapter because a
    rotated or newly-mounted identity may recover.
    """


class MqttTlsLost(MqttTlsError):
    """Every protocol level lost CONNACK over TLS — likely a rejected client identity.

    A peer that completes the TLS handshake and then closes before CONNACK is ambiguous:
    it can be a 3.1.1-only broker rejecting the MQTT level (which the auto ladder may
    still recover from) or a rejected post-handshake identity. After the ladder is
    exhausted the adapter reports this as `mqtt_tls_failed` and keeps retrying, because a
    renewed certificate/CA can fix it.
    """


class TlsConnackLost(ConnackLost):
    """TLS peer closed before CONNACK after a successful handshake; the ladder may retry."""


class SubscriptionRefused(MqttError):
    """The broker refused the subscription (bad filter or ACL) — retrying cannot help."""


class ProtocolUnsupported(MqttError):
    """The broker refused this CONNECT's protocol level; a lower level may work."""


def encode_length(value: int) -> bytes:
    """Encode the MQTT "remaining length" varint (1-4 bytes)."""
    if not 0 <= value <= 268_435_455:
        raise ValueError(f"remaining length out of range: {value}")
    out = bytearray()
    while True:
        byte, value = value % 128, value // 128
        out.append(byte | 0x80 if value else byte)
        if not value:
            return bytes(out)


async def _read_length(reader: asyncio.StreamReader) -> int:
    value, multiplier = 0, 1
    for index in range(4):
        (byte,) = await reader.readexactly(1)
        value += (byte & 0x7F) * multiplier
        if not byte & 0x80:
            if index and byte == 0:
                # [MQTT-1.5.5-1] the minimum number of bytes MUST be used.
                raise MqttError("non-minimal remaining length")
            return value
        multiplier *= 128
    raise MqttError("malformed remaining length (5th continuation byte)")


def _packet(first_byte: int, body: bytes = b"") -> bytes:
    return bytes((first_byte,)) + encode_length(len(body)) + body


def _key_header(key_file: str) -> bytes:
    """First bytes of the client key, to detect an encrypted key before OpenSSL prompts.

    ``ssl.SSLContext.load_cert_chain`` falls back to an interactive "Enter PEM pass
    phrase:" prompt when a private key is encrypted and no password is supplied — which
    in a headless gateway can block a connect instead of producing a config error.
    """
    try:
        with open(key_file, "rb") as handle:
            return handle.read(4096)
    except OSError as exc:
        raise ValueError(f"cannot read tls_key_file {key_file!r}: {exc}") from exc


def build_tls_context(
    *,
    insecure: bool = False,
    ca_file: str = "",
    cert_file: str = "",
    key_file: str = "",
    key_password: Optional[str] = None,
) -> ssl.SSLContext:
    """Build the client TLS context for one connection attempt. Standard library only.

    Server certificate and hostname verification are ON unless ``insecure`` is
    explicitly requested. ``ca_file`` is used as the *only* trust anchor when set
    (replacing the system trust store); ``cert_file``/``key_file`` form an optional
    mutual-TLS client identity. Bad material raises ``ValueError`` naming the setting
    or path (the adapter turns that into a fatal config error); file contents and the
    key password are never included in the message.
    """
    ca_file = str(ca_file or "").strip()
    cert_file = str(cert_file or "").strip()
    key_file = str(key_file or "").strip()
    if bool(cert_file) != bool(key_file):
        raise ValueError("tls_cert_file and tls_key_file must be set together")
    try:
        if insecure:
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
        else:
            context = ssl.create_default_context(cafile=ca_file or None)
        # Pin the documented floor even if a future stdlib default moves: TLS 1.0/1.1 are
        # deprecated and must never be negotiated implicitly.
        context.minimum_version = ssl.TLSVersion.TLSv1_2
    except (OSError, ssl.SSLError) as exc:
        raise ValueError(f"cannot load tls_ca_file {ca_file!r}: {exc}") from exc
    if cert_file:
        header = _key_header(key_file)
        if b"ENCRYPTED" in header and not key_password:
            raise ValueError(
                "tls_key_file is encrypted but tls_key_password is not set")
        try:
            context.load_cert_chain(cert_file, key_file, password=key_password or None)
        except (OSError, ssl.SSLError) as exc:
            raise ValueError(
                f"cannot load tls_cert_file {cert_file!r} / tls_key_file {key_file!r}: {exc}"
            ) from exc
    return context


class MqttSession:
    """One live broker connection: framing, keepalive, packet dispatch.

    ``messages()`` yields ``(topic, payload, retained)`` for inbound publishes and
    is the only reader of the socket; ``publish()`` and ``subscribe()`` await
    their own acknowledgement from inside that pump, so nothing else may read.
    """

    def __init__(
        self,
        host: str,
        port: int = 1883,
        *,
        client_id: str = "",
        username: str = "",
        password: str = "",
        keepalive: int = 60,
        ack_timeout: float = 10.0,
        queue_size: int = 500,
        protocol: int = PROTOCOL_311,
        max_inbound_packet: int = 0,
        ssl_context: Optional[ssl.SSLContext] = None,
        server_hostname: Optional[str] = None,
    ) -> None:
        self.host, self.port = host, port
        self.client_id = client_id
        self.username, self.password = username, password
        if protocol == PROTOCOL_311 and password and not username:
            # MQTT 3.1.1 [MQTT-3.1.2-18]; MQTT 5 removed the restriction, so a
            # password-only CONNECT is valid on level 5.
            raise ValueError(
                "MQTT 3.1.1 does not allow a password without a username; "
                "set username or drop the password"
            )
        try:
            keepalive = int(keepalive)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"keepalive must be an integer, not {keepalive!r}") from exc
        if not 0 <= keepalive <= 0xFFFF:
            raise ValueError(f"keepalive out of range: {keepalive}")
        self.keepalive = keepalive
        self.protocol = protocol
        self.ack_timeout = ack_timeout
        self.max_inbound_packet = max(0, int(max_inbound_packet))
        self._ssl_context = ssl_context
        self._server_hostname = server_hostname or None
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=queue_size)
        self._pending: dict[int, asyncio.Future] = {}
        self._packet_id = 0
        self._pump: Optional[asyncio.Task] = None
        self._heartbeat: Optional[asyncio.Task] = None
        self._closed = False
        self._connack_received = False
        self._awaiting_pong = False
        self._last_sent = 0.0
        self._close_reason = ""
        # MQTT 5 CONNACK limits; permissive defaults until the broker says otherwise.
        self.server_receive_maximum = 65535
        self.server_max_qos = 2
        self.server_retain_available = True
        self.server_max_packet_size = 0
        self._send_slots = asyncio.Semaphore(65535)

    # -- lifecycle ---------------------------------------------------------

    @classmethod
    async def open(cls, host: str, port: int = 1883, *, timeout: float = 10.0,
                   protocol="auto", **kwargs) -> "MqttSession":
        """TCP connect + CONNECT/CONNACK.

        ``protocol`` accepts "auto" (try MQTT 5, fall back to 3.1.1), "5" or
        "3.1.1". The ladder advances on a level refusal (CONNACK 0x84/0x01) and on a
        CONNACK that never arrives — a 3.1.1-only broker answers a level-5 CONNECT
        with a v3 CONNACK or simply closes the socket ([MQTT-3.1.2-2]). Auth and
        other permanent refusals do not fall back. The level that succeeded is
        recorded on ``session.protocol``.
        """
        levels = levels_for(normalize_protocol(protocol))
        last_error: Optional[Exception] = None
        for index, level in enumerate(levels):
            session = cls(host, port, protocol=level, **kwargs)
            try:
                await session._start(timeout)
            except (MqttAuthError, MqttFatal):
                raise
            except ProtocolUnsupported as exc:
                last_error = exc
                if index + 1 >= len(levels):
                    raise
                logger.info("broker refused MQTT level %s; trying the next level", level)
                continue
            except ConnackLost as exc:
                last_error = exc
                if index + 1 >= len(levels):
                    if isinstance(exc, TlsConnackLost):
                        raise MqttTlsLost(str(exc)) from exc
                    raise
                logger.info("no CONNACK at MQTT level %s; trying the next level", level)
                continue
            return session
        assert last_error is not None
        raise last_error

    async def _start(self, timeout: float) -> None:
        try:
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(
                    self.host, self.port,
                    ssl=self._ssl_context,
                    server_hostname=(
                        self._server_hostname or self.host) if self._ssl_context else None,
                ),
                timeout=timeout,
            )
        except ssl.SSLError as exc:
            # Certificate/hostname/version problems are configuration, not transient:
            # the adapter reports them as a fatal TLS error instead of retrying forever.
            raise MqttTlsError(f"TLS handshake failed: {exc}") from exc
        try:
            body = encode_connect(
                client_id=self.client_id,
                username=self.username,
                password=self.password.encode(),
                keepalive=self.keepalive,
                protocol=self.protocol,
            )
        except CodecError as exc:
            await self.aclose()
            raise MqttError(f"cannot encode CONNECT: {exc}") from exc
        try:
            await self._send(CONNECT << 4, body)
            first, body = await asyncio.wait_for(self._read_packet(), timeout=timeout)
        except asyncio.TimeoutError:
            await self.aclose()
            raise MqttError(f"no CONNACK from {self.host}:{self.port} within {timeout}s")
        except asyncio.CancelledError:
            await self.aclose()
            raise
        except MqttError:
            await self.aclose()
            raise
        except ssl.SSLError as exc:
            # A TLS alert can arrive after the TCP/handshake phase (e.g. the server
            # rejects a missing client certificate). That is a TLS configuration
            # failure, not a reason to try another MQTT protocol level.
            await self.aclose()
            raise MqttTlsError(f"TLS failure before CONNACK: {exc}") from exc
        except (OSError, EOFError) as exc:  # includes asyncio.IncompleteReadError
            await self.aclose()
            if self._ssl_context is not None:
                # A peer that finished the TLS handshake then closed before CONNACK is
                # ambiguous: a 3.1.1-only broker may be rejecting the MQTT level (the
                # auto ladder should still try the next level), or the server may have
                # rejected our post-handshake identity. Keep it ladder-eligible; open()
                # turns "every level lost" into MqttTlsLost, and no path downgrades TLS.
                raise TlsConnackLost(
                    "TLS peer closed before CONNACK "
                    f"(possibly rejecting the MQTT level or the client identity): {exc}"
                ) from exc
            raise ConnackLost(f"connection lost before CONNACK: {exc}") from exc
        if first >> 4 != CONNACK:
            await self.aclose()
            raise MqttError(f"expected CONNACK, got packet type {first >> 4}")
        if self.protocol == PROTOCOL_5 and len(body) == 2:
            # A 3.1.1-only broker answers a level-5 CONNECT with its own 2-byte
            # CONNACK and disconnects ([MQTT-3.1.2-2]); that is a level refusal, not
            # a malformed v5 CONNACK, so the auto ladder may fall back.
            try:
                legacy = decode_connack(body, PROTOCOL_311)
            except CodecError as exc:
                await self.aclose()
                raise MqttError(f"malformed CONNACK: {exc}") from exc
            await self.aclose()
            if legacy.code in (0, 1):
                raise ProtocolUnsupported(
                    f"broker answered MQTT level 5 with a v3 CONNACK ({legacy.code})")
            if legacy.code in AUTH_CONNACK:
                raise MqttAuthError(
                    f"broker refused {self.username or 'anonymous'}: v3 CONNACK code {legacy.code}")
            raise MqttFatal(f"broker answered level 5 with a v3 CONNACK code {legacy.code}")
        try:
            connack = decode_connack(body, self.protocol)
        except CodecError as exc:
            await self.aclose()
            raise MqttError(f"malformed CONNACK: {exc}") from exc
        self._connack_received = True
        if connack.session_present:
            # We always send Clean Start/Clean Session, so a present session is a
            # broker protocol violation ([MQTT-3.2.2-2], [MQTT-3.2.2-4]).
            await self.aclose()
            raise MqttError("broker reported a present session on a clean-start connection")
        if connack.code:
            await self.aclose()
            level_refused = connack.code == 0x84 or (
                self.protocol == PROTOCOL_311 and connack.code == 1)
            reason = (
                FATAL_CONNACK_V5.get(connack.code)
                or FATAL_CONNACK.get(connack.code)
                or f"CONNACK return code 0x{connack.code:02x}"
            )
            if level_refused:
                raise ProtocolUnsupported(
                    f"broker refused MQTT level {self.protocol}: {reason}")
            auth_refused = (
                (self.protocol == PROTOCOL_5 and connack.code in AUTH_CONNACK_V5)
                or (self.protocol == PROTOCOL_311 and connack.code in AUTH_CONNACK))
            if auth_refused:
                raise MqttAuthError(f"broker refused {self.username or 'anonymous'}: {reason}")
            fatal = (
                (self.protocol == PROTOCOL_5 and connack.code in FATAL_CONNACK_V5)
                or (self.protocol == PROTOCOL_311 and connack.code in FATAL_CONNACK))
            if fatal:
                raise MqttFatal(f"broker refused the connection: {reason}")
            raise MqttError(reason)  # server unavailable/busy/quota: retryable
        try:
            self._apply_connack(connack)
        except MqttError:
            await self.aclose()
            raise
        self._pump = asyncio.create_task(self._run(), name=f"mqtt-pump-{self.host}")
        if self.keepalive > 0:
            self._heartbeat = asyncio.create_task(
                self._heartbeat_loop(), name=f"mqtt-heartbeat-{self.host}")

    def _apply_connack(self, connack) -> None:
        """Honour the broker's MQTT 5 limits; a 3.1.1 CONNACK sets none of them.

        The v5 spec makes a zero Receive Maximum / Maximum Packet Size a Protocol Error,
        and Maximum QoS may only be 0 or 1. A broken broker must fail the connection
        instead of wedging a zero-permit semaphore or silently disabling a limit.
        """
        props = connack.properties or {}
        receive_maximum = props.get(P_RECEIVE_MAXIMUM)
        if receive_maximum is not None:
            receive_maximum = int(receive_maximum)
            if receive_maximum <= 0:
                raise MqttError("broker sent invalid Receive Maximum 0")
            self.server_receive_maximum = receive_maximum
            self._send_slots = asyncio.Semaphore(receive_maximum)
        if P_MAXIMUM_QOS in props:
            maximum_qos = int(props[P_MAXIMUM_QOS])
            if maximum_qos not in (0, 1):
                raise MqttError(f"broker sent invalid Maximum QoS {maximum_qos}")
            self.server_max_qos = min(self.server_max_qos, maximum_qos)
        if P_RETAIN_AVAILABLE in props:
            retain_available = int(props[P_RETAIN_AVAILABLE])
            if retain_available not in (0, 1):
                raise MqttError(f"broker sent invalid Retain Available {retain_available}")
            self.server_retain_available = bool(retain_available)
        if P_MAXIMUM_PACKET_SIZE in props:
            maximum_packet_size = int(props[P_MAXIMUM_PACKET_SIZE])
            if maximum_packet_size <= 0:
                raise MqttError("broker sent invalid Maximum Packet Size 0")
            self.server_max_packet_size = maximum_packet_size
        if P_SERVER_KEEP_ALIVE in props:
            self.keepalive = int(props[P_SERVER_KEEP_ALIVE])

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._heartbeat and not self._heartbeat.done():
            self._heartbeat.cancel()
            try:
                await self._heartbeat
            except (asyncio.CancelledError, Exception):
                pass
            self._heartbeat = None
        if self._pump and not self._pump.done():
            self._pump.cancel()
            try:
                await self._pump
            except (asyncio.CancelledError, Exception):
                pass
        if self._writer is not None:
            try:
                if not self._writer.is_closing() and self._connack_received:
                    # No DISCONNECT before a successful CONNACK: there is no session
                    # to end yet, and a refused level must not look like a clean close.
                    self._writer.write(_packet(DISCONNECT << 4))
                    await self._writer.drain()
            except Exception:
                pass
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except Exception:
                pass
            self._writer = None
        for future in self._pending.values():
            if not future.done():
                future.set_exception(MqttError("session closed"))
        self._pending.clear()

    # -- public API --------------------------------------------------------

    async def subscribe(self, topic_filter: str, qos: int = 1) -> int:
        """Subscribe and await the SUBACK; returns the granted QoS."""
        if qos not in (0, 1):
            raise ValueError(f"unsupported QoS {qos}: this client implements QoS 0 and 1 only")
        if not topic_filter:
            raise ValueError("empty topic filter")
        packet_id = self._next_packet_id()
        future = self._new_pending(packet_id)
        body = encode_subscribe(
            packet_id=packet_id, topic_filter=topic_filter, qos=qos, protocol=self.protocol
        )
        await self._send(SUBSCRIBE << 4 | 0x02, body)
        try:
            ack = await self._await_ack(future, f"SUBACK for {topic_filter!r}")
        finally:
            # A timed-out ack must not keep a bounded packet-id slot reserved.
            self._pending.pop(packet_id, None)
            if not future.done():
                future.cancel()
        if ack.reason >= 0x80:
            raise SubscriptionRefused(
                f"broker refused subscription to {topic_filter!r} (0x{ack.reason:02x})"
            )
        if ack.reason > qos:
            raise MqttError(
                f"broker granted QoS {ack.reason} for a QoS {qos} subscription to {topic_filter!r}"
            )
        return ack.reason

    async def publish(self, topic: str, payload: bytes, qos: int = 1, retain: bool = False) -> int:
        """Publish and await the PUBACK at QoS 1; returns the packet id (0 at QoS 0)."""
        if qos not in (0, 1):
            raise ValueError(f"unsupported QoS {qos}: this client implements QoS 0 and 1 only")
        if not topic or "+" in topic or "#" in topic:
            raise ValueError(f"invalid publish topic: {topic!r}")
        if qos > self.server_max_qos:
            raise ValueError(f"broker accepts at most QoS {self.server_max_qos}")
        if retain and not self.server_retain_available:
            raise ValueError("broker does not support retained messages")
        if qos:
            # Acquire BEFORE reserving a packet id: a cancellation while waiting must not
            # leave an orphan future in ``_pending``.
            await self._send_slots.acquire()
        future: Optional[asyncio.Future] = None
        packet_id = 0
        try:
            packet_id = self._next_packet_id() if qos else 0
            future = self._new_pending(packet_id) if qos else None
            first, body = encode_publish(
                topic=topic, payload=payload, qos=qos, retain=retain,
                packet_id=packet_id, protocol=self.protocol,
            )
            await self._send(first, body)
            if future is not None:
                ack = await self._await_ack(future, f"PUBACK for {topic!r}")
                if ack.reason >= 0x80:
                    raise MqttError(f"PUBACK refused for {topic!r} (0x{ack.reason:02x})")
                if ack.reason == 0x10:
                    logger.debug("[mqtt] no matching subscribers for %s", topic)
        finally:
            if qos:
                self._send_slots.release()
            if future is not None:
                # Pop unconditionally: a timed-out ack cancels the future (so done() is
                # True) but the id must still be released rather than leak for the session.
                self._pending.pop(packet_id, None)
                if not future.done():
                    future.cancel()
        return packet_id

    async def messages(self) -> AsyncIterator[tuple[str, bytes, bool]]:
        """Yield inbound publishes until the session fails or closes."""
        while True:
            item = await self._queue.get()
            if item is None:  # pump ended
                raise MqttError(self._close_reason or "session closed by broker")
            yield item

    # -- internals ---------------------------------------------------------

    def _next_packet_id(self) -> int:
        for _ in range(0xFFFF):
            self._packet_id = self._packet_id % 0xFFFF + 1
            if self._packet_id not in self._pending:
                return self._packet_id
        raise MqttError("no free packet identifier")

    def _new_pending(self, packet_id: int) -> asyncio.Future:
        future = asyncio.get_running_loop().create_future()
        self._pending[packet_id] = future
        return future

    async def _await_ack(self, future: asyncio.Future, what: str):
        try:
            return await asyncio.wait_for(future, timeout=self.ack_timeout)
        except asyncio.TimeoutError:
            raise MqttError(f"timed out waiting for {what}")

    async def _send(self, first_byte: int, body: bytes) -> None:
        if self._writer is None or self._writer.is_closing():
            raise MqttError("connection is closed")
        data = _packet(first_byte, body)
        if self.server_max_packet_size and len(data) > self.server_max_packet_size:
            raise MqttError(
                f"packet of {len(data)} bytes exceeds the broker maximum "
                f"of {self.server_max_packet_size}"
            )
        self._writer.write(data)
        await self._writer.drain()
        self._last_sent = time.monotonic()

    async def _read_packet(self) -> tuple[int, bytes]:
        assert self._reader is not None
        (first,) = await self._reader.readexactly(1)
        length = await _read_length(self._reader)
        if self.max_inbound_packet and length > self.max_inbound_packet:
            # Refuse BEFORE allocating: a broken or hostile peer must not choose our buffer size.
            raise MqttError(
                f"inbound packet of {length} bytes exceeds the {self.max_inbound_packet}-byte cap"
            )
        return first, await self._reader.readexactly(length)

    async def _run(self) -> None:
        """Read every inbound packet and route acknowledgements.

        The read is deliberately NOT wrapped in a keepalive timeout: cancelling a
        ``readexactly`` mid-packet would discard bytes already consumed and desync the
        framing. Liveness/heartbeat lives in :meth:`_heartbeat_loop`, which only ever
        writes; a stalled broker shuts the session down by cancelling this pump.
        """
        try:
            while not self._closed:
                first, body = await self._read_packet()
                kind = first >> 4
                if kind != PUBLISH and first & 0x0F:
                    # [MQTT-2.1.3-1] reserved flag bits must be zero on every packet
                    # except PUBLISH (where they are DUP/QoS/RETAIN).
                    raise MqttError(f"malformed fixed-header flags on packet type {kind}")
                self._awaiting_pong = False  # any inbound packet proves the peer is alive
                if kind == PUBLISH:
                    await self._on_publish(first, body)
                elif kind == PUBACK:
                    ack = decode_ack(body, self.protocol)
                    self._resolve(ack.packet_id, ack)
                elif kind == SUBACK:
                    ack = decode_suback(body, self.protocol)
                    self._resolve(ack.packet_id, ack)
                elif kind == PINGRESP:
                    if body:
                        raise MqttError("PINGRESP with a body (malformed packet)")
                elif kind in (PUBREC, PUBREL, PUBCOMP):
                    # We never grant QoS 2, so a QoS 2 flow packet is a broker protocol
                    # violation; leaving it unanswered would invite a retry loop.
                    raise MqttError(f"unexpected QoS 2 packet type {kind} from broker")
                elif kind == DISCONNECT:
                    reason = body[0] if body else 0
                    raise MqttError(f"broker sent DISCONNECT (0x{reason:02x})")
                else:
                    # Type 0 is reserved and every other valid type is handled above; an
                    # unexpected packet is a protocol violation, not something to ignore.
                    raise MqttError(f"unexpected packet type {kind} from broker")
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - surfaced to the caller via the queue
            if not self._closed:
                self._close_reason = str(exc)
                logger.debug("mqtt pump ended: %s", exc)
        finally:
            # Wake ``messages()`` if it is parked on the queue; drop the oldest item rather
            # than block, so a dead session can never leave a consumer waiting forever.
            if self._queue.full():
                try:
                    self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            self._queue.put_nowait(None)

    async def _heartbeat_loop(self) -> None:
        """Send PINGREQ when the client has been silent too long, then require a reply.

        Keepalive is an OUTBOUND obligation ([MQTT-3.1.2-20] in v5, [MQTT-3.1.2-23] in
        v3.1.1): a gap between the Control Packets WE send may not exceed the interval.
        Running it in its own task keeps it correct even while the pump is blocked in
        ``_on_publish`` on a full queue, and means a ping never cancels a read.
        """
        interval = max(0.1, self.keepalive / 2)
        while not self._closed:
            await asyncio.sleep(interval)
            if self._closed:
                return
            idle = time.monotonic() - self._last_sent
            if idle < self.keepalive:
                continue
            if self._awaiting_pong:
                self._fail(MqttError("keepalive: no PINGRESP"))
                return
            self._awaiting_pong = True
            try:
                await self._send(PINGREQ << 4, b"")
            except Exception as exc:  # noqa: BLE001 - a failed ping ends the session
                self._fail(MqttError(f"keepalive: PINGREQ failed: {exc}"))
                return

    def _fail(self, exc: Exception) -> None:
        """End the pump from the heartbeat task (the only writer-side failure path)."""
        if self._closed:
            return
        self._close_reason = str(exc)
        if self._pump is not None and not self._pump.done():
            self._pump.cancel()

    async def _on_publish(self, first: int, body: bytes) -> None:
        try:
            msg = decode_publish(first, body, self.protocol)
        except CodecError as exc:
            raise MqttError(f"malformed PUBLISH: {exc}") from exc
        if msg.qos == 2:
            # [MQTT-3.8.4-8] a QoS 2 delivery to a QoS 0/1 subscription is a protocol
            # error; dropping it silently would leave the broker retrying forever.
            raise MqttError(f"QoS 2 PUBLISH received on {msg.topic!r} (QoS 2 not supported)")
        # Enqueue BEFORE acknowledging: a PUBACK is a promise that the message was
        # accepted, so a crash/backpressure must not ack a message that was then lost.
        await self._queue.put((msg.topic, msg.payload, msg.retain))
        if msg.qos == 1:
            await self._send(PUBACK << 4, struct.pack("!H", msg.packet_id))

    def _resolve(self, packet_id: int, value) -> None:
        future = self._pending.pop(packet_id, None)
        if future is not None and not future.done():
            future.set_result(value)
