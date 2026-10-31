"""Byte-level and socket-level tests for the plugin's MQTT client.

The fake broker below re-implements framing independently of the client under test,
so a mistake in the client's encoder cannot be mirrored by its own test harness.
Run with:

    PYTHONPATH=/path/to/hermes:/path/to/site-packages python -m unittest discover -s tests
"""

from __future__ import annotations

import asyncio
import os
import ssl
import struct
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _bootstrap  # noqa: E402,F401  (registers the package however the repo is checked out)

from mqtt.mqtt_client import (  # noqa: E402
    MqttAuthError,
    MqttError,
    MqttFatal,
    MqttSession,
    SubscriptionRefused,
    encode_length,
)
from mqtt import mqtt_codec  # noqa: E402


async def read_packet(reader: asyncio.StreamReader) -> tuple[int, bytes]:
    """Independent packet reader for the fake broker."""
    first = (await reader.readexactly(1))[0]
    length, multiplier = 0, 1
    while True:
        byte = (await reader.readexactly(1))[0]
        length += (byte & 0x7F) * multiplier
        if not byte & 0x80:
            break
        multiplier *= 128
    body = await reader.readexactly(length) if length else b""
    return first, body


def packet(first: int, body: bytes = b"") -> bytes:
    return bytes((first,)) + encode_length(len(body)) + body


def utf8(value: str) -> bytes:
    raw = value.encode()
    return struct.pack("!H", len(raw)) + raw


async def wait_for_type(broker: "FakeBroker", kind: int, timeout: float = 4.0) -> bool:
    """Wait for the broker to observe a packet type; keepalive/gateway timing is not fixed.

    Fixed sleeps raced slow Windows CI runners: a 1 s keepalive PINGREQ had still not
    arrived after a 1.2 s sleep, so the assertion failed without any protocol defect.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if kind in broker.types():
            return True
        await asyncio.sleep(0.02)
    return kind in broker.types()


class FakeBroker:
    """Just enough broker for both dialects: CONNACK, SUBACK, PUBACK, PINGRESP, injected PUBLISHes."""

    def __init__(self, connack: int = 0, suback: int = 1, *, protocol: int = 4,
                 receive_maximum: int | None = None, server_keepalive: int | None = None,
                 maximum_qos: int | None = None, retain_available: bool | int | None = None,
                 maximum_packet_size: int | None = None,
                 puback_reason: int = 0, auto_ack: bool = True, auto_suback: bool = True,
                 answer_ping: bool = True, refuse_level_5: bool = False,
                 connack_flags: int = 0, extra_connack_props: bytes = b"",
                 server_ssl: ssl.SSLContext | None = None):
        self.connack, self.suback = connack, suback
        self.server_ssl = server_ssl
        self.auto_suback = auto_suback
        self.connack_flags = connack_flags
        self.extra_connack_props = extra_connack_props
        self.protocol = protocol
        self.receive_maximum = receive_maximum
        self.server_keepalive = server_keepalive
        self.maximum_qos = maximum_qos
        self.retain_available = retain_available
        self.maximum_packet_size = maximum_packet_size
        self.puback_reason = puback_reason
        self.auto_ack = auto_ack
        self.answer_ping = answer_ping
        self.refuse_level_5 = refuse_level_5
        self.port = 0
        self.connect_body = b""
        self.level = 0
        self.packets: list[tuple[int, bytes]] = []
        self.pids: list[bytes] = []
        self._server = None
        self._writer = None
        self._connected = asyncio.Event()

    async def __aenter__(self):
        self._server = await asyncio.start_server(
            self._serve, "127.0.0.1", 0, ssl=self.server_ssl)
        self.port = self._server.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *exc):
        if self._writer is not None:
            self._writer.close()
        self._server.close()
        await self._server.wait_closed()

    async def wait_connected(self, timeout: float = 5.0) -> None:
        await asyncio.wait_for(self._connected.wait(), timeout)

    def _connack_body(self) -> bytes:
        if self.protocol != 5:
            return bytes((self.connack_flags, self.connack))
        props = self.extra_connack_props
        if self.receive_maximum is not None:
            props += bytes((0x21,)) + struct.pack("!H", self.receive_maximum)
        if self.server_keepalive is not None:
            props += bytes((0x13,)) + struct.pack("!H", self.server_keepalive)
        if self.maximum_qos is not None:
            props += bytes((0x24, self.maximum_qos))
        if self.retain_available is not None:
            if isinstance(self.retain_available, bool):
                retain = 1 if self.retain_available else 0
            else:
                retain = int(self.retain_available)
            props += bytes((0x25, retain))
        if self.maximum_packet_size is not None:
            props += bytes((0x27,)) + struct.pack("!I", self.maximum_packet_size)
        return bytes((self.connack_flags, self.connack, len(props))) + props

    def _puback(self, pid: bytes) -> bytes:
        if self.protocol == 5:
            return packet(4 << 4, pid + bytes((self.puback_reason, 0)))
        return packet(4 << 4, pid)

    async def _serve(self, reader, writer):
        self._writer = writer
        try:
            first, body = await read_packet(reader)
            assert first >> 4 == 1, "expected CONNECT"
            self.connect_body = body
            self.level = body[6]
            self.protocol = self.level
            if self.level == 5 and self.refuse_level_5:
                writer.write(packet(2 << 4, bytes((0, 0x84, 0))))
                await writer.drain()
                return
            writer.write(packet(2 << 4, self._connack_body()))
            await writer.drain()
            self._connected.set()
            if self.connack:
                return
            while True:
                first, body = await read_packet(reader)
                kind = first >> 4
                self.packets.append((kind, body))
                if kind == 8:  # SUBSCRIBE
                    pid = body[:2]
                    if self.auto_suback:
                        suffix = b"\x00" if self.protocol == 5 else b""
                        writer.write(packet(9 << 4, pid + suffix + bytes((self.suback,))))
                elif kind == 3:  # PUBLISH qos1
                    if (first >> 1) & 0x03 == 1:
                        topic_len = struct.unpack("!H", body[:2])[0]
                        pid = body[2 + topic_len:4 + topic_len]
                        self.pids.append(pid)
                        if self.auto_ack:
                            writer.write(self._puback(pid))
                elif kind == 12:
                    if self.answer_ping:
                        writer.write(packet(13 << 4))
                elif kind == 14:
                    break
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionResetError, asyncio.CancelledError):
            pass
        finally:
            writer.close()

    async def publish(self, topic: str, payload: bytes, *, qos: int = 1, pid: int = 7,
                      chunk: int = 0) -> None:
        body = utf8(topic) + (struct.pack("!H", pid) if qos else b"")
        if self.protocol == 5:
            body += b"\x00"
        body += payload
        data = packet(3 << 4 | (qos << 1), body)
        if chunk <= 0:
            self._writer.write(data)
        else:
            for i in range(0, len(data), chunk):
                self._writer.write(data[i:i + chunk])
                await self._writer.drain()
        await self._writer.drain()

    async def release(self, pid: bytes) -> None:
        self._writer.write(self._puback(pid))
        await self._writer.drain()

    async def disconnect(self, reason: int = 0) -> None:
        self._writer.write(packet(14 << 4, bytes((reason,))))
        await self._writer.drain()

    def types(self) -> list[int]:
        return [kind for kind, _ in self.packets]

    async def wait_for_type(self, kind: int, timeout: float = 2.0) -> None:
        deadline = asyncio.get_running_loop().time() + timeout
        while kind not in self.types():
            if asyncio.get_running_loop().time() > deadline:
                raise AssertionError(f"broker never received packet type {kind}; got {self.types()}")
            await asyncio.sleep(0.01)

    def body_of(self, kind: int) -> bytes:
        return next(body for t, body in self.packets if t == kind)


class EncodingTests(unittest.TestCase):
    def test_remaining_length_roundtrip(self):
        for value, expected in [
            (0, b"\x00"), (127, b"\x7f"), (128, b"\x80\x01"),
            (16383, b"\xff\x7f"), (16384, b"\x80\x80\x01"), (2_097_151, b"\xff\xff\x7f"),
        ]:
            self.assertEqual(encode_length(value), expected, value)

    def test_remaining_length_rejects_out_of_range(self):
        with self.assertRaises(ValueError):
            encode_length(268_435_456)
        with self.assertRaises(ValueError):
            encode_length(-1)


class SessionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.broker = await FakeBroker().__aenter__()

    async def asyncTearDown(self):
        await self.broker.__aexit__()

    async def session(self, **kwargs) -> MqttSession:
        return await MqttSession.open(
            "127.0.0.1", self.broker.port, client_id="test-client",
            username="hermes", password="secret", timeout=5.0, keepalive=5,
            protocol=4,  # MQTT 3.1.1: pinned so the dual-dialect broker answers v4
            **kwargs,
        )

    async def test_connect_carries_protocol_level_flags_user_and_password(self):
        session = await self.session()
        try:
            await self.broker.wait_connected()
            body = self.broker.connect_body
            self.assertEqual(body[:6], utf8("MQTT"))
            self.assertEqual(body[6], 4)  # protocol level 3.1.1
            flags = body[7]
            self.assertTrue(flags & 0x02, "clean session not requested")
            self.assertTrue(flags & 0x80, "username flag not set")
            self.assertTrue(flags & 0x40, "password flag not set")
            self.assertEqual(struct.unpack("!H", body[8:10])[0], 5)  # keepalive
            rest = body[10:]
            client_id = rest[2 : 2 + struct.unpack("!H", rest[:2])[0]]
            self.assertEqual(client_id, b"test-client")
            rest = rest[2 + len(client_id) :]
            user = rest[2 : 2 + struct.unpack("!H", rest[:2])[0]]
            self.assertEqual(user, b"hermes")
            rest = rest[2 + len(user) :]
            password = rest[2 : 2 + struct.unpack("!H", rest[:2])[0]]
            self.assertEqual(password, b"secret")
        finally:
            await session.aclose()

    async def test_subscribe_returns_granted_qos(self):
        session = await self.session()
        try:
            granted = await session.subscribe("events/#", qos=1)
            self.assertEqual(granted, 1)
            self.assertIn(8, self.broker.types())
            body = self.broker.body_of(8)
            self.assertEqual(struct.unpack("!H", body[:2])[0], 1)  # packet id
            self.assertEqual(body[2:], utf8("events/#") + b"\x01")
        finally:
            await session.aclose()

    async def test_qos1_publish_is_acknowledged(self):
        session = await self.session()
        try:
            packet_id = await session.publish("hermes/out", b'{"text":"hi"}', qos=1)
            self.assertEqual(packet_id, 1)
            body = self.broker.body_of(3)
            self.assertTrue(body.endswith(b'{"text":"hi"}'))
            self.assertEqual(body[2 : 2 + len("hermes/out")], b"hermes/out")
        finally:
            await session.aclose()

    async def test_publish_rejects_qos2(self):
        session = await self.session()
        try:
            with self.assertRaises(ValueError):
                await session.publish("hermes/out", b"x", qos=2)
            self.assertNotIn(3, self.broker.types())
        finally:
            await session.aclose()

    async def test_subscribe_rejects_qos2(self):
        session = await self.session()
        try:
            with self.assertRaises(ValueError):
                await session.subscribe("events/#", qos=2)
            self.assertNotIn(8, self.broker.types())
        finally:
            await session.aclose()

    async def test_inbound_publish_is_yielded_and_acked(self):
        session = await self.session()
        try:
            await session.subscribe("events/#")
            await self.broker.publish("events/sleep/x", b'{"topic":"sleep.x"}', qos=1, pid=42)
            async with asyncio.timeout(5):
                async for topic, payload, retained in session.messages():
                    self.assertEqual(topic, "events/sleep/x")
                    self.assertEqual(payload, b'{"topic":"sleep.x"}')
                    self.assertFalse(retained)
                    break
            await self.broker.wait_for_type(4)  # PUBACK came back
            self.assertEqual(struct.unpack("!H", self.broker.body_of(4)[:2])[0], 42)
        finally:
            await session.aclose()

    async def test_packet_split_across_reads_still_parses(self):
        session = await self.session()
        try:
            await session.subscribe("events/#")
            await self.broker.publish("events/a", b"x" * 300, qos=0, chunk=7)
            async with asyncio.timeout(5):
                async for topic, payload, _retained in session.messages():
                    self.assertEqual((topic, payload), ("events/a", b"x" * 300))
                    break
        finally:
            await session.aclose()

    async def test_keepalive_sends_pingreq(self):
        session = await MqttSession.open(
            "127.0.0.1", self.broker.port, client_id="ka", keepalive=1, timeout=5.0,
            protocol=4,
        )
        try:
            self.assertTrue(
                await wait_for_type(self.broker, 12),
                "no PINGREQ within the keepalive window",
            )
            for _ in range(50):  # let the PINGRESP be processed (keepalive is async)
                if not session._awaiting_pong:
                    break
                await asyncio.sleep(0.02)
            self.assertFalse(session._awaiting_pong, "PINGRESP was not processed")
        finally:
            await session.aclose()

    async def test_keepalive_zero_disables_ping(self):
        session = await MqttSession.open(
            "127.0.0.1", self.broker.port, client_id="ka0", keepalive=0, timeout=5.0,
            protocol=4,
        )
        try:
            await asyncio.sleep(0.5)
            self.assertNotIn(12, self.broker.types(), "keepalive=0 must not send PINGREQ")
        finally:
            await session.aclose()

    async def test_traffic_after_pingreq_keeps_session_alive(self):
        broker = await FakeBroker(answer_ping=False).__aenter__()
        self.addAsyncCleanup(broker.__aexit__)
        session = await MqttSession.open(
            "127.0.0.1", broker.port, client_id="ka2", keepalive=1, timeout=5.0,
            protocol=4,
        )
        try:
            await asyncio.sleep(1.2)          # PINGREQ sent; no PINGRESP
            await broker.publish("events/x", b"y", qos=0)
            await asyncio.sleep(1.2)          # a full keepalive window after the ping
            self.assertFalse(session._pump.done(), "pump died despite live traffic")
        finally:
            await session.aclose()

    async def test_truncated_publish_body_raises_mqtt_error(self):
        session = await self.session()
        try:
            with self.assertRaises(MqttError):
                await session._on_publish(0x32, b"\x00\x20abc")
        finally:
            await session.aclose()

    async def test_dead_broker_raises_mqtt_error(self):
        with self.assertRaises((MqttError, OSError)):
            await MqttSession.open("127.0.0.1", 1, client_id="x", timeout=2.0, protocol=4)


class V5SessionTests(unittest.IsolatedAsyncioTestCase):
    async def test_connect_is_level_5_with_property_block(self):
        broker = await FakeBroker(protocol=5).__aenter__()
        self.addAsyncCleanup(broker.__aexit__)
        session = await MqttSession.open(
            "127.0.0.1", broker.port, client_id="v5", protocol="5", timeout=5.0,
        )
        try:
            await broker.wait_connected()
            self.assertEqual(broker.level, 5)
            body = broker.connect_body
            self.assertEqual(body[6], 5)
            self.assertEqual(body[10], 0, "v5 CONNECT must carry an empty property block")
            self.assertEqual(session.protocol, mqtt_codec.PROTOCOL_5)
            # the rest of the payload is unchanged from 3.1.1: client id first
            self.assertEqual(body[11:], utf8("v5"))
        finally:
            await session.aclose()


class CONNACKLimitsTests(unittest.IsolatedAsyncioTestCase):
    async def test_v5_connack_limits_are_applied(self):
        broker = await FakeBroker(
            protocol=5, receive_maximum=3, server_keepalive=2,
            maximum_qos=1, retain_available=False,
        ).__aenter__()
        self.addAsyncCleanup(broker.__aexit__)
        session = await MqttSession.open(
            "127.0.0.1", broker.port, client_id="v5", protocol="5", timeout=5.0,
        )
        try:
            self.assertEqual(session.server_receive_maximum, 3)
            self.assertEqual(session.keepalive, 2)  # SERVER_KEEP_ALIVE wins
            self.assertEqual(session._send_slots._value, 3)
            self.assertEqual(session.server_max_qos, 1)
            self.assertFalse(session.server_retain_available)
        finally:
            await session.aclose()


class V5TestCase(unittest.IsolatedAsyncioTestCase):
    """Fake broker plus a level-5 session pinned to it."""

    async def broker(self, **kwargs) -> FakeBroker:
        broker = await FakeBroker(protocol=5, **kwargs).__aenter__()
        self.addAsyncCleanup(broker.__aexit__)
        return broker

    async def open(self, broker: FakeBroker) -> MqttSession:
        return await MqttSession.open(
            "127.0.0.1", broker.port, client_id="test-client", username="hermes",
            password="secret", timeout=5.0, keepalive=5, protocol="5",
        )


class ReasonCodeTests(V5TestCase):
    """v5 reason codes on the acknowledgements the session awaits."""

    async def test_suback_refusal_raises(self):
        broker = await self.broker(suback=0x87)
        session = await self.open(broker)
        try:
            with self.assertRaises(MqttError):
                await session.subscribe("events/#", qos=1)
        finally:
            await session.aclose()

    async def test_puback_refusal_raises(self):
        broker = await self.broker(puback_reason=0x87)
        session = await self.open(broker)
        try:
            with self.assertRaises(MqttError):
                await session.publish("hermes/out", b"x", qos=1)
        finally:
            await session.aclose()


class PumpTests(V5TestCase):
    """What the pump decodes and how it reports the end of the session."""

    async def test_inbound_v5_publish_payload_is_not_shifted(self):
        broker = await self.broker()
        session = await self.open(broker)
        try:
            await session.subscribe("events/#", qos=1)
            prop = bytes((0x26,)) + utf8("k") + utf8("v")
            body = utf8("events/x") + b"\x00\x07" + bytes((len(prop),)) + prop + b"hi"
            broker._writer.write(packet(3 << 4 | 2, body))
            await broker._writer.drain()
            async with asyncio.timeout(2):
                topic, payload, _ = await session.messages().__anext__()
            self.assertEqual((topic, payload), ("events/x", b"hi"))
        finally:
            await session.aclose()

    async def test_broker_disconnect_reason_is_reported(self):
        broker = await self.broker()
        session = await self.open(broker)
        try:
            await session.subscribe("events/#", qos=1)
            await broker.disconnect(0x8E)
            with self.assertRaisesRegex(MqttError, "0x8e"):
                async with asyncio.timeout(2):
                    await session.messages().__anext__()
        finally:
            await session.aclose()


class FlowControlTests(V5TestCase):
    """Outbound limits the broker advertises: packet size and Receive Maximum."""

    async def test_packet_over_broker_maximum_is_refused(self):
        broker = await self.broker()
        session = await self.open(broker)
        session.server_max_packet_size = 32
        try:
            with self.assertRaises(MqttError):
                await session.publish("hermes/out", b"x" * 64, qos=1)
            self.assertNotIn(3, broker.types())
        finally:
            await session.aclose()

    async def test_receive_maximum_serialises_inflight(self):
        broker = await self.broker(receive_maximum=1, auto_ack=False)
        session = await self.open(broker)
        try:
            first = asyncio.create_task(session.publish("hermes/out", b"1", qos=1))
            await broker.wait_for_type(3)
            second = asyncio.create_task(session.publish("hermes/out", b"2", qos=1))
            await asyncio.sleep(0.1)
            self.assertEqual(broker.types().count(3), 1, "second publish exceeded Receive Maximum")
            await broker.release(broker.pids[0])
            await first
            await asyncio.sleep(0.1)
            self.assertEqual(broker.types().count(3), 2)
            await broker.release(broker.pids[1])
            await second
        finally:
            await session.aclose()


class RefusalTests(unittest.IsolatedAsyncioTestCase):
    async def test_bad_credentials_raise_auth_error(self):
        async with FakeBroker(connack=4) as broker:
            with self.assertRaises(MqttAuthError):
                await MqttSession.open(
                    "127.0.0.1", broker.port, client_id="x", username="hermes",
                    password="wrong", timeout=5.0, protocol=4,
                )

    async def test_rejected_client_id_is_fatal(self):
        async with FakeBroker(connack=2) as broker:
            with self.assertRaises(MqttFatal):
                await MqttSession.open(
                    "127.0.0.1", broker.port, client_id="x", username="hermes",
                    password="pw", timeout=5.0, protocol=4,
                )

    async def test_explicit_refused_subscription_raises(self):
        async with FakeBroker(suback=0x80) as broker:
            session = await MqttSession.open(
                "127.0.0.1", broker.port, client_id="x", timeout=5.0, protocol=4,
            )
            try:
                with self.assertRaises(MqttError):
                    await session.subscribe("events/#")
            finally:
                await session.aclose()


class NegotiationTests(unittest.IsolatedAsyncioTestCase):
    async def test_auto_falls_back_to_311(self):
        broker = await FakeBroker(protocol=4, refuse_level_5=True).__aenter__()
        self.addAsyncCleanup(broker.__aexit__)
        session = await MqttSession.open(
            "127.0.0.1", broker.port, client_id="test-client", username="hermes",
            password="secret", timeout=5.0, keepalive=5, protocol="auto",
        )
        try:
            self.assertEqual(session.protocol, 4)
            self.assertEqual(broker.level, 4)
        finally:
            await session.aclose()


class ValidationTests(unittest.TestCase):
    def test_password_without_username_is_rejected(self):
        with self.assertRaises(ValueError):
            MqttSession("127.0.0.1", 1883, password="secret")

    def test_publish_rejects_invalid_topics(self):
        session = MqttSession("127.0.0.1", 1883)
        for bad in ("", "events/#", "events/+/x"):
            with self.assertRaises(ValueError, msg=bad):
                asyncio.run(session.publish(bad, b"x"))

    def test_subscribe_rejects_empty_filter(self):
        session = MqttSession("127.0.0.1", 1883)
        with self.assertRaises(ValueError):
            asyncio.run(session.subscribe(""))


class LimitEnforcementTests(V5TestCase):
    """A broker's advertised limits must change what the client is willing to send."""

    async def test_maximum_packet_size_is_recorded(self):
        broker = await self.broker(maximum_packet_size=50)
        session = await self.open(broker)
        try:
            self.assertEqual(session.server_max_packet_size, 50)
        finally:
            await session.aclose()

    async def test_publish_above_broker_max_qos_is_refused(self):
        broker = await self.broker(maximum_qos=0)
        session = await self.open(broker)
        try:
            with self.assertRaises(ValueError):
                await session.publish("hermes/out", b"x", qos=1)
            self.assertNotIn(3, broker.types())
        finally:
            await session.aclose()

    async def test_retain_refused_when_unavailable(self):
        broker = await self.broker(retain_available=False)
        session = await self.open(broker)
        try:
            with self.assertRaises(ValueError):
                await session.publish("hermes/out", b"x", qos=1, retain=True)
            self.assertNotIn(3, broker.types())
        finally:
            await session.aclose()


class ConnectAbortTests(unittest.IsolatedAsyncioTestCase):
    """A peer that dies before CONNACK must surface as MqttError, not EOFError."""

    async def _close_server(self, server):
        server.close()
        await server.wait_closed()

    async def test_eof_before_connack_is_an_mqtt_error(self):
        async def handler(reader, writer):
            try:
                await read_packet(reader)  # CONNECT
            except (asyncio.IncompleteReadError, ConnectionError):
                pass
            finally:
                writer.close()

        server = await asyncio.start_server(handler, "127.0.0.1", 0)
        self.addAsyncCleanup(self._close_server, server)
        port = server.sockets[0].getsockname()[1]
        with self.assertRaises(MqttError):
            await MqttSession.open("127.0.0.1", port, client_id="abort", timeout=5.0, protocol=4)


class PasswordOnlySessionTests(unittest.IsolatedAsyncioTestCase):
    async def test_v5_password_without_username_connects(self):
        broker = await FakeBroker(protocol=5).__aenter__()
        self.addAsyncCleanup(broker.__aexit__)
        session = await MqttSession.open(
            "127.0.0.1", broker.port, client_id="pw", username="", password="secret",
            protocol="5", timeout=5.0,
        )
        try:
            await broker.wait_connected()
            flags = broker.connect_body[7]
            self.assertTrue(flags & 0x40, "password flag not set")
            self.assertFalse(flags & 0x80, "username flag must stay clear")
        finally:
            await session.aclose()

    def test_v311_password_without_username_is_still_rejected(self):
        with self.assertRaises(ValueError):
            MqttSession("127.0.0.1", 1883, username="", password="secret", protocol=4)


class InvalidConnackLimitTests(unittest.IsolatedAsyncioTestCase):
    """v5 protocol errors in CONNACK must fail the connection instead of wedging the client."""

    async def _open_expecting_error(self, **limits):
        broker = await FakeBroker(protocol=5, **limits).__aenter__()
        self.addAsyncCleanup(broker.__aexit__)
        with self.assertRaises(MqttError):
            await MqttSession.open("127.0.0.1", broker.port, client_id="lim", protocol="5", timeout=5.0)

    async def test_receive_maximum_zero(self):
        await self._open_expecting_error(receive_maximum=0)

    async def test_maximum_packet_size_zero(self):
        await self._open_expecting_error(maximum_packet_size=0)

    async def test_maximum_qos_two(self):
        await self._open_expecting_error(maximum_qos=2)


class InboundGuardTests(unittest.IsolatedAsyncioTestCase):
    """Inbound hostile packets must end the session rather than be routed."""

    async def _session(self, max_inbound_packet=0):
        broker = await FakeBroker(protocol=4).__aenter__()
        self.addAsyncCleanup(broker.__aexit__)
        session = await MqttSession.open(
            "127.0.0.1", broker.port, client_id="guard", protocol=4, timeout=5.0,
            max_inbound_packet=max_inbound_packet,
        )
        self.addAsyncCleanup(session.aclose)
        await session.subscribe("events/#")
        return broker, session

    async def test_packet_over_the_inbound_cap_ends_the_session(self):
        broker, session = await self._session(max_inbound_packet=64)
        await broker.publish("events/big", b"x" * 256, qos=0)
        with self.assertRaises(MqttError):
            async with asyncio.timeout(5):
                await session.messages().__anext__()

    async def test_qos3_publish_ends_the_session(self):
        broker, session = await self._session()
        broker._writer.write(packet(0x36, utf8("events/x") + b"hi"))
        await broker._writer.drain()
        with self.assertRaises(MqttError):
            async with asyncio.timeout(5):
                await session.messages().__anext__()


class SubscriptionReasonTests(unittest.IsolatedAsyncioTestCase):
    async def test_refusal_has_its_own_type(self):
        broker = await FakeBroker(protocol=4, suback=0x80).__aenter__()
        self.addAsyncCleanup(broker.__aexit__)
        session = await MqttSession.open("127.0.0.1", broker.port, client_id="sub", protocol=4, timeout=5.0)
        self.addAsyncCleanup(session.aclose)
        with self.assertRaises(SubscriptionRefused):
            await session.subscribe("events/#")

    async def test_granted_qos_above_requested_is_an_error(self):
        broker = await FakeBroker(protocol=4, suback=2).__aenter__()
        self.addAsyncCleanup(broker.__aexit__)
        session = await MqttSession.open("127.0.0.1", broker.port, client_id="sub2", protocol=4, timeout=5.0)
        self.addAsyncCleanup(session.aclose)
        with self.assertRaises(MqttError):
            await session.subscribe("events/#", qos=1)


class KeepaliveObligationTests(unittest.IsolatedAsyncioTestCase):
    """Keepalive is about the packets WE send, not about inbound silence."""

    async def test_pingreq_fires_despite_continuous_inbound_traffic(self):
        broker = await FakeBroker(protocol=4).__aenter__()
        self.addAsyncCleanup(broker.__aexit__)
        session = await MqttSession.open(
            "127.0.0.1", broker.port, client_id="ka3", keepalive=1, timeout=5.0, protocol=4,
        )
        try:
            await session.subscribe("events/#")
            for _ in range(6):
                await broker.publish("events/tick", b"x", qos=0)
                await asyncio.sleep(0.3)
            self.assertTrue(
                await wait_for_type(broker, 12),
                "no PINGREQ while QoS 0 inbound traffic kept the read timer busy",
            )
        finally:
            await session.aclose()


class ReservedPacketTests(unittest.IsolatedAsyncioTestCase):
    async def test_reserved_packet_type_ends_the_session(self):
        broker = await FakeBroker(protocol=4).__aenter__()
        self.addAsyncCleanup(broker.__aexit__)
        session = await MqttSession.open("127.0.0.1", broker.port, client_id="res", protocol=4, timeout=5.0)
        self.addAsyncCleanup(session.aclose)
        await session.subscribe("events/#")
        broker._writer.write(packet(0x00, b""))  # reserved Control Packet type 0
        await broker._writer.drain()
        with self.assertRaises(MqttError):
            async with asyncio.timeout(5):
                await session.messages().__anext__()


class _ProtocolFussyBroker:
    """Accepts both levels but reproduces how a 3.1.1-only broker rejects a v5 CONNECT."""

    def __init__(self, mode: str, server_ssl: ssl.SSLContext | None = None):
        self.mode = mode
        self.server_ssl = server_ssl
        self.levels: list[int] = []
        self.port = 0
        self._server = None

    async def __aenter__(self):
        self._server = await asyncio.start_server(
            self._serve, "127.0.0.1", 0, ssl=self.server_ssl)
        self.port = self._server.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *exc):
        self._server.close()
        await self._server.wait_closed()

    async def _serve(self, reader, writer):
        try:
            _first, body = await read_packet(reader)
            self.levels.append(body[6])
            if body[6] == 5 and self.mode == "v3_connack":
                writer.write(packet(2 << 4, bytes((0, 1))))  # v3 CONNACK: bad level
                await writer.drain()
                return
            if self.mode == "silent_close_all":
                return  # close before CONNACK at every level
            if body[6] == 5 and self.mode == "silent_close":
                return  # v5 permits closing without a CONNACK
            writer.write(packet(2 << 4, bytes((0, 0))))
            await writer.drain()
            while True:
                first, _body = await read_packet(reader)
                if first >> 4 == 14:  # DISCONNECT
                    return
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()


class AutoNegotiationTests(unittest.IsolatedAsyncioTestCase):
    """`auto` must survive the two ways a 3.1.1-only broker rejects a level-5 CONNECT."""

    async def _open_auto(self, mode: str):
        broker = await _ProtocolFussyBroker(mode).__aenter__()
        self.addAsyncCleanup(broker.__aexit__)
        session = await MqttSession.open(
            "127.0.0.1", broker.port, client_id="auto", username="hermes",
            password="secret", timeout=5.0, protocol="auto",
        )
        self.addAsyncCleanup(session.aclose)
        return broker, session

    async def test_v3_connack_on_the_v5_attempt_falls_back(self):
        broker, session = await self._open_auto("v3_connack")
        self.assertEqual(session.protocol, 4)
        self.assertEqual(broker.levels, [5, 4])

    async def test_silent_close_on_the_v5_attempt_falls_back(self):
        broker, session = await self._open_auto("silent_close")
        self.assertEqual(session.protocol, 4)
        self.assertEqual(broker.levels, [5, 4])

    async def test_pinned_v5_against_a_v3_only_broker_is_fatal(self):
        from mqtt.mqtt_client import ProtocolUnsupported

        broker = await _ProtocolFussyBroker("v3_connack").__aenter__()
        self.addAsyncCleanup(broker.__aexit__)
        with self.assertRaises(ProtocolUnsupported):
            await MqttSession.open(
                "127.0.0.1", broker.port, client_id="pinned", protocol="5", timeout=5.0,
            )


class FrameIntegrityTests(unittest.IsolatedAsyncioTestCase):
    """A stalled mid-packet server read must never desync or lose the frame."""

    async def test_stalled_mid_packet_is_still_delivered_intact(self):
        # answer_ping=False keeps the broker from injecting a PINGRESP into the middle of
        # our deliberately half-written packet (a real broker never interleaves packets).
        broker = await FakeBroker(protocol=4, answer_ping=False).__aenter__()
        self.addAsyncCleanup(broker.__aexit__)
        session = await MqttSession.open(
            "127.0.0.1", broker.port, client_id="stall", keepalive=2, timeout=5.0, protocol=4,
        )
        self.addAsyncCleanup(session.aclose)
        await session.subscribe("events/#")
        data = packet(3 << 4, utf8("events/stall") + b'{"ok":true}')
        broker._writer.write(data[:1])  # first byte only
        await broker._writer.drain()
        await asyncio.sleep(2.5)  # longer than keepalive, mid-packet
        broker._writer.write(data[1:])
        await broker._writer.drain()
        async with asyncio.timeout(5):
            topic, payload, _ = await session.messages().__anext__()
        self.assertEqual(topic, "events/stall")
        self.assertEqual(payload, b'{"ok":true}')

    async def test_qos1_is_acknowledged_only_after_it_is_queued(self):
        broker = await FakeBroker(protocol=4).__aenter__()
        self.addAsyncCleanup(broker.__aexit__)
        session = await MqttSession.open(
            "127.0.0.1", broker.port, client_id="bp", keepalive=1,
            queue_size=1, timeout=5.0, protocol=4,
        )
        self.addAsyncCleanup(session.aclose)
        await session.subscribe("events/#")
        await broker.publish("events/first", b"1", qos=1, pid=11)
        await broker.publish("events/second", b"2", qos=1, pid=12)
        # The pump is blocked handing the second message to the consumer, but the
        # heartbeat task keeps the connection alive and only the queued message is acked.
        self.assertTrue(
            await wait_for_type(broker, 12),
            "no PINGREQ while the pump was backpressured",
        )
        self.assertEqual(broker.types().count(4), 1)
        first = await asyncio.wait_for(session.messages().__anext__(), 2)
        second = await asyncio.wait_for(session.messages().__anext__(), 2)
        self.assertEqual((first[0], second[0]), ("events/first", "events/second"))
        await asyncio.sleep(0.1)
        self.assertEqual(broker.types().count(4), 2)

    async def test_ack_timeout_releases_the_packet_id(self):
        broker = await FakeBroker(protocol=4, auto_ack=False).__aenter__()
        self.addAsyncCleanup(broker.__aexit__)
        session = await MqttSession.open(
            "127.0.0.1", broker.port, client_id="ack", timeout=5.0, protocol=4,
        )
        session.ack_timeout = 0.2
        self.addAsyncCleanup(session.aclose)
        with self.assertRaises(MqttError):
            await session.publish("hermes/out", b"x", qos=1)
        self.assertEqual(session._pending, {}, "a timed-out PUBACK leaked its pending future")

    async def test_suback_timeout_releases_the_packet_id(self):
        broker = await FakeBroker(protocol=4, auto_suback=False).__aenter__()
        self.addAsyncCleanup(broker.__aexit__)
        session = await MqttSession.open(
            "127.0.0.1", broker.port, client_id="ack2", timeout=5.0, protocol=4,
        )
        session.ack_timeout = 0.2
        self.addAsyncCleanup(session.aclose)
        with self.assertRaises(MqttError):
            await session.subscribe("events/#")
        self.assertEqual(session._pending, {})

    async def test_malformed_packet_flags_end_the_session(self):
        broker = await FakeBroker(protocol=4, auto_ack=False).__aenter__()
        self.addAsyncCleanup(broker.__aexit__)
        session = await MqttSession.open(
            "127.0.0.1", broker.port, client_id="flags", protocol=4, timeout=5.0,
        )
        self.addAsyncCleanup(session.aclose)
        await session.subscribe("events/#")
        broker._writer.write(packet(0x41, struct.pack("!H", 1)))  # PUBACK with flags set
        await broker._writer.drain()
        with self.assertRaises(MqttError):
            async with asyncio.timeout(5):
                await session.messages().__anext__()


class ConnackValidationTests(unittest.IsolatedAsyncioTestCase):
    async def _expect_open_error(self, **broker_kwargs):
        broker = await FakeBroker(protocol=5, **broker_kwargs).__aenter__()
        self.addAsyncCleanup(broker.__aexit__)
        with self.assertRaises(MqttError):
            await MqttSession.open("127.0.0.1", broker.port, client_id="v", protocol="5", timeout=5.0)

    async def test_retain_available_must_be_zero_or_one(self):
        await self._expect_open_error(retain_available=2)

    async def test_duplicate_receive_maximum_is_rejected(self):
        extra = bytes((0x21,)) + struct.pack("!H", 2)  # second Receive Maximum
        await self._expect_open_error(receive_maximum=5, extra_connack_props=extra)

    async def test_session_present_after_clean_start_is_rejected(self):
        await self._expect_open_error(connack_flags=1)


class InboundQos2Tests(unittest.IsolatedAsyncioTestCase):
    async def test_qos2_publish_ends_the_session(self):
        broker = await FakeBroker(protocol=4).__aenter__()
        self.addAsyncCleanup(broker.__aexit__)
        session = await MqttSession.open("127.0.0.1", broker.port, client_id="q2", protocol=4, timeout=5.0)
        self.addAsyncCleanup(session.aclose)
        await session.subscribe("events/#")
        broker._writer.write(packet(0x34, utf8("events/q2") + b"\x00\x07hi"))
        await broker._writer.drain()
        with self.assertRaises(MqttError):
            async with asyncio.timeout(5):
                await session.messages().__anext__()


if __name__ == "__main__":
    unittest.main()
