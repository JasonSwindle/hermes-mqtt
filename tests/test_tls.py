"""TLS transport tests: a real TLS handshake on loopback, no mocks of the TLS layer.

The certificates under ``tests/fixtures/tls`` are throwaway, test-only fixtures (their
private keys are committed deliberately — see that directory's README). This lets the
suite exercise certificate verification, hostname checking, mTLS and the adapter's fatal
classification without a live broker or a third-party dependency.
"""

from __future__ import annotations

import asyncio
import json
import os
import ssl
import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _bootstrap  # noqa: E402,F401  (registers the package however the repo is checked out)

from gateway.config import PlatformConfig  # noqa: E402

from mqtt.adapter import MqttAdapter  # noqa: E402
from mqtt.mqtt_client import (  # noqa: E402
    MqttSession,
    MqttTlsError,
    MqttTlsLost,
    build_tls_context,
)

from test_mqtt_client import FakeBroker, _ProtocolFussyBroker  # noqa: E402

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "tls"
CA = str(FIXTURES / "ca.pem")
SERVER_CERT = str(FIXTURES / "server.pem")
SERVER_KEY = str(FIXTURES / "server-key.pem")
CLIENT_CERT = str(FIXTURES / "client.pem")
CLIENT_KEY = str(FIXTURES / "client-key.pem")
ENCRYPTED_CLIENT_KEY = str(FIXTURES / "client-key-encrypted.pem")
KEY_PASSWORD = "test-key-password"


def server_context(*, require_client: bool = False) -> ssl.SSLContext:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(SERVER_CERT, SERVER_KEY)
    if require_client:
        context.verify_mode = ssl.CERT_REQUIRED
        context.load_verify_locations(CA)
    return context


def secure_client_context(**overrides) -> ssl.SSLContext:
    options = {"ca_file": CA}
    options.update(overrides)
    return build_tls_context(**options)


def envelope(topic: str) -> bytes:
    return json.dumps(
        {
            "source": "tls-e2e",
            "event_id": f"tls-{topic}",
            "occurred_at": "2026-10-26T13:00:00-04:00",
            "topic": topic,
            "summary": f"{topic} happened",
        }
    ).encode("utf-8")


class ContextTests(unittest.TestCase):
    """What build_tls_context guarantees before a socket is opened."""

    def test_secure_by_default(self):
        context = build_tls_context(ca_file=CA)
        self.assertTrue(context.check_hostname)
        self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)

    def test_insecure_disables_both_checks(self):
        context = build_tls_context(insecure=True)
        self.assertFalse(context.check_hostname)
        self.assertEqual(context.verify_mode, ssl.CERT_NONE)

    def test_tls_1_2_is_the_floor_in_both_modes(self):
        for context in (build_tls_context(ca_file=CA), build_tls_context(insecure=True)):
            self.assertEqual(context.minimum_version, ssl.TLSVersion.TLSv1_2)

    def test_cert_and_key_must_be_set_together(self):
        with self.assertRaises(ValueError):
            build_tls_context(ca_file=CA, cert_file=CLIENT_CERT)
        with self.assertRaises(ValueError):
            build_tls_context(ca_file=CA, key_file=CLIENT_KEY)

    def test_missing_files_are_config_errors_that_name_the_setting(self):
        with self.assertRaisesRegex(ValueError, "tls_ca_file"):
            build_tls_context(ca_file=str(FIXTURES / "missing-ca.pem"))
        with self.assertRaisesRegex(ValueError, "tls_cert_file"):
            build_tls_context(
                ca_file=CA,
                cert_file=str(FIXTURES / "missing-client.pem"),
                key_file=CLIENT_KEY,
            )
        with self.assertRaisesRegex(ValueError, "tls_key_file"):
            build_tls_context(
                ca_file=CA,
                cert_file=CLIENT_CERT,
                key_file=str(FIXTURES / "missing-key.pem"),
            )

    def test_encrypted_key_without_password_is_rejected_without_prompting(self):
        with self.assertRaisesRegex(ValueError, "tls_key_password"):
            build_tls_context(ca_file=CA, cert_file=CLIENT_CERT, key_file=ENCRYPTED_CLIENT_KEY)

    def test_encrypted_key_with_password_loads(self):
        context = build_tls_context(
            ca_file=CA, cert_file=CLIENT_CERT, key_file=ENCRYPTED_CLIENT_KEY,
            key_password=KEY_PASSWORD)
        self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)

    def test_wrong_key_password_is_a_config_error(self):
        with self.assertRaisesRegex(ValueError, "tls_cert_file|tls_key_file"):
            build_tls_context(
                ca_file=CA, cert_file=CLIENT_CERT, key_file=ENCRYPTED_CLIENT_KEY,
                key_password="wrong-password")

    def test_key_password_never_appears_in_error_messages(self):
        secret = "correct-horse-battery-staple"
        with self.assertRaises(ValueError) as caught:
            build_tls_context(
                ca_file=CA, cert_file=CLIENT_CERT, key_file=ENCRYPTED_CLIENT_KEY,
                key_password=secret)
        self.assertNotIn(secret, str(caught.exception))

    def test_client_identity_loads(self):
        context = build_tls_context(ca_file=CA, cert_file=CLIENT_CERT, key_file=CLIENT_KEY)
        self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)


class _QuietExpectedTlsFailures:
    """Negative TLS tests make the server side fail its handshake; don't spam stderr."""

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self._loop = asyncio.get_running_loop()
        self._old_exception_handler = self._loop.get_exception_handler()

        def handler(loop, context):
            exc = context.get("exception")
            if isinstance(exc, (ConnectionResetError, ssl.SSLError)):
                return
            message = str(context.get("message", "")).lower()
            if "ssl" in message or "handshake" in message:
                return
            if self._old_exception_handler is not None:
                self._old_exception_handler(loop, context)
            else:
                loop.default_exception_handler(context)

        self._loop.set_exception_handler(handler)
        self.addAsyncCleanup(self._restore_exception_handler)

    async def _restore_exception_handler(self):
        self._loop.set_exception_handler(self._old_exception_handler)


class TlsSessionTests(_QuietExpectedTlsFailures, unittest.IsolatedAsyncioTestCase):
    async def _broker(self, *, protocol: int = 4, require_client: bool = False) -> FakeBroker:
        broker = await FakeBroker(
            protocol=protocol, server_ssl=server_context(require_client=require_client)
        ).__aenter__()
        self.addAsyncCleanup(broker.__aexit__)
        return broker

    async def _session(self, broker: FakeBroker, *, protocol: str = "auto",
                       context: ssl.SSLContext | None = None,
                       server_hostname: str = "localhost") -> MqttSession:
        session = await MqttSession.open(
            "127.0.0.1", broker.port,
            client_id="tls", username="hermes", password="secret",
            protocol=protocol, timeout=5.0,
            ssl_context=context or secure_client_context(),
            server_hostname=server_hostname,
        )
        self.addAsyncCleanup(session.aclose)
        return session

    async def test_round_trip_over_tls_mqtt_311(self):
        broker = await self._broker(protocol=4)
        session = await self._session(broker, protocol="3.1.1")
        self.assertEqual(session.protocol, 4)
        self.assertEqual(await session.subscribe("events/#"), 1)
        await broker.publish("events/tls", b'{"ok":true}', qos=1, pid=21)
        async with asyncio.timeout(5):
            topic, payload, _retained = await session.messages().__anext__()
        self.assertEqual((topic, payload), ("events/tls", b'{"ok":true}'))
        await asyncio.sleep(0.05)
        self.assertIn(4, broker.types(), "the inbound QoS 1 PUBLISH was not PUBACKed")

    async def test_round_trip_over_tls_mqtt_5(self):
        broker = await self._broker(protocol=5)
        session = await self._session(broker, protocol="5")
        self.assertEqual(session.protocol, 5)
        await session.subscribe("events/#")
        await broker.publish("events/tls5", b"v5", qos=1, pid=22)
        async with asyncio.timeout(5):
            topic, _payload, _retained = await session.messages().__anext__()
        self.assertEqual(topic, "events/tls5")

    async def test_untrusted_certificate_is_a_tls_error(self):
        broker = await self._broker()
        with self.assertRaises(MqttTlsError):
            await self._session(broker, context=ssl.create_default_context())

    async def test_hostname_mismatch_is_a_tls_error(self):
        broker = await self._broker()
        with self.assertRaises(MqttTlsError):
            await self._session(broker, server_hostname="wrong.invalid")

    async def test_ip_host_verifies_against_the_ip_san(self):
        broker = await self._broker()
        session = await self._session(broker, server_hostname="127.0.0.1")
        self.assertFalse(session._closed)

    async def test_insecure_mode_accepts_a_self_signed_certificate(self):
        broker = await self._broker()
        session = await self._session(broker, context=build_tls_context(insecure=True))
        self.assertFalse(session._closed)

    async def test_mutual_tls_with_a_client_certificate(self):
        broker = await self._broker(require_client=True)
        session = await self._session(
            broker,
            context=build_tls_context(
                ca_file=CA, cert_file=CLIENT_CERT, key_file=CLIENT_KEY),
        )
        self.assertFalse(session._closed)

    async def test_mutual_tls_without_a_client_certificate_is_a_tls_error(self):
        broker = await self._broker(require_client=True)
        # The server rejects the handshake. A lost CONNACK over TLS may still use the MQTT
        # level ladder, but every rung must end TLS-classified: never a plaintext retry, and
        # the final failure must be a TLS error.
        with self.assertRaises(MqttTlsError):
            await self._session(broker, protocol="auto")

    async def test_mutual_tls_with_an_encrypted_client_key(self):
        broker = await self._broker(require_client=True)
        session = await self._session(
            broker,
            context=build_tls_context(
                ca_file=CA, cert_file=CLIENT_CERT, key_file=ENCRYPTED_CLIENT_KEY,
                key_password=KEY_PASSWORD),
        )
        self.assertFalse(session._closed)


class TlsNegotiationTests(_QuietExpectedTlsFailures, unittest.IsolatedAsyncioTestCase):
    """The TLS layer must not break the MQTT protocol ladder, nor mask TLS failures."""

    async def _fussy(self, mode: str) -> _ProtocolFussyBroker:
        broker = await _ProtocolFussyBroker(
            mode, server_ssl=server_context()).__aenter__()
        self.addAsyncCleanup(broker.__aexit__)
        return broker

    async def _open_auto(self, broker):
        session = await MqttSession.open(
            "127.0.0.1", broker.port, client_id="tls-auto", username="hermes",
            password="secret", protocol="auto", timeout=5.0,
            ssl_context=secure_client_context(), server_hostname="localhost",
        )
        self.addAsyncCleanup(session.aclose)
        return session

    async def test_auto_falls_back_when_a_broker_closes_v5(self):
        # A 3.1.1-only broker that accepts TLS then closes on a level-5 CONNECT must
        # still get its level-4 retry; this must not be mistaken for a TLS failure.
        broker = await self._fussy("silent_close")
        session = await self._open_auto(broker)
        self.assertEqual(session.protocol, 4)
        self.assertEqual(broker.levels, [5, 4])

    async def test_auto_falls_back_on_a_v3_connack_over_tls(self):
        broker = await self._fussy("v3_connack")
        session = await self._open_auto(broker)
        self.assertEqual(session.protocol, 4)
        self.assertEqual(broker.levels, [5, 4])

    async def test_record_level_tls_error_does_not_try_another_level(self):
        calls = []

        async def fake_start(session, timeout):
            calls.append(session.protocol)
            raise MqttTlsError("certificate verification failed")

        with mock.patch.object(MqttSession, "_start", fake_start):
            with self.assertRaises(MqttTlsError):
                await MqttSession.open("127.0.0.1", 1, protocol="auto", timeout=0.1)
        self.assertEqual(len(calls), 1, "a record-level TLS error must not try another level")

    async def test_certificate_verification_failure_does_not_use_the_ladder(self):
        broker = await FakeBroker(protocol=5, server_ssl=server_context()).__aenter__()
        self.addAsyncCleanup(broker.__aexit__)
        with self.assertRaises(MqttTlsError) as caught:
            await MqttSession.open(
                "127.0.0.1", broker.port, client_id="tls-bad", protocol="auto",
                timeout=5.0, ssl_context=ssl.create_default_context(),
                server_hostname="localhost",
            )
        self.assertNotIsInstance(caught.exception, MqttTlsLost)


class TlsAdapterTests(_QuietExpectedTlsFailures, unittest.IsolatedAsyncioTestCase):
    """The adapter's config → context → connect → route path over real TLS."""

    async def _broker(self, *, protocol: int = 5, require_client: bool = False) -> FakeBroker:
        broker = await FakeBroker(
            protocol=protocol, server_ssl=server_context(require_client=require_client)
        ).__aenter__()
        self.addAsyncCleanup(broker.__aexit__)
        return broker

    def _adapter(self, broker: FakeBroker, **extra) -> MqttAdapter:
        config = PlatformConfig(
            enabled=True,
            extra={
                "url": f"mqtts://127.0.0.1:{broker.port}",
                "username": "e2e",
                "password": "e2e",
                "tls_ca_file": CA,
                "topics": {"test.": {"mode": "deliver", "deliver": "telegram", "chat_id": "c"}},
                "default": {"mode": "drop"},
                **extra,
            },
        )
        adapter = MqttAdapter(config)
        adapter.delivered = []

        async def fake_deliver(text, target):
            adapter.delivered.append((text, target))

            class Result:
                success = True
                error = None

            return Result()

        adapter._deliver = fake_deliver
        return adapter

    async def test_adapter_connects_and_routes_over_tls(self):
        broker = await self._broker()
        adapter = self._adapter(broker)
        self.addAsyncCleanup(adapter.disconnect)
        self.assertTrue(await adapter.connect())
        await broker.publish("events/test.hello", envelope("test.hello"), qos=1, pid=31)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 5
        while not adapter.delivered and loop.time() < deadline:
            await asyncio.sleep(0.02)
        self.assertTrue(adapter.delivered, "the event never reached deliver over TLS")
        text, target = adapter.delivered[0]
        self.assertIn("test.hello", text)
        self.assertEqual(target["chat_id"], "c")

    async def test_missing_ca_file_is_a_fatal_config_error_before_dialling(self):
        broker = await self._broker()
        adapter = self._adapter(broker, tls_ca_file=str(FIXTURES / "missing-ca.pem"))
        self.assertFalse(await adapter.connect())
        self.assertEqual(adapter._fatal_error_code, "mqtt_tls_failed")
        self.assertFalse(adapter.fatal_error_retryable)
        self.assertIsNone(broker._writer, "the adapter dialled despite a bad TLS config")

    async def test_untrusted_server_certificate_is_fatal(self):
        broker = await self._broker()
        adapter = self._adapter(broker)
        adapter._tls_ca_file = ""  # system trust store cannot verify the fixture CA
        self.assertFalse(await adapter.connect())
        self.assertEqual(adapter._fatal_error_code, "mqtt_tls_failed")
        self.assertFalse(adapter.fatal_error_retryable)

    async def test_tls_failure_is_fatal_on_a_first_connect(self):
        broker = await self._broker()
        adapter = self._adapter(broker)
        adapter._tls_ca_file = ""
        self.assertFalse(await adapter.connect())
        self.assertEqual(adapter._fatal_error_code, "mqtt_tls_failed")
        self.assertFalse(adapter.fatal_error_retryable)

    async def test_tls_failure_stays_retryable_on_a_host_reconnect(self):
        # The gateway's reconnect watcher calls connect(is_reconnect=True); the adapter
        # must forward that to _open so a renewed cert/secret can recover.
        broker = await self._broker()
        adapter = self._adapter(broker)
        adapter._tls_ca_file = ""
        self.assertFalse(await adapter.connect(is_reconnect=True))
        self.assertEqual(adapter._fatal_error_code, "mqtt_tls_failed")
        self.assertTrue(adapter.fatal_error_retryable)

    async def test_mtls_failure_is_reported_as_a_tls_error(self):
        broker = await self._broker(require_client=True)
        adapter = self._adapter(broker)  # no client certificate configured
        self.assertFalse(await adapter.connect())
        self.assertEqual(adapter._fatal_error_code, "mqtt_tls_failed")
        self.assertTrue(adapter._fatal)
        # A CONNACK loss over TLS is ambiguous (bad identity vs flaky peer), so it stays
        # retryable even on a first connect rather than permanently parking the platform.
        self.assertTrue(adapter.fatal_error_retryable)

    async def test_tls_connack_loss_is_retryable_on_first_connect(self):
        broker = await _ProtocolFussyBroker(
            "silent_close_all", server_ssl=server_context()).__aenter__()
        self.addAsyncCleanup(broker.__aexit__)
        adapter = MqttAdapter(PlatformConfig(enabled=True, extra={
            "url": f"mqtts://127.0.0.1:{broker.port}",
            "username": "e2e", "password": "e2e", "tls_ca_file": CA,
        }))
        self.assertFalse(await adapter.connect())
        self.assertEqual(adapter._fatal_error_code, "mqtt_tls_failed")
        self.assertTrue(adapter.fatal_error_retryable)

    async def test_insecure_adapter_connects_with_a_self_signed_broker(self):
        broker = await self._broker()
        adapter = self._adapter(broker, tls_ca_file="", tls_insecure=True)
        with self.assertLogs("mqtt.adapter", level="WARNING") as captured:
            self.assertTrue(await adapter.connect())
        self.addAsyncCleanup(adapter.disconnect)
        disabled = [r for r in captured.records if "DISABLED" in r.getMessage()]
        self.assertEqual(len(disabled), 1, "insecure TLS must warn exactly once per adapter")

    async def test_invalid_insecure_flag_is_a_fatal_config_error(self):
        broker = await self._broker()
        adapter = self._adapter(broker, tls_insecure=2)
        self.assertFalse(await adapter.connect())
        self.assertEqual(adapter._fatal_error_code, "mqtt_tls_failed")
        self.assertFalse(adapter.fatal_error_retryable)


if __name__ == "__main__":
    unittest.main()
