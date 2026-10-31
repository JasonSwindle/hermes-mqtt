"""Router tests: envelope parsing, prefix policy matching, dedupe, and mode dispatch.

No broker, no gateway: the adapter is built with a real ``PlatformConfig`` and its
delivery leg is replaced with a recorder, so a policy regression fails here rather
than in production silence.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _bootstrap  # noqa: E402,F401  (registers the package however the repo is checked out)

from gateway.config import HomeChannel, Platform, PlatformConfig  # noqa: E402

from mqtt.adapter import (  # noqa: E402
    MAX_DEDUPE_KEY_CHARS,
    MAX_LOGGED_DROPS,
    BrokerUrl,
    MqttAdapter,
    _Dedup,
    _as_bool,
    _bounded_event_id,
    _env_enablement,
    _parse_send_target,
    _timestamp,
    _validate_config,
    is_valid_topic_filter,
    coerce_policy,
    envelope,
    parse_url,
    register,
    render,
    resolve_policy,
)

from mqtt.mqtt_client import MqttError, SubscriptionRefused  # noqa: E402
from mqtt.mqtt_codec import normalize_protocol  # noqa: E402

ENVELOPE = {
    "source": "hass",
    "event_id": "abc123",
    "occurred_at": "2026-10-26T01:02:03-04:00",
    "topic": "sleep.metrics",
    "summary": "AHI 3.2, usage 7h",
    "detail": {"ahi": 3.2},
}

TABLE = {
    "sleep.": {"mode": "agent"},
    "sleep.metrics": {"mode": "deliver", "deliver": "telegram", "chat_id": "chat-1"},
    "job.": {"mode": "job", "job_id": "c0ffee"},
    "test.": "drop",
}


def payload(data: dict) -> bytes:
    return json.dumps(data).encode()


class UrlTests(unittest.TestCase):
    def test_plaintext_schemes_and_bare_host(self):
        self.assertEqual(parse_url("tcp://mqtt.example.com:1883"),
                         BrokerUrl("mqtt.example.com", 1883, False))
        self.assertEqual(parse_url("mqtt://broker"), BrokerUrl("broker", 1883, False))
        self.assertEqual(parse_url("broker"), BrokerUrl("broker", 1883, False))
        self.assertEqual(parse_url("tcp://broker:1234"), BrokerUrl("broker", 1234, False))

    def test_tls_schemes_and_default_port(self):
        for scheme in ("mqtts", "tls", "ssl"):
            self.assertEqual(parse_url(f"{scheme}://broker"), BrokerUrl("broker", 8883, True), scheme)
        self.assertEqual(parse_url("mqtts://broker:1883"), BrokerUrl("broker", 1883, True))
        self.assertEqual(parse_url("mqtts://broker:8884"), BrokerUrl("broker", 8884, True))

    def test_refuses_garbage_and_url_credentials(self):
        for bad in ("http://broker", "", "tcp://", "://broker"):
            with self.assertRaises(ValueError, msg=bad):
                parse_url(bad)
        with self.assertRaises(ValueError):
            parse_url("mqtts://user:secret@broker:8883")


class PolicyTests(unittest.TestCase):
    def test_longest_prefix_wins(self):
        policy, prefix = resolve_policy("sleep.metrics", TABLE, {"mode": "drop"})
        self.assertEqual((policy["mode"], prefix), ("deliver", "sleep.metrics"))
        policy, prefix = resolve_policy("sleep.other", TABLE, {"mode": "drop"})
        self.assertEqual((policy["mode"], prefix), ("agent", "sleep."))

    def test_keys_are_prefixes_not_paths(self):
        """A key matches everything it prefixes — 'sleep.metrics' also catches 'sleep.metrics.x'.

        That is what "prefix" means here, and the reason the documented keys end in a dot
        ('sleep.') when the author means a path boundary.
        """
        policy, prefix = resolve_policy("sleep.metrics.nightly", TABLE, {"mode": "drop"})
        self.assertEqual((policy["mode"], prefix), ("deliver", "sleep.metrics"))
        policy, prefix = resolve_policy("sleeping", TABLE, {"mode": "drop"})
        self.assertEqual((policy["mode"], prefix), ("drop", ""))

    def test_unmatched_uses_default_and_never_escalates(self):
        policy, prefix = resolve_policy("unraid.disk1", TABLE, {"mode": "drop"})
        self.assertEqual((policy["mode"], prefix), ("drop", ""))
        self.assertEqual(resolve_policy("anything", {}, {"mode": "drop"})[0]["mode"], "drop")

    def test_unknown_mode_degrades_to_drop(self):
        self.assertEqual(coerce_policy("yolo")["mode"], "drop")
        self.assertEqual(coerce_policy({"mode": "Agent"})["mode"], "agent")
        self.assertEqual(coerce_policy(None)["mode"], "drop")


class EnvelopeTests(unittest.TestCase):
    def test_parses_bus_envelope(self):
        values = envelope(mqtt_topic="events/sleep.metrics", payload=payload(ENVELOPE))
        self.assertEqual(values["topic"], "sleep.metrics")
        self.assertEqual(values["source"], "hass")
        self.assertEqual(values["summary"], "AHI 3.2, usage 7h")
        self.assertEqual(values["mqtt_topic"], "events/sleep.metrics")
        self.assertNotIn("event_id", values["json"])  # router fields are not repeated in the payload

    def test_non_json_body_still_routes(self):
        values = envelope(mqtt_topic="events/x", payload=b"plain text\n")
        self.assertEqual(values["summary"], "plain text")
        self.assertEqual(values["topic"], "events/x")

    def test_missing_summary_falls_back_to_json(self):
        values = envelope(mqtt_topic="events/x", payload=payload({"temp": 21.5}))
        self.assertEqual(values["summary"], '{"temp":21.5}')

    def test_render_is_forgiving(self):
        self.assertEqual(render("{topic}: {summary}", {"topic": "a", "summary": "b"}), "a: b")
        self.assertEqual(render("{nope}|{topic}", {"topic": "a"}), "|a")
        self.assertEqual(render("{unclosed", {"summary": "fallback"}), "fallback")


class DedupTests(unittest.TestCase):
    def test_second_sighting_is_duplicate(self):
        dedup = _Dedup()
        self.assertFalse(dedup.seen("evt-1"))
        self.assertTrue(dedup.seen("evt-1"))
        self.assertFalse(dedup.seen("evt-2"))
        self.assertFalse(dedup.seen(""))  # no key → never a duplicate

    def test_bounded(self):
        dedup = _Dedup(ttl=60, max_entries=3)
        for n in range(10):
            dedup.seen(f"evt-{n}")
        self.assertLessEqual(len(dedup._seen), 4)


class AdapterRoutingTests(unittest.IsolatedAsyncioTestCase):
    """Drive _handle() directly; record what each mode would do."""

    def make(self, topics=None, **extra) -> MqttAdapter:
        config = PlatformConfig(
            enabled=True,
            extra={
                "url": "tcp://broker:1883",
                "username": "hermes",
                "password": "secret",
                "topics": topics if topics is not None else TABLE,
                **extra,
            },
        )
        adapter = MqttAdapter(config)
        adapter.delivered = []
        adapter.agent_events = []
        adapter.jobs = []

        async def fake_deliver(text, target):
            adapter.delivered.append((text, target))

            class Result:
                success = True
                error = None

            return Result()

        async def fake_agent(values, mqtt_topic, policy, prefix=""):
            adapter.agent_events.append(values["topic"])

        async def fake_job(values, mqtt_topic, policy):
            adapter.jobs.append(policy.get("job_id"))

        adapter._deliver = fake_deliver
        adapter._agent_mode = fake_agent
        adapter._job_mode = fake_job
        return adapter

    async def test_deliver_mode_routes_without_an_agent_turn(self):
        adapter = self.make()
        await adapter._handle("events/sleep.metrics", payload(ENVELOPE))
        self.assertEqual(len(adapter.delivered), 1)
        text, target = adapter.delivered[0]
        self.assertEqual(text, "sleep.metrics: AHI 3.2, usage 7h")
        self.assertEqual(target["chat_id"], "chat-1")
        self.assertEqual(adapter.agent_events, [])
        self.assertEqual(adapter._counts["deliver"], 1)

    async def test_agent_mode_uses_longest_prefix(self):
        adapter = self.make()
        event = {**ENVELOPE, "topic": "sleep.nightly", "event_id": "agent-1"}
        await adapter._handle("events/sleep.nightly", payload(event))
        self.assertEqual(adapter.agent_events, ["sleep.nightly"])

    async def test_unmatched_topic_is_dropped_silently(self):
        adapter = self.make()
        await adapter._handle("events/unraid/disk", payload({"event_id": "x", "topic": "unraid.disk"}))
        self.assertEqual(adapter.delivered, [])
        self.assertEqual(adapter.agent_events, [])
        self.assertEqual(adapter._counts["drop"], 1)

    async def test_unmatched_drop_logs_once_per_topic(self):
        """Default-drop topics must not log a line per message: the dedupe key for an
        unmatched topic is the wire topic, because the matched prefix is empty."""
        adapter = self.make()
        with self.assertLogs("mqtt.adapter", level="INFO") as captured:
            for i in range(3):
                await adapter._handle(
                    "events/noisy.topic",
                    payload({"event_id": f"noisy-{i}", "topic": "noisy.topic"}),
                )
        drops = [r.getMessage() for r in captured.records if "dropping" in r.getMessage()]
        self.assertEqual(len(drops), 1, f"expected one line for three messages, got {drops}")
        self.assertEqual(adapter._counts["drop"], 3)

    async def test_our_own_outbound_topic_is_ignored(self):
        adapter = self.make()
        await adapter._handle("hermes/out", payload({"topic": "hermes/out", "event_id": "e"}))
        self.assertEqual(sum(adapter._counts.values()), 0)

    async def test_retained_message_is_ignored_by_default(self):
        adapter = self.make()
        await adapter._handle("events/sleep.metrics", payload(ENVELOPE), retained=True)
        self.assertEqual(adapter.delivered, [])
        self.assertEqual(adapter._counts["retained_skipped"], 1)

    async def test_retained_message_can_be_accepted(self):
        adapter = self.make(accept_retained=True)
        await adapter._handle("events/sleep.metrics", payload(ENVELOPE), retained=True)
        self.assertEqual(len(adapter.delivered), 1)

    async def test_duplicate_event_id_is_handled_once(self):
        adapter = self.make()
        await adapter._handle("events/sleep.metrics", payload(ENVELOPE))
        await adapter._handle("events/sleep.metrics", payload(ENVELOPE))
        self.assertEqual(len(adapter.delivered), 1)

    async def test_job_mode_reads_job_id(self):
        adapter = self.make()
        await adapter._handle("events/job.nightly", payload({"event_id": "j1", "topic": "job.nightly"}))
        self.assertEqual(adapter.jobs, ["c0ffee"])

    async def test_job_mode_without_id_is_an_error_not_a_crash(self):
        config = PlatformConfig(enabled=True, extra={"url": "tcp://broker:1883", "password": "x"})
        adapter = MqttAdapter(config)
        await adapter._job_mode(
            {"topic": "job.nightly", "json": "{}"}, "events/job.nightly", {"mode": "job"}
        )
        self.assertEqual(adapter._counts["errors"], 1)


class ConfigTests(unittest.IsolatedAsyncioTestCase):
    async def test_missing_password_refuses_to_connect(self):
        config = PlatformConfig(enabled=True, extra={"url": "tcp://broker:1883", "topics": TABLE})
        adapter = MqttAdapter(config)
        with mock.patch("mqtt.adapter._get_scoped_secret", return_value=""):
            self.assertFalse(await adapter.connect())
        self.assertFalse(adapter.is_connected)
        self.assertEqual(adapter._fatal_error_code, "mqtt_no_password")

    async def test_bad_url_is_fatal_and_connect_refuses(self):
        config = PlatformConfig(enabled=True, extra={"url": "http://broker:1883", "password": "x"})
        adapter = MqttAdapter(config)
        self.assertEqual(adapter._host, "")
        self.assertFalse(await adapter.connect())
        self.assertEqual(adapter._fatal_error_code, "mqtt_no_url")


class ProtocolConfigTests(unittest.TestCase):
    def test_values(self):
        self.assertEqual(normalize_protocol(None), "auto")
        self.assertEqual(normalize_protocol("5"), "5")
        self.assertEqual(normalize_protocol("3.1.1"), "3.1.1")
        self.assertEqual(normalize_protocol("nonsense"), "auto")


class TlsConfigTests(unittest.TestCase):
    def _adapter(self, **extra) -> MqttAdapter:
        base = {"url": "mqtts://broker:8883", "username": "hermes", "password": "secret"}
        base.update(extra)
        return MqttAdapter(PlatformConfig(enabled=True, extra=base))

    def test_mqtts_url_is_valid(self):
        self.assertTrue(_validate_config(PlatformConfig(
            enabled=True, extra={"url": "mqtts://broker:8883", "password": "p"})))

    def test_mutual_tls_needs_both_halves(self):
        for extra in ({"tls_cert_file": "/tmp/client.pem"},
                      {"tls_key_file": "/tmp/client.key"}):
            config = PlatformConfig(
                enabled=True,
                extra={"url": "mqtts://broker:8883", "password": "p", **extra},
            )
            self.assertFalse(_validate_config(config), extra)
        self.assertTrue(_validate_config(PlatformConfig(
            enabled=True,
            extra={"url": "mqtts://broker:8883", "password": "p",
                   "tls_cert_file": "/tmp/client.pem", "tls_key_file": "/tmp/client.key"},
        )))

    def test_bad_insecure_flag_is_invalid(self):
        config = PlatformConfig(
            enabled=True,
            extra={"url": "mqtts://broker:8883", "password": "p", "tls_insecure": "maybe"},
        )
        self.assertFalse(_validate_config(config))

    def test_non_boolean_insecure_flag_fails_closed(self):
        for value in (2, 0.5, {"a": 1}, ["on"]):
            config = PlatformConfig(
                enabled=True,
                extra={"url": "mqtts://broker:8883", "password": "p", "tls_insecure": value},
            )
            self.assertFalse(_validate_config(config), value)

    def test_boolean_like_insecure_values_are_accepted(self):
        for value in (True, False, "true", "false", "on", "off", "1", "0"):
            config = PlatformConfig(
                enabled=True,
                extra={"url": "mqtts://broker:8883", "password": "p", "tls_insecure": value},
            )
            self.assertTrue(_validate_config(config), value)

    def test_plaintext_url_with_tls_settings_logs_an_ignore_warning(self):
        with self.assertLogs("mqtt.adapter", level="WARNING") as captured:
            MqttAdapter(PlatformConfig(
                enabled=True, extra={"url": "tcp://broker:1883", "password": "p",
                                     "tls_ca_file": "/ca.pem"}))
        self.assertTrue(
            any("ignoring" in record.getMessage() for record in captured.records),
            "TLS settings on a plaintext URL must warn that they are ignored",
        )

    def test_url_credentials_are_invalid_config(self):
        config = PlatformConfig(
            enabled=True,
            extra={"url": "mqtts://user:hunter2@broker:8883", "password": "p"},
        )
        self.assertFalse(_validate_config(config))

    def test_tls_fields_are_parsed_for_a_tls_url(self):
        adapter = self._adapter(
            tls_ca_file="/etc/hermes/ca.pem",
            tls_server_name="mqtt.internal",
            tls_insecure="true",
        )
        self.assertTrue(adapter._tls)
        self.assertEqual((adapter._host, adapter._port), ("broker", 8883))
        self.assertEqual(adapter._tls_ca_file, "/etc/hermes/ca.pem")
        self.assertEqual(adapter._tls_server_name, "mqtt.internal")
        self.assertTrue(adapter._tls_insecure)

    def test_plaintext_url_has_no_tls(self):
        adapter = MqttAdapter(PlatformConfig(
            enabled=True, extra={"url": "tcp://broker:1883", "password": "p"}))
        self.assertFalse(adapter._tls)
        self.assertEqual(adapter._port, 1883)

    def test_plaintext_url_ignores_tls_settings(self):
        self.assertTrue(_validate_config(PlatformConfig(
            enabled=True,
            extra={"url": "tcp://broker:1883", "password": "p",
                   "tls_cert_file": "/tmp/client.pem"},
        )))

    def test_env_enablement_seeds_tls_settings(self):
        env = {
            "MQTT_HERMES_URL": "mqtts://broker:8883",
            "MQTT_HERMES_TLS_CA_FILE": "/etc/hermes/ca.pem",
            "MQTT_HERMES_TLS_CERT_FILE": "/etc/hermes/client.pem",
            "MQTT_HERMES_TLS_KEY_FILE": "/etc/hermes/client.key",
            "MQTT_HERMES_TLS_SERVER_NAME": "mqtt.internal",
            "MQTT_HERMES_TLS_INSECURE": "false",
            "MQTT_HERMES_TLS_KEY_PASSWORD": "not-a-real-secret",
        }
        with mock.patch(
            "mqtt.adapter._get_scoped_secret",
            side_effect=lambda key, default="": env.get(key, default),
        ):
            seed = _env_enablement()
        self.assertEqual(seed["tls_ca_file"], "/etc/hermes/ca.pem")
        self.assertEqual(seed["tls_cert_file"], "/etc/hermes/client.pem")
        self.assertEqual(seed["tls_server_name"], "mqtt.internal")
        self.assertIn("tls_insecure", seed)
        self.assertNotIn("tls_key_password", seed)  # stays a runtime scoped secret


class TlsEnvPairingTests(unittest.TestCase):
    def test_env_only_mutual_tls_half_is_invalid(self):
        config = PlatformConfig(
            enabled=True,
            extra={"url": "mqtts://broker:8883", "password": "p"},
        )
        env = {"MQTT_HERMES_TLS_CERT_FILE": "/etc/hermes/client.pem"}
        with mock.patch(
            "mqtt.adapter._get_scoped_secret",
            side_effect=lambda key, default="": env.get(key, default),
        ):
            self.assertFalse(_validate_config(config))


class SendTargetParserTests(unittest.TestCase):
    """The parser handed to register_platform for send_message refs."""

    def test_strips_surrounding_space(self):
        self.assertEqual(_parse_send_target("  chat-42 \n"), ("chat-42", None))

    def test_blank_is_not_a_target(self):
        self.assertIsNone(_parse_send_target("   "))
        self.assertIsNone(_parse_send_target(""))


class JobModeGuardTests(unittest.IsolatedAsyncioTestCase):
    """job mode must degrade, not crash the pump, when the cron seam is gone."""

    async def test_missing_cron_seam_counts_an_error(self):
        config = PlatformConfig(
            enabled=True,
            extra={"url": "tcp://broker:1883", "username": "hermes", "password": "secret"},
        )
        adapter = MqttAdapter(config)
        with mock.patch.dict(sys.modules, {"tools.cronjob_tools": None}):
            await adapter._job_mode(
                dict(ENVELOPE), "events/job/nightly", {"mode": "job", "job_id": "nightly"}
            )
        self.assertEqual(adapter._counts["errors"], 1)


def _adapter(**extra) -> MqttAdapter:
    base = {"url": "tcp://broker:1883", "username": "hermes", "password": "secret"}
    base.update(extra)
    return MqttAdapter(PlatformConfig(enabled=True, extra=base))


class OwnTopicGuardTests(unittest.IsolatedAsyncioTestCase):
    """The own-topic guard is the publish subtree only — not its parent namespace."""

    def make(self) -> MqttAdapter:
        adapter = _adapter(
            publish_topic="hermes/out",
            topics={"state.": {"mode": "deliver", "deliver": "telegram", "chat_id": "ops"}},
        )
        adapter.delivered = []

        async def fake_deliver(text, target):
            adapter.delivered.append((text, target))

            class Result:
                success = True
                error = None

            return Result()

        adapter._deliver = fake_deliver
        return adapter

    def test_guard_covers_publish_topic_and_descendants(self):
        adapter = self.make()
        self.assertTrue(adapter._is_own_topic("hermes/out"))
        self.assertTrue(adapter._is_own_topic("hermes/out/reply"))
        self.assertFalse(adapter._is_own_topic("hermes/state/x"))
        self.assertFalse(adapter._is_own_topic("hermes/outbox"))

    async def test_sibling_topic_is_routed_not_swallowed(self):
        adapter = self.make()
        await adapter._handle(
            "hermes/state/x", payload({"event_id": "s1", "topic": "state.x", "summary": "ok"})
        )
        self.assertEqual(len(adapter.delivered), 1)

    async def test_the_publish_topic_itself_is_still_ignored(self):
        adapter = self.make()
        await adapter._handle("hermes/out", payload({"event_id": "s2", "topic": "state.x"}))
        self.assertEqual(adapter.delivered, [])


class DeliveryTargetTests(unittest.IsolatedAsyncioTestCase):
    """Cross-platform delivery must use the routed adapter's profile-scoped home channel."""

    def make(self):
        return _adapter()

    def harness(self, home_channel):
        sent = []

        class TargetAdapter:
            config = type("Config", (), {"home_channel": home_channel})()

            async def send(self, chat_id, text, metadata=None):
                sent.append((chat_id, text, metadata))

                class Result:
                    success = True
                    error = None

                return Result()

        class Runner:
            config = mock.Mock()
            config.get_home_channel = mock.Mock(
                side_effect=AssertionError("default-profile config must not be consulted")
            )

            def _authorization_adapter(self, platform, profile):
                return TargetAdapter()

        adapter = self.make()
        adapter.gateway_runner = Runner()
        return adapter, sent

    async def test_routed_adapter_home_channel_wins(self):
        home = HomeChannel(platform=Platform.TELEGRAM, chat_id="home-42", name="ops")
        adapter, sent = self.harness(home)
        result = await adapter._deliver("hi", {"platform": "telegram", "profile": "work"})
        self.assertTrue(result.success)
        self.assertEqual(sent, [("home-42", "hi", None)])

    async def test_profiled_policy_never_falls_back_to_the_default_home(self):
        adapter, sent = self.harness(None)
        result = await adapter._deliver("hi", {"platform": "telegram", "profile": "work"})
        self.assertFalse(result.success)
        self.assertIn("no home channel", result.error)
        self.assertEqual(sent, [])

    async def test_missing_deliver_platform_is_a_config_error(self):
        adapter = _adapter()
        adapter.delivered = []

        async def fake_deliver(text, target):
            adapter.delivered.append(text)

        adapter._deliver = fake_deliver
        await adapter._deliver_mode(
            {"topic": "x.y", "summary": "s", "json": "{}"}, {"mode": "deliver", "chat_id": "c"}
        )
        self.assertEqual(adapter.delivered, [])
        self.assertEqual(adapter._counts["errors"], 1)

    async def test_local_is_not_a_gateway_adapter(self):
        result = await self.make()._deliver("hi", {"platform": "local"})
        self.assertFalse(result.success)
        self.assertIn("not a gateway adapter", result.error)

    async def test_thread_id_rides_in_send_metadata(self):
        home = HomeChannel(platform=Platform.TELEGRAM, chat_id="home-42", name="ops")
        adapter, sent = self.harness(home)
        await adapter._deliver("hi", {"platform": "telegram", "thread_id": "7"})
        self.assertEqual(sent[0][2], {"thread_id": "7"})


class EventHardeningTests(unittest.IsolatedAsyncioTestCase):
    """Bus payload text is data: it must not be able to drive gateway commands."""

    def capture(self) -> MqttAdapter:
        adapter = _adapter(topics={"alert.": {"mode": "agent"}})
        adapter.events = []

        async def capture_event(event):
            adapter.events.append(event)

        adapter.handle_message = capture_event
        return adapter

    async def test_agent_session_identity_comes_from_config_not_payload(self):
        adapter = self.capture()
        values = envelope(mqtt_topic="events/alert.x", payload=payload(
            {"event_id": "e2", "topic": "alert.x", "summary": "hi"}))
        await adapter._agent_mode(values, "events/alert.x", {"mode": "agent"}, "alert.")
        self.assertEqual(adapter.events[0].source.chat_id, "alert.")

    async def test_agent_event_cannot_control_the_gateway(self):
        adapter = self.capture()
        values = envelope(
            mqtt_topic="events/alert.x",
            payload=payload(
                {"event_id": "e1", "topic": "alert.x", "summary": "/update", "occurred_at": "2026-10-26T01:02:03"}
            ),
        )
        await adapter._agent_mode(values, "events/alert.x", {"mode": "agent"})
        event = adapter.events[0]
        self.assertFalse(event.allow_gateway_control)
        self.assertIsNotNone(event.timestamp.tzinfo)

    def test_naive_timestamp_is_normalised_to_utc(self):
        self.assertEqual(_timestamp("2026-10-26T01:02:03").utcoffset().total_seconds(), 0)

    def test_garbage_timestamp_falls_back_to_aware_now(self):
        self.assertIsNotNone(_timestamp("not-a-timestamp").tzinfo)


class DedupeIdentityTests(unittest.IsolatedAsyncioTestCase):
    def test_oversized_event_id_is_hashed_to_a_fixed_size_key(self):
        short = "evt-1"
        self.assertEqual(_bounded_event_id(short), short)
        long_id = "x" * 5000
        hashed = _bounded_event_id(long_id)
        self.assertEqual(hashed, _bounded_event_id(long_id))
        self.assertLessEqual(len(hashed), MAX_DEDUPE_KEY_CHARS)

    async def test_payloads_without_event_id_are_all_delivered(self):
        # A periodic producer publishing an identical body without an event_id must
        # not be silenced for an hour by a content fingerprint.
        adapter = AdapterRoutingTests().make()
        body = payload({"topic": "sleep.metrics", "summary": "no event id"})
        await adapter._handle("events/sleep.metrics", body)
        await adapter._handle("events/sleep.metrics", body)
        self.assertEqual(len(adapter.delivered), 2)


class EnvelopeScalarTests(unittest.TestCase):
    def test_json_string_body_becomes_summary(self):
        values = envelope(mqtt_topic="events/x", payload=json.dumps("hello").encode())
        self.assertEqual(values["summary"], "hello")


class ConfigValidationTests(unittest.TestCase):
    def test_wildcard_publish_topic_is_invalid(self):
        config = PlatformConfig(
            enabled=True,
            extra={"url": "tcp://broker:1883", "publish_topic": "hermes/#", "password": "p"},
        )
        self.assertFalse(_validate_config(config))

    def test_non_numeric_packet_cap_is_invalid(self):
        config = PlatformConfig(
            enabled=True,
            extra={"url": "tcp://broker:1883", "max_packet_bytes": "lots", "password": "p"},
        )
        self.assertFalse(_validate_config(config))

    def test_non_numeric_render_cap_is_invalid(self):
        config = PlatformConfig(
            enabled=True,
            extra={"url": "tcp://broker:1883", "max_render_chars": "big", "password": "p"},
        )
        self.assertFalse(_validate_config(config))

    def test_valid_config_still_passes(self):
        self.assertTrue(_validate_config(PlatformConfig(enabled=True, extra={"url": "tcp://broker:1883"})))

    def test_malformed_subscribe_filter_is_invalid(self):
        for bad in ("events/#/x", "events+/x", "events/+x", "#/events"):
            config = PlatformConfig(
                enabled=True,
                extra={"url": "tcp://broker:1883", "subscribe": bad, "password": "p"},
            )
            self.assertFalse(_validate_config(config), bad)

    def test_valid_filters(self):
        for good in ("events/#", "events/+", "+", "#", "home/+/events", "$share/g/events/#"):
            self.assertTrue(is_valid_topic_filter(good), good)

    def test_string_false_is_false(self):
        self.assertFalse(_as_bool("false"))
        self.assertFalse(_as_bool("0"))
        self.assertTrue(_as_bool("true"))
        self.assertTrue(_as_bool(True))
        self.assertFalse(_as_bool(None))


class DropLogBoundTests(unittest.IsolatedAsyncioTestCase):
    async def test_drop_log_is_bounded(self):
        adapter = _adapter()
        for n in range(MAX_LOGGED_DROPS + 10):
            await adapter._handle(
                f"events/noise.{n}", payload({"topic": f"noise.{n}", "event_id": f"n-{n}"})
            )
        self.assertLessEqual(len(adapter._logged_drops), MAX_LOGGED_DROPS)


class _FakeSession:
    protocol = 4

    def __init__(self):
        self.closed = False

    def messages(self):
        async def pump():
            while True:
                await asyncio.sleep(0.05)
                yield ("events/x", b"{}", False)

        return pump()

    async def aclose(self):
        self.closed = True


class ConnectLifecycleTests(unittest.IsolatedAsyncioTestCase):
    """connect() must be idempotent and must not stay poisoned by an old fatal verdict."""

    def make(self) -> MqttAdapter:
        adapter = _adapter()
        self.sessions = []

        async def fake_open(*, retrying: bool = False):
            session = _FakeSession()
            self.sessions.append(session)
            adapter._mark_connected()
            return session

        adapter._open = fake_open
        self.addAsyncCleanup(adapter.disconnect)
        return adapter

    async def test_second_connect_does_not_spawn_a_second_consumer(self):
        adapter = self.make()
        self.assertTrue(await adapter.connect())
        supervisor, dispatcher = adapter._supervisor, adapter._dispatcher
        self.assertTrue(await adapter.connect())
        self.assertIs(adapter._supervisor, supervisor)
        self.assertIs(adapter._dispatcher, dispatcher)
        consumers = [t for t in asyncio.all_tasks() if t.get_name() == "mqtt-dispatch"]
        self.assertEqual(len(consumers), 1)

    async def test_connect_clears_a_previous_fatal_verdict(self):
        adapter = self.make()
        adapter._fatal = True
        adapter._running = False
        self.assertTrue(await adapter.connect())
        self.assertFalse(adapter._fatal)
        self.assertFalse(adapter._supervisor.done())

    async def test_runtime_fatal_notifies_the_host(self):
        adapter = _adapter()
        adapter._running = True
        adapter._fatal = True
        notified = []

        async def fake_notify():
            notified.append(True)

        adapter._notify_fatal_error = fake_notify
        await adapter._supervise(_FakeSession())
        self.assertEqual(notified, [True])


class SubscribeClassificationTests(unittest.IsolatedAsyncioTestCase):
    """A refused subscription is fatal; a transient SUBACK failure must keep retrying."""

    class _Session:
        protocol = 4

        def __init__(self, error):
            self.error = error
            self.closed = False

        async def subscribe(self, *args, **kwargs):
            raise self.error

        async def aclose(self):
            self.closed = True

    async def _open_with(self, error):
        adapter = _adapter()
        session = self._Session(error)
        with mock.patch("mqtt.adapter.MqttSession.open", new=mock.AsyncMock(return_value=session)):
            self.assertIsNone(await adapter._open())
        return adapter, session

    async def test_refusal_is_fatal(self):
        adapter, session = await self._open_with(SubscriptionRefused("denied"))
        self.assertTrue(adapter._fatal)
        self.assertEqual(adapter._fatal_error_code, "mqtt_subscribe_refused")
        self.assertTrue(session.closed)

    async def test_transient_failure_is_not_fatal(self):
        adapter, session = await self._open_with(MqttError("timed out waiting for SUBACK"))
        self.assertFalse(adapter._fatal)
        self.assertIsNone(adapter._fatal_error_code)
        self.assertTrue(session.closed)


class SubscribeFilterGuardTests(unittest.IsolatedAsyncioTestCase):
    async def test_bad_filter_is_fatal_before_connecting(self):
        adapter = _adapter(subscribe="events/#/x")
        with mock.patch("mqtt.adapter.MqttSession.open", new=mock.AsyncMock()) as opened:
            self.assertIsNone(await adapter._open())
        opened.assert_not_awaited()
        self.assertTrue(adapter._fatal)
        self.assertEqual(adapter._fatal_error_code, "mqtt_bad_subscribe_filter")


class RegistrationTests(unittest.TestCase):
    def test_event_bus_does_not_allow_update_commands(self):
        captured = {}

        class Ctx:
            def register_platform(self, **kwargs):
                captured.update(kwargs)

        register(Ctx())
        self.assertEqual(captured["name"], "mqtt")
        self.assertFalse(captured["allow_update_command"])
        self.assertTrue(captured["trusted_inbound"])

    def test_long_messages_are_not_truncated_by_the_router(self):
        adapter = _adapter()
        self.assertTrue(adapter.splits_long_messages)

    def test_env_enablement_seeds_protocol_and_publish_topic(self):
        env = {
            "MQTT_HERMES_URL": "tcp://broker:1883",
            "MQTT_HERMES_PROTOCOL": "5",
            "MQTT_PUBLISH_TOPIC": "hermes/replies",
        }
        with mock.patch(
            "mqtt.adapter._get_scoped_secret",
            side_effect=lambda key, default="": env.get(key, default),
        ):
            seed = _env_enablement()
        self.assertEqual(seed["protocol"], "5")
        self.assertEqual(seed["publish_topic"], "hermes/replies")

    def test_env_enablement_is_none_without_a_url(self):
        with mock.patch("mqtt.adapter._get_scoped_secret", return_value=""):
            self.assertIsNone(_env_enablement())


class HostDedupeCarryTests(unittest.TestCase):
    """The gateway rebuilds a failed adapter; its dedupe must carry over, not reset empty."""

    def test_dedupe_is_carried_to_a_rebuilt_adapter(self):
        from gateway.platforms.helpers import MessageDeduplicator, carry_inbound_dedup

        previous = _adapter()
        self.assertIsInstance(previous._dedup, MessageDeduplicator)
        previous._dedup.seen("evt-42")
        rebuilt = _adapter()
        carry_inbound_dedup(previous, rebuilt)
        self.assertTrue(rebuilt._dedup.seen("evt-42"))


class RenderCapTests(unittest.IsolatedAsyncioTestCase):
    """One huge payload must not become a huge prompt or delivery."""

    def make(self) -> MqttAdapter:
        adapter = _adapter(
            max_render_chars=20,
            topics={"big.": {"mode": "deliver", "deliver": "telegram", "chat_id": "ops"}},
        )
        adapter.delivered = []

        async def fake_deliver(text, target):
            adapter.delivered.append(text)

            class Result:
                success = True
                error = None

            return Result()

        adapter._deliver = fake_deliver
        return adapter

    async def test_delivered_text_is_capped(self):
        adapter = self.make()
        await adapter._handle(
            "events/big.x",
            payload({"event_id": "r1", "topic": "big.x", "summary": "y" * 500}),
        )
        text = adapter.delivered[0]
        self.assertTrue(text.startswith("big.x: "))
        self.assertIn("truncated", text)
        self.assertLess(len(text), 100)


class DedupeKeyBoundTests(unittest.IsolatedAsyncioTestCase):
    async def test_oversized_event_id_is_hashed_and_still_dedupes(self):
        adapter = AdapterRoutingTests().make()
        long_id = "x" * 5000
        body = payload({"event_id": long_id, "topic": "sleep.metrics", "summary": "a"})
        await adapter._handle("events/sleep.metrics", body)
        await adapter._handle("events/sleep.metrics", body)
        self.assertEqual(len(adapter.delivered), 1)
        self.assertLessEqual(max(len(key) for key in adapter._dedup._seen), MAX_DEDUPE_KEY_CHARS)


class ReconnectBackoffTests(unittest.IsolatedAsyncioTestCase):
    """The documented 2/5/15/30/60 s ladder must actually start at 2 s."""

    async def test_first_reconnect_uses_the_first_backoff_step(self):
        class FailingSession:
            protocol = 4

            def messages(self):
                async def pump():
                    raise MqttError("dropped")
                    yield  # pragma: no cover - makes this an async generator

                return pump()

            async def aclose(self):
                return None

        adapter = _adapter()
        adapter._running = True
        attempts = []

        async def fake_retry(attempt):
            attempts.append(attempt)
            return None

        adapter._await_retry = fake_retry
        with mock.patch.object(adapter, "_write_runtime_status_safe"):
            await adapter._supervise(FailingSession())
        self.assertEqual(attempts, [0])


if __name__ == "__main__":
    unittest.main()
