"""End-to-end: a real MQTT wire, the real session pump, the real router, one stubbed sink.

Everything below the router is production code — FakeBroker speaks the wire protocol on a
loopback socket, ``MqttSession`` parses it, ``_supervise`` moves packets into the inbox and
``_dispatch_loop`` routes them. Only ``_deliver`` is replaced, because the far side of a
delivery is another platform, not this plugin.
"""

from __future__ import annotations

import asyncio
import json
import sys
import unittest
from unittest.mock import patch

import _bootstrap  # noqa: E402,F401  (registers the package however the repo is checked out)

from gateway.config import PlatformConfig  # noqa: E402

from mqtt.adapter import MqttAdapter  # noqa: E402
from mqtt.mqtt_client import MqttSession  # noqa: E402

from test_mqtt_client import FakeBroker  # noqa: E402


def envelope(topic: str) -> bytes:
    return json.dumps(
        {
            "source": "e2e",
            "event_id": f"e2e-{topic}",
            "occurred_at": "2026-10-26T13:00:00-04:00",
            "topic": topic,
            "summary": f"{topic} happened",
            "value": 42,
        }
    ).encode("utf-8")


class WireEndToEndTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.broker = FakeBroker(protocol=5)
        await self.broker.__aenter__()
        self.tasks: list[asyncio.Task] = []

    async def asyncTearDown(self) -> None:
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        await self.broker.__aexit__(None, None, None)

    def make_adapter(self, topics: dict, default: dict | None = None) -> MqttAdapter:
        config = PlatformConfig(
            enabled=True,
            extra={
                "url": f"tcp://127.0.0.1:{self.broker.port}",
                "username": "e2e",
                "password": "e2e",
                "topics": topics,
                "default": default or {"mode": "drop"},
            },
        )
        adapter = MqttAdapter(config)
        adapter.delivered: list[tuple[str, dict]] = []

        async def deliver(text, target):
            adapter.delivered.append((text, target))

            class Result:
                success = True
                error = None

            return Result()

        adapter._deliver = deliver  # the only stub: the platform on the far side
        adapter._running = True
        return adapter

    async def start_pipeline(self, adapter: MqttAdapter) -> None:
        session = await MqttSession.open("127.0.0.1", self.broker.port, protocol="5")
        await session.subscribe("events/#", 1)
        self.tasks.append(asyncio.create_task(adapter._supervise(session)))
        self.tasks.append(asyncio.create_task(adapter._dispatch_loop()))
        await asyncio.sleep(0.1)

    async def wait_for(self, predicate, timeout: float = 5.0) -> bool:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while loop.time() < deadline:
            if predicate():
                return True
            await asyncio.sleep(0.05)
        return False

    async def test_event_travels_from_broker_to_deliver(self) -> None:
        adapter = self.make_adapter(
            {"test.": {"mode": "deliver", "deliver": "local", "chat_id": "chat-e2e"}}
        )
        await self.start_pipeline(adapter)

        await self.broker.publish("events/test.hello", envelope("test.hello"))

        self.assertTrue(
            await self.wait_for(lambda: bool(adapter.delivered)),
            "the event never reached deliver within 5s",
        )
        text, target = adapter.delivered[0]
        self.assertIn("test.hello", text)
        self.assertIn("test.hello happened", text)
        self.assertEqual(target["chat_id"], "chat-e2e")

    async def test_unmatched_topic_is_dropped(self) -> None:
        adapter = self.make_adapter({"test.": {"mode": "deliver", "deliver": "local"}})
        await self.start_pipeline(adapter)

        await self.broker.publish("events/other.thing", envelope("other.thing"))
        await asyncio.sleep(1.0)

        self.assertEqual(adapter.delivered, [], "an unmatched topic must never be delivered")

    async def test_a_dropped_session_reconnects_instead_of_ending_the_supervisor(self) -> None:
        """A broker restart must cost a reconnect, not the consumer's life."""
        adapter = self.make_adapter(
            {"test.": {"mode": "deliver", "deliver": "local", "chat_id": "chat-e2e"}}
        )
        await self.start_pipeline(adapter)
        # The fixture hands `_supervise` a session directly, so `_session` only becomes set when a
        # reconnect goes through `_open()` — exactly the behaviour under test.
        self.assertIsNone(adapter._session)

        with patch("mqtt.adapter.RECONNECT_BACKOFF", (0.05,)):
            await self.broker.disconnect()
            reconnected = await self.wait_for(lambda: adapter._session is not None, timeout=6.0)

        self.assertTrue(reconnected, "the supervisor exited on its first drop instead of reconnecting")

        await self.broker.publish("events/test.after", envelope("test.after"))
        self.assertTrue(
            await self.wait_for(lambda: bool(adapter.delivered)),
            "nothing was routed after the reconnect — the consumer came back deaf",
        )


if __name__ == "__main__":
    unittest.main()
