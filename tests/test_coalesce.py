"""Burst-coalescing tests for the MQTT platform adapter (0.7.0).

No broker, no gateway: ``_handle()`` is driven directly with a real ``PlatformConfig``.
The agent sink is the real ``_agent_mode`` path with ``handle_message`` replaced by a
recorder, the job sink is ``tools.cronjob_tools.execute_job_for_event`` injected into
``sys.modules``, and the deliver sink is ``_deliver`` replaced by a recorder — so each
assertion is on what the adapter actually dispatches, not on internals.

No fixed sleeps: a flush is awaited through ``asyncio.Event``/a polling helper with a
deadline. The window under test is 0.05 s; a 60 s window is used when the test wants a
pending bucket that must not flush on its own.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import types
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _bootstrap  # noqa: E402,F401  (registers the package however the repo is checked out)

from gateway.config import PlatformConfig  # noqa: E402

from mqtt.adapter import MqttAdapter  # noqa: E402

WINDOW = 0.05
LONG_WINDOW = 60


def payload(topic: str, marker: str, event_id: str = "") -> bytes:
    """One bus event whose summary carries a unique marker the rendered batch can be
    checked against (agent rendering includes the payload JSON; job/deliver rendering
    includes the summary verbatim)."""
    data = {
        "topic": topic,
        "source": "hass",
        "summary": marker,
        "occurred_at": "2026-10-26T01:02:03-04:00",
    }
    if event_id:
        data["event_id"] = event_id
    return json.dumps(data).encode()


def cron_seam():
    """Inject fake ``tools``/``tools.cronjob_tools`` modules and record the calls.

    Returns ``(patcher, calls)`` where ``calls`` collects ``(job_id, context)`` from a
    thread (``_job_mode`` dispatches through ``asyncio.to_thread``).
    """
    tools = types.ModuleType("tools")
    cronjob_tools = types.ModuleType("tools.cronjob_tools")
    calls: list[tuple[str, str]] = []

    def execute_job_for_event(job_id, context):
        calls.append((job_id, context))
        return {"success": True, "claimed": True}

    cronjob_tools.execute_job_for_event = execute_job_for_event
    tools.cronjob_tools = cronjob_tools
    return mock.patch.dict(
        sys.modules, {"tools": tools, "tools.cronjob_tools": cronjob_tools}
    ), calls


class CoalesceTestCase(unittest.IsolatedAsyncioTestCase):
    """Shared harness: build an adapter and record what each sink receives."""

    def make(self, topics, **extra) -> MqttAdapter:
        adapter = MqttAdapter(PlatformConfig(
            enabled=True,
            extra={
                "url": "tcp://broker:1883",
                "username": "hermes",
                "password": "secret",
                "topics": topics,
                **extra,
            },
        ))
        adapter.events = []
        adapter.deliveries = []
        # Tests never connect; keep disconnects hermetic (no runtime-status file writes).
        adapter._write_runtime_status_safe = mock.Mock()

        async def record_event(event):
            adapter.events.append(event)

        async def record_delivery(text, target):
            adapter.deliveries.append((text, target))

            class Result:
                success = True
                error = None

            return Result()

        adapter.handle_message = record_event
        adapter._deliver = record_delivery
        self.addAsyncCleanup(adapter.disconnect)
        return adapter

    async def wait_for(self, predicate, *, message: str, timeout: float = 2.0) -> None:
        """Poll until ``predicate()`` is true; fail with ``message`` after ``timeout`` s."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while loop.time() < deadline:
            if predicate():
                return
            await asyncio.sleep(0.01)
        self.fail(message)

    async def wait_for_events(self, adapter, count: int) -> None:
        await self.wait_for(
            lambda: len(adapter.events) >= count,
            message=f"expected at least {count} agent action(s), got {len(adapter.events)}",
        )

    def send(self, adapter, topic: str, marker: str, event_id: str = "", retained: bool = False):
        return adapter._handle(f"events/{topic}", payload(topic, marker, event_id), retained)


# --- the 10 mandatory tests (spec section 5) ----------------------------------------


class MandatoryCoalesceTests(CoalesceTestCase):
    # 1. 5 messages under one prefix with coalesce 0.05 -> exactly 1 agent turn, all 5.
    # Falsifier: remove the batch flush -> 5 turns.
    async def test_burst_under_one_prefix_becomes_one_agent_turn(self):
        adapter = self.make({"cloud.": {"mode": "agent", "coalesce": WINDOW}})
        with self.assertLogs("mqtt.adapter", level="INFO") as captured:
            for i in range(5):
                await self.send(adapter, f"cloud.{i}", f"E{i}", f"evt-{i}")
            await self.wait_for_events(adapter, 1)
        self.assertEqual(len(adapter.events), 1, "a burst must be one action, not one per message")
        event = adapter.events[0]
        self.assertEqual(event.message_id, "evt-0", "message_id is the first event's event_id")
        self.assertFalse(event.allow_gateway_control)
        text = event.text
        self.assertTrue(
            text.startswith("5 events on cloud. in 0.05s\n"),
            f"batch header wrong: {text[:120]!r}",
        )
        for i in range(5):
            self.assertIn(f"E{i}", text)
        positions = [text.index(f"E{i}") for i in range(5)]
        self.assertEqual(positions, sorted(positions), "batch must preserve arrival order")
        self.assertEqual(adapter._counts["agent"], 1, "counts.agent counts actions, not messages")
        self.assertEqual(adapter._counts["coalesced_messages"], 5)
        flushes = [r.getMessage() for r in captured.records if "coalesced" in r.getMessage()]
        self.assertEqual(len(flushes), 1, flushes)
        self.assertIn("5 messages on cloud.", flushes[0])
        self.assertIn("→ agent", flushes[0])

    # 2. 1 message, window elapses -> 1 action.
    # Falsifier: never flush on timer.
    async def test_single_message_flushes_when_the_window_elapses(self):
        adapter = self.make({"cloud.": {"mode": "agent", "coalesce": WINDOW}})
        await self.send(adapter, "cloud.a", "SOLO", "evt-solo")
        self.assertEqual(adapter.events, [], "the event must be buffered first, not dispatched")
        await self.wait_for_events(adapter, 1)
        self.assertEqual(len(adapter.events), 1)
        text = adapter.events[0].text
        self.assertTrue(
            text.startswith("1 events on cloud. in 0.05s\n"),
            f"batch header wrong: {text[:120]!r}",
        )
        self.assertIn("SOLO", text)
        self.assertEqual(adapter._counts["agent"], 1)

    # 3. coalesce_max 3, 7 messages -> batches of 3, 3, 1.
    # Falsifier: ignore coalesce_max -> 1 batch of 7.
    async def test_coalesce_max_splits_burst_into_full_batches(self):
        adapter = self.make({
            "cloud.": {"mode": "agent", "coalesce": LONG_WINDOW, "coalesce_max": 3},
        })
        for i in range(6):
            await self.send(adapter, f"cloud.{i}", f"E{i}", f"evt-{i}")
        await self.wait_for_events(adapter, 2)
        self.assertEqual(len(adapter.events), 2, "7 events at max 3 must not be one batch")
        first, second = adapter.events[0].text, adapter.events[1].text
        self.assertTrue(first.startswith("3 events on cloud. in 60s\n"), first[:120])
        self.assertTrue(second.startswith("3 events on cloud. in 60s\n"), second[:120])
        for i in (0, 1, 2):
            self.assertIn(f"E{i}", first)
        for i in (3, 4, 5):
            self.assertIn(f"E{i}", second)
        self.assertNotIn("E3", first)
        self.assertNotIn("E2", second)
        await self.send(adapter, "cloud.6", "E6", "evt-6")
        self.assertEqual(len(adapter.events), 2, "the 7th event waits inside a fresh window")
        await adapter.disconnect()
        self.assertEqual(len(adapter.events), 3, "the 1-event tail flushes on disconnect")
        tail = adapter.events[2].text
        self.assertTrue(tail.startswith("1 events on cloud. in 60s\n"), tail[:120])
        self.assertIn("E6", tail)
        self.assertEqual(adapter._counts["coalesced_messages"], 7)
        self.assertEqual(adapter._counts["agent"], 3)

    # 4. Two prefixes with different windows -> separate actions, no mixing.
    # Falsifier: key the bucket by leaf topic or by mode.
    async def test_prefixes_flush_separately_without_mixing(self):
        adapter = self.make({
            "fast.": {"mode": "agent", "coalesce": WINDOW},
            "slow.": {"mode": "agent", "coalesce": LONG_WINDOW},
        })
        await self.send(adapter, "fast.a", "F0", "evt-f0")
        await self.send(adapter, "fast.b", "F1", "evt-f1")
        await self.send(adapter, "slow.a", "S0", "evt-s0")
        await self.send(adapter, "slow.b", "S1", "evt-s1")
        await self.wait_for_events(adapter, 1)
        self.assertEqual(len(adapter.events), 1, "only the 0.05 s prefix may have flushed")
        fast_text = adapter.events[0].text
        self.assertTrue(fast_text.startswith("2 events on fast. in 0.05s\n"), fast_text[:120])
        self.assertIn("F0", fast_text)
        self.assertIn("F1", fast_text)
        self.assertNotIn("S0", fast_text, "a different prefix must never mix into the bucket")
        self.assertNotIn("S1", fast_text)
        self.assertEqual(adapter._counts["coalesced_messages"], 4)
        await adapter.disconnect()
        self.assertEqual(len(adapter.events), 2)
        slow_text = adapter.events[1].text
        self.assertTrue(slow_text.startswith("2 events on slow. in 60s\n"), slow_text[:120])
        self.assertIn("S0", slow_text)
        self.assertIn("S1", slow_text)
        self.assertNotIn("F0", slow_text)
        self.assertEqual(adapter._counts["agent"], 2)

    # 5. Duplicate event_id + distinct events -> 1 batch of 3.
    # Four publishes: A, A (same event_id), B, C. The duplicate is filtered before
    # buffering, so exactly 3 distinct events form the batch; if dedupe ran after
    # buffering the batch would be 4 events and the header says so.
    # Falsifier: move _dedup.seen() after buffering.
    async def test_duplicate_event_does_not_enter_the_batch(self):
        adapter = self.make({"cloud.": {"mode": "agent", "coalesce": WINDOW}})
        await self.send(adapter, "cloud.a", "EVENT-A", "evt-a")
        await self.send(adapter, "cloud.a", "EVENT-A", "evt-a")  # same event_id
        await self.send(adapter, "cloud.b", "EVENT-B", "evt-b")
        await self.send(adapter, "cloud.c", "EVENT-C", "evt-c")
        await self.wait_for_events(adapter, 1)
        self.assertEqual(len(adapter.events), 1)
        text = adapter.events[0].text
        self.assertTrue(
            text.startswith("3 events on cloud. in 0.05s\n"),
            f"duplicate inflated/deflated the batch: {text[:120]!r}",
        )
        for marker in ("EVENT-A", "EVENT-B", "EVENT-C"):
            self.assertIn(marker, text)
        self.assertEqual(adapter._counts["coalesced_messages"], 3,
                         "a duplicate must not count as a buffered message")

    # 6. disconnect() with a pending batch -> flushed before the adapter reports
    # disconnected. Falsifier: drop buckets on disconnect.
    async def test_disconnect_flushes_pending_batch_before_reporting_disconnected(self):
        adapter = self.make({"cloud.": {"mode": "agent", "coalesce": LONG_WINDOW}})
        for i in range(3):
            await self.send(adapter, f"cloud.{i}", f"E{i}", f"evt-{i}")
        self.assertEqual(adapter.events, [], "the 60 s window cannot have flushed yet")
        with self.assertLogs("mqtt.adapter", level="INFO") as captured:
            await adapter.disconnect()
        self.assertEqual(len(adapter.events), 1, "disconnect must flush the pending burst")
        text = adapter.events[0].text
        self.assertTrue(text.startswith("3 events on cloud. in 60s\n"), text[:120])
        for i in range(3):
            self.assertIn(f"E{i}", text)
        messages = [r.getMessage() for r in captured.records]
        flush_at = next(
            (i for i, m in enumerate(messages) if "coalesced 3 messages on cloud." in m), None)
        down_at = next(
            (i for i, m in enumerate(messages) if "disconnected (modes seen" in m), None)
        self.assertIsNotNone(flush_at, messages)
        self.assertIsNotNone(down_at, messages)
        self.assertLess(flush_at, down_at, "the flush must precede the 'disconnected' report")
        self.assertIn("coalesced_messages", messages[down_at])

    # 7. After disconnect(), _background_tasks is empty (no leaked timer/flush tasks).
    # Falsifier: leave a flushed window task in _background_tasks after disconnect.
    # The burst. prefix flushes from its window timer first (exercising the tracked
    # coalescing task), then hold. is pending when disconnect() runs.
    async def test_disconnect_leaves_no_background_tasks(self):
        adapter = self.make({
            "burst.": {"mode": "agent", "coalesce": WINDOW},
            "hold.": {"mode": "agent", "coalesce": LONG_WINDOW},
        })
        await self.send(adapter, "burst.a", "BURST", "evt-burst")
        await self.wait_for_events(adapter, 1)  # window timer created + completed a task
        await self.send(adapter, "hold.a", "HOLD", "evt-hold")
        await adapter.disconnect()
        self.assertEqual(len(adapter.events), 2, "the pending bucket flushes on disconnect")
        await self.wait_for(
            lambda: not adapter._background_tasks,
            message=f"coalescing task outlived disconnect(): {adapter._background_tasks!r}",
        )
        self.assertEqual(adapter._background_tasks, set())

    # 8. job mode: execute_job_for_event receives the batch string as extra_prompt.
    # Falsifier: pass only the first event.
    async def test_job_mode_receives_batch_as_extra_prompt(self):
        patcher, calls = cron_seam()
        adapter = self.make({
            "cloud.": {"mode": "job", "job_id": "c0ffee", "coalesce": WINDOW},
        })
        with patcher:  # keep the fake seam installed while the window timer fires
            for i in range(3):
                await self.send(adapter, f"cloud.{i}", f"JOB{i}", f"job-{i}")
            await self.wait_for(
                lambda: len(calls) >= 1 and adapter._counts["job"] >= 1,
                message="job batch did not fire",
            )
        self.assertEqual(len(calls), 1, "a burst must fire the job once")
        job_id, context = calls[0]
        self.assertEqual(job_id, "c0ffee")
        self.assertIsInstance(context, str)
        self.assertTrue(
            context.startswith("3 events on cloud. in 0.05s\n"),
            f"job context is not the batch string: {context[:120]!r}",
        )
        for i in range(3):
            self.assertIn(f"JOB{i}", context)
        self.assertEqual(adapter._counts["coalesced_messages"], 3)
        self.assertEqual(adapter._counts["job"], 1)

    # 9. Batch bigger than max_render_chars -> truncated with the note, still one action.
    # Falsifier: truncate silently or raise.
    async def test_oversized_batch_is_truncated_with_a_note_and_still_dispatched(self):
        adapter = self.make(
            {"big.": {"mode": "agent", "coalesce": WINDOW}}, max_render_chars=400)
        for i in range(8):
            await self.send(adapter, f"big.{i}", f"M{i}", f"evt-{i}")
        await self.wait_for_events(adapter, 1)
        self.assertEqual(len(adapter.events), 1, "a truncated batch still dispatches one action")
        text = adapter.events[0].text
        shown = [i for i in range(8) if f"M{i}" in text]
        self.assertLess(len(shown), 8, "the batch was not truncated")
        self.assertIn("truncated:", text)
        self.assertIn("more events not shown", text)
        self.assertIn(
            f"truncated: {8 - len(shown)} more events not shown",
            text,
            "the note must name how many events were omitted",
        )
        self.assertLessEqual(
            len(text), 400 + 100, "rendered batch ignored max_render_chars")
        self.assertEqual(adapter._counts["agent"], 1)

    # 10. coalesce absent -> current per-message behaviour.
    # Falsifier: default-on coalescing.
    async def test_coalesce_absent_keeps_per_message_behaviour(self):
        adapter = self.make({"plain.": {"mode": "agent"}})
        self.assertIn("coalesced_messages", adapter._counts,
                      "the counter must exist from construction")
        for i in range(2):
            await self.send(adapter, f"plain.{i}", f"E{i}", f"evt-{i}")
        self.assertEqual(len(adapter.events), 2, "without coalesce every message is its own action")
        for i, event in enumerate(adapter.events):
            self.assertEqual(event.message_id, f"evt-{i}")
            self.assertTrue(event.text.startswith(f"MQTT event on events/plain.{i} from hass"),
                            event.text[:120])
            self.assertNotIn("events on", event.text.split("\n")[0])
        self.assertEqual(adapter._counts["agent"], 2)
        self.assertEqual(adapter._counts["coalesced_messages"], 0)

    # Registry for the falsification task: method name -> spec row it proves.
    MANDATORY = {
        1: "test_burst_under_one_prefix_becomes_one_agent_turn",
        2: "test_single_message_flushes_when_the_window_elapses",
        3: "test_coalesce_max_splits_burst_into_full_batches",
        4: "test_prefixes_flush_separately_without_mixing",
        5: "test_duplicate_event_does_not_enter_the_batch",
        6: "test_disconnect_flushes_pending_batch_before_reporting_disconnected",
        7: "test_disconnect_leaves_no_background_tasks",
        8: "test_job_mode_receives_batch_as_extra_prompt",
        9: "test_oversized_batch_is_truncated_with_a_note_and_still_dispatched",
        10: "test_coalesce_absent_keeps_per_message_behaviour",
    }


# --- extras: the rest of the config/buffering contract ------------------------------


class CoalesceConfigTests(CoalesceTestCase):
    async def test_explicit_zero_coalesce_is_off(self):
        adapter = self.make({"plain.": {"mode": "agent", "coalesce": 0}})
        for i in range(2):
            await self.send(adapter, f"plain.{i}", f"E{i}", f"evt-{i}")
        self.assertEqual(len(adapter.events), 2)
        self.assertNotIn("events on", adapter.events[0].text)
        self.assertEqual(adapter._counts["coalesced_messages"], 0)

    async def test_non_numeric_coalesce_is_clamped_with_one_warning_and_no_crash(self):
        with self.assertLogs("mqtt.adapter", level="WARNING") as captured:
            adapter = self.make({"bad.": {"mode": "agent", "coalesce": "not-a-number"}})
        warnings = [r.getMessage() for r in captured.records
                    if "coalesce" in r.getMessage().lower()]
        self.assertEqual(len(warnings), 1, warnings)
        self.assertIsInstance(adapter, MqttAdapter)
        self.assertEqual(adapter._counts["coalesced_messages"], 0)

    async def test_out_of_range_window_is_clamped_to_3600s(self):
        with self.assertLogs("mqtt.adapter", level="WARNING") as captured:
            adapter = self.make({"big.": {
                "mode": "agent", "coalesce": 99_999, "coalesce_max": 2}})
        warnings = [r.getMessage() for r in captured.records
                    if "coalesce" in r.getMessage().lower()]
        self.assertEqual(len(warnings), 1, warnings)
        for i in range(2):
            await self.send(adapter, f"big.{i}", f"E{i}", f"evt-{i}")
        await self.wait_for_events(adapter, 1)
        self.assertTrue(
            adapter.events[0].text.startswith("2 events on big. in 3600s\n"),
            f"window was not clamped: {adapter.events[0].text[:120]!r}",
        )

    async def test_below_minimum_coalesce_max_is_clamped_to_two(self):
        with self.assertLogs("mqtt.adapter", level="WARNING") as captured:
            adapter = self.make({"big.": {
                "mode": "agent", "coalesce": LONG_WINDOW, "coalesce_max": -5}})
        warnings = [r.getMessage() for r in captured.records
                    if "coalesce" in r.getMessage().lower()]
        self.assertEqual(len(warnings), 1, warnings)
        for i in range(2):
            await self.send(adapter, f"big.{i}", f"E{i}", f"evt-{i}")
        await self.wait_for_events(adapter, 1)
        self.assertTrue(
            adapter.events[0].text.startswith("2 events on big. in 60s\n"),
            f"coalesce_max was not clamped to 2: {adapter.events[0].text[:120]!r}",
        )

    async def test_default_policy_batches_under_the_default_label(self):
        adapter = self.make({}, default={"mode": "agent", "coalesce": WINDOW})
        for i in range(2):
            await self.send(adapter, f"unmatched.{i}", f"E{i}", f"evt-{i}")
        await self.wait_for_events(adapter, 1)
        self.assertEqual(len(adapter.events), 1)
        self.assertTrue(
            adapter.events[0].text.startswith("2 events on default in 0.05s\n"),
            f"default bucket label wrong: {adapter.events[0].text[:120]!r}",
        )


class CoalesceScopeTests(CoalesceTestCase):
    """Only agent/job coalesce; deliver/drop keep their per-message behaviour."""

    async def test_deliver_never_coalesces(self):
        adapter = self.make({"notify.": {
            "mode": "deliver", "deliver": "telegram", "chat_id": "ops",
            "coalesce": WINDOW}})
        for i in range(2):
            await self.send(adapter, f"notify.{i}", f"E{i}", f"evt-{i}")
        self.assertEqual(len(adapter.deliveries), 2, "deliver must stay per-message")
        for i, (text, _target) in enumerate(adapter.deliveries):
            self.assertEqual(text, f"notify.{i}: E{i}")
        self.assertEqual(adapter._counts["deliver"], 2)
        self.assertEqual(adapter._counts["coalesced_messages"], 0)

    async def test_drop_never_coalesces(self):
        adapter = self.make({"noise.": {"mode": "drop", "coalesce": WINDOW}})
        for i in range(2):
            await self.send(adapter, f"noise.{i}", f"E{i}", f"evt-{i}")
        self.assertEqual(adapter._counts["drop"], 2)
        self.assertEqual(adapter._counts["coalesced_messages"], 0)
        self.assertEqual(adapter.events, [])


class CoalesceBufferTests(CoalesceTestCase):
    async def test_accepted_retained_message_joins_the_bucket(self):
        adapter = self.make(
            {"ret.": {"mode": "agent", "coalesce": WINDOW}}, accept_retained=True)
        await self.send(adapter, "ret.a", "LIVE", "evt-live")
        await self.send(adapter, "ret.b", "RETAINED", "evt-ret", retained=True)
        await self.wait_for_events(adapter, 1)
        self.assertEqual(len(adapter.events), 1)
        text = adapter.events[0].text
        self.assertTrue(text.startswith("2 events on ret. in 0.05s\n"), text[:120])
        self.assertIn("LIVE", text)
        self.assertIn("RETAINED", text)
        self.assertEqual(adapter._counts["coalesced_messages"], 2)

    async def test_bucket_limit_flushes_the_oldest_bucket(self):
        count = 257  # MAX_COALESCE_BUCKETS is 256: one bucket creation overflows
        topics = {
            f"p{i}.": {"mode": "agent", "coalesce": LONG_WINDOW} for i in range(count)
        }
        adapter = self.make(topics)
        with self.assertLogs("mqtt.adapter", level="WARNING") as captured:
            for i in range(count):
                await self.send(adapter, f"p{i}.x", f"BOUND{i}", f"evt-{i}")
            await self.wait_for_events(adapter, 1)
        self.assertEqual(len(adapter.events), 1, "overflow must flush, never drop")
        text = adapter.events[0].text
        self.assertTrue(text.startswith("1 events on p0. in 60s\n"), text[:120])
        self.assertIn("BOUND0", text)
        self.assertEqual(adapter._counts["coalesced_messages"], count)
        self.assertEqual(len(adapter._coalesce_buckets), 256,
                         "the live bucket map must stay bounded at 256")
        warnings = [r.getMessage() for r in captured.records
                    if "bucket" in r.getMessage().lower()
                    or "coalesce" in r.getMessage().lower()]
        self.assertEqual(len(warnings), 1, warnings)


if __name__ == "__main__":
    unittest.main()
