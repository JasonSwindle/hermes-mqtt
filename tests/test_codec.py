"""Byte-level tests for the MQTT wire codecs. No sockets, no asyncio."""

import os
import struct
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _bootstrap  # noqa: E402,F401  (registers the package however the repo is checked out)

from mqtt import mqtt_codec as c  # noqa: E402


class PrimitiveTests(unittest.TestCase):
    def test_varint_roundtrip(self):
        for value in (0, 127, 128, 16383, 16384, 2_097_151, 268_435_455):
            encoded = c.encode_varint(value)
            self.assertEqual(c.decode_varint(encoded, 0), (value, len(encoded)))

    def test_varint_rejects_five_bytes(self):
        with self.assertRaises(c.CodecError):
            c.decode_varint(b"\xff\xff\xff\xff\x7f", 0)

    def test_properties_roundtrip(self):
        raw = c.encode_properties([(c.P_RECEIVE_MAXIMUM, struct.pack("!H", 10))])
        self.assertEqual(c.parse_properties(raw, 0), ({c.P_RECEIVE_MAXIMUM: 10}, len(raw)))

    def test_unknown_property_is_an_error(self):
        with self.assertRaises(c.CodecError):
            c.parse_properties(b"\x02\x7f\x00", 0)


class ConnectTests(unittest.TestCase):
    def test_v311_golden_bytes(self):
        body = c.encode_connect(client_id="cid", username="u", password=b"p",
                                keepalive=60, protocol=4)
        self.assertEqual(
            body,
            c.encode_string("MQTT") + bytes((4, 0xC2)) + struct.pack("!H", 60)
            + c.encode_string("cid") + c.encode_string("u") + c.encode_binary(b"p"),
        )

    def test_v5_has_property_block(self):
        body = c.encode_connect(client_id="cid", username="", password=b"",
                                keepalive=60, protocol=5)
        self.assertEqual(body[6], 5)
        self.assertEqual(body[7], 0x02)
        self.assertEqual(body[10], 0x00)          # property length
        self.assertEqual(body[11:], c.encode_string("cid"))

    def test_v311_refuses_password_without_username(self):
        with self.assertRaises(c.CodecError):
            c.encode_connect(client_id="c", username="", password=b"p",
                             keepalive=60, protocol=4)

    def test_v5_connack_properties(self):
        body = bytes((0, 0, 3, c.P_RECEIVE_MAXIMUM, 0, 10))
        ack = c.decode_connack(body, 5)
        self.assertEqual((ack.code, ack.session_present, ack.properties[c.P_RECEIVE_MAXIMUM]),
                         (0, False, 10))


class SubscribeTests(unittest.TestCase):
    def test_v5_sets_retain_handling_2(self):
        body = c.encode_subscribe(packet_id=1, topic_filter="events/#", qos=1, protocol=5)
        self.assertEqual(body, b"\x00\x01\x00" + c.encode_string("events/#") + b"\x21")

    def test_v5_suback_reason(self):
        ack = c.decode_suback(b"\x00\x01\x00\x87", 5)
        self.assertEqual((ack.packet_id, ack.reason), (1, 0x87))

    def test_v311_options_are_qos_only(self):
        body = c.encode_subscribe(packet_id=1, topic_filter="events/#", qos=1, protocol=4)
        self.assertEqual(body, b"\x00\x01" + c.encode_string("events/#") + b"\x01")


class PublishTests(unittest.TestCase):
    def test_v5_publish_has_property_block(self):
        first, body = c.encode_publish(topic="a/b", payload=b"hi", qos=1, retain=False,
                                       packet_id=7, protocol=5)
        self.assertEqual(first, 0x32)
        self.assertEqual(body, c.encode_string("a/b") + b"\x00\x07\x00hi")

    def test_decode_v5_publish_with_user_property(self):
        prop = bytes((c.P_USER_PROPERTY,)) + c.encode_string("k") + c.encode_string("v")
        body = c.encode_string("a/b") + b"\x00\x07" + bytes((len(prop),)) + prop + b"hi"
        msg = c.decode_publish(0x32, body, 5)
        self.assertEqual((msg.topic, msg.payload, msg.packet_id), ("a/b", b"hi", 7))
        self.assertEqual(msg.properties["user_properties"], [("k", "v")])

    def test_v5_puback_reason(self):
        self.assertEqual(c.decode_ack(b"\x00\x07\x87\x00", 5).reason, 0x87)
        self.assertEqual(c.decode_ack(b"\x00\x07", 5).reason, 0)

    def test_v311_puback_is_id_only(self):
        self.assertEqual(c.decode_ack(b"\x00\x07", 4).reason, 0)


class MalformedPublishTests(unittest.TestCase):
    """Malformed PUBLISH packets must be codec errors, never routed as if they were valid."""

    def test_qos3_flag_bits_are_rejected(self):
        with self.assertRaises(c.CodecError):
            c.decode_publish(0x36, c.encode_string("a/b") + b"hi", 4)

    def test_empty_topic_is_rejected(self):
        with self.assertRaises(c.CodecError):
            c.decode_publish(0x30, b"\x00\x00hi", 4)

    def test_wildcard_topic_is_rejected(self):
        for topic in ("a/#", "a/+/b"):
            with self.assertRaises(c.CodecError, msg=topic):
                c.decode_publish(0x30, c.encode_string(topic) + b"hi", 4)

    def test_qos1_publish_with_packet_id_zero_is_rejected(self):
        body = c.encode_string("a/b") + b"\x00\x00hi"
        with self.assertRaises(c.CodecError):
            c.decode_publish(0x32, body, 4)

    def test_topic_alias_property_is_rejected_on_v5(self):
        prop = bytes((c.P_TOPIC_ALIAS,)) + struct.pack("!H", 1)
        body = c.encode_string("a/b") + bytes((len(prop),)) + prop + b"hi"
        with self.assertRaises(c.CodecError):
            c.decode_publish(0x30, body, 5)


class PasswordOnlyConnectTests(unittest.TestCase):
    def test_v5_password_without_username_encodes(self):
        body = c.encode_connect(client_id="cid", username="", password=b"p",
                                keepalive=60, protocol=5)
        self.assertEqual(body[7], 0x42)  # clean start + password flag, username flag clear


class HostilePacketTests(unittest.TestCase):
    """Malformed-but-plausible packets must be codec errors, never silently accepted."""

    def test_invalid_utf8_topic_is_rejected(self):
        body = bytes((0, 2)) + b"\xff\xfe" + b"hi"
        with self.assertRaises(c.CodecError):
            c.decode_publish(0x30, body, 4)

    def test_nul_topic_is_rejected(self):
        with self.assertRaises(c.CodecError):
            c.decode_publish(0x30, c.encode_string("a\x00b") + b"hi", 4)

    def test_dup_at_qos0_is_rejected(self):
        with self.assertRaises(c.CodecError):
            c.decode_publish(0x38, c.encode_string("a") + b"hi", 4)

    def test_connack_reserved_flags_are_rejected(self):
        with self.assertRaises(c.CodecError):
            c.decode_connack(b"\x02\x00", 4)

    def test_connack_trailing_bytes_are_rejected(self):
        with self.assertRaises(c.CodecError):
            c.decode_connack(bytes((0, 0, 0, 0x99)), 5)

    def test_duplicate_property_is_rejected(self):
        raw = c.encode_properties([
            (c.P_RECEIVE_MAXIMUM, struct.pack("!H", 5)),
            (c.P_RECEIVE_MAXIMUM, struct.pack("!H", 6)),
        ])
        with self.assertRaises(c.CodecError):
            c.parse_properties(raw, 0)

    def test_suback_with_two_reason_codes_is_rejected(self):
        with self.assertRaises(c.CodecError):
            c.decode_suback(b"\x00\x01\x00\x01\x01", 5)

    def test_puback_unknown_reason_is_rejected(self):
        with self.assertRaises(c.CodecError):
            c.decode_ack(b"\x00\x07\x05\x00", 5)

    def test_puback_packet_id_zero_is_rejected(self):
        with self.assertRaises(c.CodecError):
            c.decode_ack(b"\x00\x00", 4)

    def test_puback_trailing_bytes_are_rejected(self):
        with self.assertRaises(c.CodecError):
            c.decode_ack(b"\x00\x07\x00\x00\x99", 5)

    def test_non_minimal_varint_is_rejected(self):
        with self.assertRaises(c.CodecError):
            c.decode_varint(b"\x80\x00", 0)

class LegacyFixedLengthTests(unittest.TestCase):
    def test_v311_connack_trailing_bytes_are_rejected(self):
        with self.assertRaises(c.CodecError):
            c.decode_connack(b"\x00\x00\x99", 4)

    def test_v311_puback_trailing_bytes_are_rejected(self):
        with self.assertRaises(c.CodecError):
            c.decode_ack(b"\x00\x07\x99", 4)
