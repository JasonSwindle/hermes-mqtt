"""Wire codecs for MQTT 3.1.1 (protocol level 4) and MQTT 5.0 (level 5).

Pure functions only: bytes in, dataclasses out. No sockets, no asyncio, so the
whole wire format is testable at byte level.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass, field

PROTOCOL_311 = 4
PROTOCOL_5 = 5

P_PAYLOAD_FORMAT = 0x01
P_MESSAGE_EXPIRY = 0x02
P_CONTENT_TYPE = 0x03
P_RESPONSE_TOPIC = 0x08
P_CORRELATION_DATA = 0x09
P_SUBSCRIPTION_ID = 0x0B
P_SESSION_EXPIRY = 0x11
P_ASSIGNED_CLIENT_ID = 0x12
P_SERVER_KEEP_ALIVE = 0x13
P_AUTH_METHOD = 0x15
P_AUTH_DATA = 0x16
P_REQUEST_PROBLEM_INFO = 0x17
P_WILL_DELAY = 0x18
P_REQUEST_RESPONSE_INFO = 0x19
P_RESPONSE_INFO = 0x1A
P_SERVER_REFERENCE = 0x1C
P_REASON_STRING = 0x1F
P_RECEIVE_MAXIMUM = 0x21
P_TOPIC_ALIAS_MAXIMUM = 0x22
P_TOPIC_ALIAS = 0x23
P_MAXIMUM_QOS = 0x24
P_RETAIN_AVAILABLE = 0x25
P_USER_PROPERTY = 0x26
P_MAXIMUM_PACKET_SIZE = 0x27
P_WILDCARD_SUBSCRIPTION = 0x28
P_SUBSCRIPTION_ID_AVAILABLE = 0x29
P_SHARED_SUBSCRIPTION = 0x2A

PROPERTY_TYPES: dict[int, str] = {
    P_PAYLOAD_FORMAT: "u8", P_MESSAGE_EXPIRY: "u32", P_CONTENT_TYPE: "utf8",
    P_RESPONSE_TOPIC: "utf8", P_CORRELATION_DATA: "binary", P_SUBSCRIPTION_ID: "varint",
    P_SESSION_EXPIRY: "u32", P_ASSIGNED_CLIENT_ID: "utf8", P_SERVER_KEEP_ALIVE: "u16",
    P_AUTH_METHOD: "utf8", P_AUTH_DATA: "binary", P_REQUEST_PROBLEM_INFO: "u8",
    P_WILL_DELAY: "u32", P_REQUEST_RESPONSE_INFO: "u8", P_RESPONSE_INFO: "utf8",
    P_SERVER_REFERENCE: "utf8", P_REASON_STRING: "utf8", P_RECEIVE_MAXIMUM: "u16",
    P_TOPIC_ALIAS_MAXIMUM: "u16", P_TOPIC_ALIAS: "u16", P_MAXIMUM_QOS: "u8",
    P_RETAIN_AVAILABLE: "u8", P_USER_PROPERTY: "pair", P_MAXIMUM_PACKET_SIZE: "u32",
    P_WILDCARD_SUBSCRIPTION: "u8", P_SUBSCRIPTION_ID_AVAILABLE: "u8",
    P_SHARED_SUBSCRIPTION: "u8",
}


class CodecError(ValueError):
    """A packet that cannot be encoded or parsed."""


@dataclass
class Connack:
    code: int
    session_present: bool = False
    properties: dict = field(default_factory=dict)


@dataclass
class Ack:
    packet_id: int
    reason: int = 0
    properties: dict = field(default_factory=dict)


@dataclass
class Publish:
    topic: str
    payload: bytes
    qos: int
    retain: bool
    packet_id: int = 0
    properties: dict = field(default_factory=dict)


def normalize_protocol(value) -> str:
    """Map config input to 'auto' | '5' | '3.1.1'."""
    text = str(value if value is not None else "auto").strip().lower()
    if text in ("5", "5.0", "v5"):
        return "5"
    if text in ("4", "3.1.1", "v3.1.1", "311"):
        return "3.1.1"
    return "auto"


def levels_for(choice: str) -> list[int]:
    if choice == "5":
        return [PROTOCOL_5]
    if choice == "3.1.1":
        return [PROTOCOL_311]
    return [PROTOCOL_5, PROTOCOL_311]


def encode_varint(value: int) -> bytes:
    if not 0 <= value <= 268_435_455:
        raise CodecError(f"variable byte integer out of range: {value}")
    out = bytearray()
    while True:
        value, byte = divmod(value, 128)
        out.append(byte | 0x80 if value else byte)
        if not value:
            return bytes(out)


def decode_varint(buf: bytes, i: int) -> tuple[int, int]:
    start = i
    value, multiplier = 0, 1
    for _ in range(4):
        if i >= len(buf):
            raise CodecError("truncated variable byte integer")
        byte = buf[i]
        i += 1
        value += (byte & 0x7F) * multiplier
        if not byte & 0x80:
            if i - start > 1 and byte == 0:
                # [MQTT-1.5.5-1] the minimum number of bytes MUST be used.
                raise CodecError("non-minimal variable byte integer")
            return value, i
        multiplier *= 128
    raise CodecError("variable byte integer longer than 4 bytes")


def encode_string(value: str) -> bytes:
    raw = value.encode("utf-8")
    if len(raw) > 0xFFFF:
        raise CodecError("string longer than 65535 bytes")
    return struct.pack("!H", len(raw)) + raw


def encode_binary(value: bytes) -> bytes:
    if len(value) > 0xFFFF:
        raise CodecError("binary value longer than 65535 bytes")
    return struct.pack("!H", len(value)) + value


def encode_properties(items: list[tuple[int, bytes]]) -> bytes:
    body = b"".join(bytes((pid,)) + raw for pid, raw in items)
    return encode_varint(len(body)) + body


def _read_typed(buf: bytes, i: int, kind: str):
    if kind == "u8":
        if i + 1 > len(buf):
            raise CodecError("truncated u8 property")
        return buf[i], i + 1
    if kind == "u16":
        if i + 2 > len(buf):
            raise CodecError("truncated u16 property")
        return struct.unpack("!H", buf[i:i + 2])[0], i + 2
    if kind == "u32":
        if i + 4 > len(buf):
            raise CodecError("truncated u32 property")
        return struct.unpack("!I", buf[i:i + 4])[0], i + 4
    if kind == "varint":
        return decode_varint(buf, i)
    if kind in ("utf8", "binary"):
        if i + 2 > len(buf):
            raise CodecError("truncated length prefix")
        size = struct.unpack("!H", buf[i:i + 2])[0]
        i += 2
        if i + size > len(buf):
            raise CodecError("truncated property value")
        raw = buf[i:i + size]
        i += size
        if kind == "binary":
            return raw, i
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise CodecError("invalid UTF-8 in property string") from exc
        if "\x00" in text:
            raise CodecError("U+0000 is not allowed in an MQTT string")
        return text, i
    if kind == "pair":
        key, i = _read_typed(buf, i, "utf8")
        value, i = _read_typed(buf, i, "utf8")
        return (key, value), i
    raise CodecError(f"unknown property type {kind}")


def parse_properties(buf: bytes, i: int) -> tuple[dict, int]:
    length, i = decode_varint(buf, i)
    end = i + length
    if end > len(buf):
        raise CodecError("property block overruns packet")
    props: dict = {}
    while i < end:
        pid, i = decode_varint(buf, i)
        kind = PROPERTY_TYPES.get(pid)
        if kind is None:
            raise CodecError(f"unknown property identifier {pid}")
        value, i = _read_typed(buf, i, kind)
        if pid == P_USER_PROPERTY:
            props.setdefault("user_properties", []).append(value)
            continue
        if pid in props:
            # Every property except User Property is a Protocol Error to repeat.
            raise CodecError(f"duplicate property identifier {pid:#04x}")
        props[pid] = value
    if i != end:
        raise CodecError("property block length mismatch")
    return props, i


def encode_connect(*, client_id: str, username: str, password: bytes,
                   keepalive: int, protocol: int) -> bytes:
    if protocol == PROTOCOL_311 and password and not username:
        raise CodecError("MQTT 3.1.1 requires a username when a password is set")
    flags = 0x02  # Clean Session (3.1.1) / Clean Start (5)
    payload = encode_string(client_id)
    if username:
        flags |= 0x80
        payload += encode_string(username)
    if password:
        flags |= 0x40
        payload += encode_binary(password)
    body = encode_string("MQTT") + bytes((protocol, flags)) + struct.pack("!H", keepalive)
    if protocol == PROTOCOL_5:
        body += b"\x00"  # no CONNECT properties
    return body + payload


def decode_connack(body: bytes, protocol: int) -> Connack:
    if len(body) < 2:
        raise CodecError("truncated CONNACK")
    flags, code = body[0], body[1]
    if flags & 0xFE:
        # [MQTT-3.2.2-1] bits 7-1 of the CONNACK flags are reserved and MUST be zero.
        raise CodecError("CONNACK has reserved flag bits set")
    properties: dict = {}
    if protocol == PROTOCOL_5:
        properties, end = parse_properties(body, 2)
        if end != len(body):
            raise CodecError("CONNACK has trailing bytes after its properties")
    elif len(body) != 2:
        # A 3.1.1 CONNACK is exactly flags+return code, nothing else.
        raise CodecError("3.1.1 CONNACK has trailing bytes")
    return Connack(code=code, session_present=bool(flags & 1), properties=properties)


def encode_subscribe(*, packet_id: int, topic_filter: str, qos: int, protocol: int) -> bytes:
    if qos not in (0, 1, 2):
        raise CodecError(f"subscription QoS out of range: {qos}")
    body = struct.pack("!H", packet_id)
    if protocol == PROTOCOL_5:
        body += b"\x00"       # no SUBSCRIBE properties
        options = qos | 0x20  # Retain Handling 2: never send retained messages
    else:
        options = qos
    return body + encode_string(topic_filter) + bytes((options,))


def decode_suback(body: bytes, protocol: int) -> Ack:
    if len(body) < 2:
        raise CodecError("truncated SUBACK")
    packet_id = struct.unpack("!H", body[:2])[0]
    i = 2
    properties: dict = {}
    if protocol == PROTOCOL_5:
        properties, i = parse_properties(body, i)
    if i >= len(body):
        raise CodecError("SUBACK has no reason code")
    if i + 1 != len(body):
        # We send exactly one topic filter per SUBSCRIBE, so a conforming broker
        # answers with exactly one reason code.
        raise CodecError("SUBACK reason-code count does not match the subscription")
    return Ack(packet_id, body[i], properties)


def encode_publish(*, topic: str, payload: bytes, qos: int, retain: bool,
                   packet_id: int, protocol: int) -> tuple[int, bytes]:
    if qos not in (0, 1):
        raise CodecError(f"unsupported publish QoS: {qos}")
    first = 0x30 | (qos << 1) | (1 if retain else 0)
    body = encode_string(topic)
    if qos:
        body += struct.pack("!H", packet_id)
    if protocol == PROTOCOL_5:
        body += b"\x00"
    return first, body + payload


def decode_publish(first: int, body: bytes, protocol: int) -> Publish:
    if len(body) < 2:
        raise CodecError("truncated PUBLISH")
    topic_len = struct.unpack("!H", body[:2])[0]
    cursor = 2 + topic_len
    if cursor > len(body):
        raise CodecError("truncated PUBLISH topic")
    try:
        topic = body[2:cursor].decode("utf-8")
    except UnicodeDecodeError as exc:
        raise CodecError("PUBLISH topic is not valid UTF-8") from exc
    if "\x00" in topic:
        raise CodecError("PUBLISH topic contains U+0000")
    qos = (first >> 1) & 0x03
    if qos == 3:
        # [MQTT-3.3.1-4] the QoS bits are 00, 01 or 10; 11 is a malformed packet.
        raise CodecError("PUBLISH with QoS 3 (invalid fixed-header flags)")
    if qos == 0 and first & 0x08:
        # [MQTT-3.3.1-2] the DUP flag MUST be 0 for a QoS 0 message.
        raise CodecError("PUBLISH with DUP set at QoS 0")
    packet_id = 0
    if qos:
        if cursor + 2 > len(body):
            raise CodecError("truncated PUBLISH packet id")
        packet_id = struct.unpack("!H", body[cursor:cursor + 2])[0]
        if packet_id == 0:
            # [MQTT-2.3.1-1] a packet identifier must be non-zero for QoS 1/2.
            raise CodecError("PUBLISH with packet identifier 0")
        cursor += 2
    properties: dict = {}
    if protocol == PROTOCOL_5:
        properties, cursor = parse_properties(body, cursor)
    if not topic:
        raise CodecError("PUBLISH with an empty topic")
    if "+" in topic or "#" in topic:
        # [MQTT-3.3.2-2] wildcards are filter-only; they can never appear in a topic name.
        raise CodecError(f"PUBLISH topic {topic!r} contains a wildcard")
    if protocol == PROTOCOL_5 and P_TOPIC_ALIAS in properties:
        # We never advertise a Topic Alias Maximum, so an alias is a protocol violation.
        raise CodecError("PUBLISH with an unsupported topic alias")
    return Publish(topic=topic, payload=body[cursor:], qos=qos,
                   retain=bool(first & 1), packet_id=packet_id, properties=properties)


#: MQTT 5 PUBACK reason codes (Table 3-4). Anything else is a malformed packet.
PUBACK_REASONS = frozenset({
    0x00, 0x10, 0x80, 0x83, 0x87, 0x90, 0x91, 0x97, 0x99, 0x9A, 0x9B,
})


def decode_ack(body: bytes, protocol: int) -> Ack:
    if len(body) < 2:
        raise CodecError("truncated acknowledgement")
    packet_id = struct.unpack("!H", body[:2])[0]
    if packet_id == 0:
        # [MQTT-2.3.1-1] a packet identifier must be non-zero for QoS 1/2.
        raise CodecError("acknowledgement with packet identifier 0")
    reason, i, properties = 0, 2, {}
    if protocol == PROTOCOL_5 and len(body) > 2:
        reason = body[2]
        if reason not in PUBACK_REASONS:
            raise CodecError(f"PUBACK has an unknown reason code 0x{reason:02x}")
        i = 3
        if i < len(body):
            properties, i = parse_properties(body, i)
            if i != len(body):
                raise CodecError("PUBACK has trailing bytes after its properties")
    elif protocol != PROTOCOL_5 and len(body) != 2:
        # A 3.1.1 PUBACK is exactly the two-byte packet identifier.
        raise CodecError("3.1.1 PUBACK has trailing bytes")
    return Ack(packet_id, reason, properties)
