#!/usr/bin/env python3
"""
Self-check harness for the CodeCrafters "Build your own Kafka" challenge.

Usage:
    1. Start your broker in one terminal:  ./your_program.sh
    2. Run this in another terminal:       python3 test_stages.py

It sends the same raw hex payloads described in each stage's instructions
and checks the response against that stage's stated validation rules.
It does NOT give you the solution -- it just tells you pass/fail and why,
same as the real tester would.

Add a stage's function to STAGES to run it. Comment ones out you haven't
reached yet.
adding this here to commit
"""

import socket
import struct
import uuid

HOST = "localhost"
PORT = 9092
TIMEOUT = 5


class Fail(Exception):
    pass


def connect():
    s = socket.create_connection((HOST, PORT), timeout=TIMEOUT)
    s.settimeout(TIMEOUT)
    return s


def send_recv(sock, hex_payload):
    """Send raw bytes (hex string) and read one length-prefixed response."""
    sock.sendall(bytes.fromhex(hex_payload))
    return read_message(sock)


def read_message(sock):
    size_bytes = recv_exact(sock, 4)
    size = struct.unpack(">i", size_bytes)[0]
    body = recv_exact(sock, size)
    return size_bytes + body, size, body


def recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise Fail(f"connection closed early, wanted {n} bytes, got {len(buf)}")
        buf += chunk
    return buf


# ---------- compact-type helpers for reading responses ----------

def read_unsigned_varint(buf, off):
    value = 0
    shift = 0
    while True:
        b = buf[off]
        off += 1
        value |= (b & 0x7F) << shift
        if not (b & 0x80):
            break
        shift += 7
    return value, off


def read_compact_string(buf, off):
    length, off = read_unsigned_varint(buf, off)
    length -= 1  # compact encoding: length+1
    if length < 0:
        return None, off
    s = buf[off:off + length].decode("utf-8")
    return s, off + length


# =========================================================
# nc5: Parse ApiVersion -- unsupported version -> error 35
# =========================================================
def test_nc5():
    sock = connect()
    try:
        # request_api_key=18, request_api_version=26442 (unsupported)
        payload = ("000000230012674a4f74d28b00096b61666b612d636c69"
                   "000a6b61666b612d636c6904302e3100")
        raw, size, body = send_recv(sock, payload)

        if len(size.to_bytes(4, "big", signed=True)) != 4:
            raise Fail("message_size field must be 4 bytes")

        correlation_id = struct.unpack(">i", body[0:4])[0]
        expected_corr = 0x4f74d28b
        if correlation_id != expected_corr:
            raise Fail(f"correlation_id mismatch: got {correlation_id:#x}, "
                        f"want {expected_corr:#x}")

        error_code = struct.unpack(">h", body[4:6])[0]
        if error_code != 35:
            raise Fail(f"error_code should be 35 (UNSUPPORTED_VERSION), got {error_code}")

        print("nc5  PASS  (correlation_id echoed, error_code=35)")
    finally:
        sock.close()


# =========================================================
# correlation id stage: echo correlation_id from header v2
# =========================================================
def test_correlation_id():
    sock = connect()
    try:
        payload = ("00000023001200046f7fc66100096b61666b612d636c69"
                   "000a6b61666b612d636c6904302e3100")
        raw, size, body = send_recv(sock, payload)
        correlation_id = struct.unpack(">i", body[0:4])[0]
        expected = 0x6f7fc661
        if correlation_id != expected:
            raise Fail(f"correlation_id mismatch: got {correlation_id:#x}, want {expected:#x}")
        print("correlation_id  PASS")
    finally:
        sock.close()


# =========================================================
# pv1 / yk1: ApiVersions v4 body -- error_code 0, api_keys
# includes 18 (0-4) and (after yk1) 75 (0-0 or higher)
# =========================================================
def test_apiversions(require_describe_topic_partitions=False):
    sock = connect()
    try:
        payload = ("0000001a0012000467890abc00096b61666b612d636c69"
                   "000a6b61666b612d636c6904302e3100")
        raw, size, body = send_recv(sock, payload)

        expected_size = len(raw) - 4
        if size != expected_size:
            raise Fail(f"message_size {size} != actual remaining bytes {expected_size}")

        off = 0
        correlation_id = struct.unpack(">i", body[off:off + 4])[0]
        off += 4
        expected_corr = 0x67890abc
        if correlation_id != expected_corr:
            raise Fail(f"correlation_id mismatch: got {correlation_id:#x}, want {expected_corr:#x}")

        error_code = struct.unpack(">h", body[off:off + 2])[0]
        off += 2
        if error_code != 0:
            raise Fail(f"error_code should be 0, got {error_code}")

        arr_len, off = read_unsigned_varint(body, off)
        arr_len -= 1  # compact array encoding

        found = {}
        for _ in range(arr_len):
            api_key = struct.unpack(">h", body[off:off + 2])[0]
            off += 2
            min_v = struct.unpack(">h", body[off:off + 2])[0]
            off += 2
            max_v = struct.unpack(">h", body[off:off + 2])[0]
            off += 2
            _tag, off = read_unsigned_varint(body, off)
            found[api_key] = (min_v, max_v)

        if 18 not in found:
            raise Fail("no entry for api_key 18 (ApiVersions)")
        min_v, max_v = found[18]
        if min_v != 0 or max_v < 4:
            raise Fail(f"ApiVersions entry wrong: min={min_v}, max={max_v} (want min=0, max>=4)")

        if require_describe_topic_partitions:
            if 75 not in found:
                raise Fail("no entry for api_key 75 (DescribeTopicPartitions)")
            min_v75, max_v75 = found[75]
            if min_v75 != 0 or max_v75 < 0:
                raise Fail(f"DescribeTopicPartitions entry wrong: min={min_v75}, max={max_v75}")

        throttle = struct.unpack(">i", body[off:off + 4])[0]
        off += 4
        _tag, off = read_unsigned_varint(body, off)

        if off != len(body):
            raise Fail(f"{len(body) - off} extra bytes left after decoding response body")

        label = "yk1 (with DescribeTopicPartitions)" if require_describe_topic_partitions else "pv1"
        print(f"{label}  PASS  found api_keys={list(found.keys())}")
    finally:
        sock.close()


# =========================================================
# nh4: multiple sequential ApiVersions requests, same conn
# =========================================================
def test_serial_requests(n=3):
    sock = connect()
    try:
        payload = ("0000001a0012000467890abc00096b61666b612d636c69"
                   "000a6b61666b612d636c6904302e3100")
        for i in range(n):
            raw, size, body = send_recv(sock, payload)
            correlation_id = struct.unpack(">i", body[0:4])[0]
            if correlation_id != 0x67890abc:
                raise Fail(f"request {i}: correlation_id mismatch")
            error_code = struct.unpack(">h", body[4:6])[0]
            if error_code != 0:
                raise Fail(f"request {i}: error_code {error_code} != 0")
        print(f"nh4  PASS  ({n} sequential requests on one connection all OK)")
    finally:
        sock.close()


# =========================================================
# sk0: multiple concurrent connections
# =========================================================
def test_concurrent_requests(n_clients=3):
    import threading

    payload = ("0000001a0012000467890abc00096b61666b612d636c69"
               "000a6b61666b612d636c6904302e3100")
    errors = []

    def worker(idx):
        try:
            s = connect()
            try:
                for _ in range(3):
                    raw, size, body = send_recv(s, payload)
                    correlation_id = struct.unpack(">i", body[0:4])[0]
                    if correlation_id != 0x67890abc:
                        errors.append(f"client {idx}: correlation_id mismatch")
                    error_code = struct.unpack(">h", body[4:6])[0]
                    if error_code != 0:
                        errors.append(f"client {idx}: error_code {error_code}")
            finally:
                s.close()
        except Exception as e:
            errors.append(f"client {idx}: {e}")

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_clients)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=TIMEOUT * 2)

    if errors:
        raise Fail("; ".join(errors))
    print(f"sk0  PASS  ({n_clients} concurrent clients all served correctly)")


# =========================================================
# vt6: DescribeTopicPartitions -- unknown topic "foo"
# =========================================================
def test_unknown_topic():
    sock = connect()
    try:
        payload = ("00000020004b00000000000700096b61666b612d636c69"
                   "000204666f6f0000000064ff00")
        raw, size, body = send_recv(sock, payload)

        expected_size = len(raw) - 4
        if size != expected_size:
            raise Fail(f"message_size {size} != actual remaining bytes {expected_size}")

        off = 0
        correlation_id = struct.unpack(">i", body[off:off + 4])[0]
        off += 4
        expected_corr = 0x00000007  # last 4 bytes before client_id in this payload
        # correlation_id bytes in payload are 00 00 00 07
        if correlation_id != 7:
            raise Fail(f"correlation_id mismatch: got {correlation_id}, want 7")

        tag, off = read_unsigned_varint(body, off)  # response header v1 tag buffer
        if tag != 0:
            raise Fail("expected empty TAG_BUFFER in response header v1")

        throttle = struct.unpack(">i", body[off:off + 4])[0]
        off += 4

        arr_len, off = read_unsigned_varint(body, off)
        arr_len -= 1
        if arr_len != 1:
            raise Fail(f"expected 1 topic in response, got {arr_len}")

        error_code = struct.unpack(">h", body[off:off + 2])[0]
        off += 2
        if error_code != 3:
            raise Fail(f"error_code should be 3 (UNKNOWN_TOPIC_OR_PARTITION), got {error_code}")

        name, off = read_compact_string(body, off)
        if name != "foo":
            raise Fail(f"topic_name mismatch: got {name!r}, want 'foo'")

        topic_id = body[off:off + 16]
        off += 16
        if topic_id != b"\x00" * 16:
            raise Fail(f"topic_id should be all zeros, got {topic_id.hex()}")

        is_internal = body[off]
        off += 1
        if is_internal != 0:
            raise Fail("is_internal should be false (0)")

        parts_len, off = read_unsigned_varint(body, off)
        parts_len -= 1
        if parts_len != 0:
            raise Fail(f"partitions array should be empty, got {parts_len} entries")

        authz = struct.unpack(">i", body[off:off + 4])[0]
        off += 4
        _tag, off = read_unsigned_varint(body, off)  # topic tag buffer

        next_cursor = struct.unpack(">b", body[off:off + 1])[0]
        off += 1
        if next_cursor != -1:
            raise Fail(f"next_cursor should be -1 (null), got {next_cursor}")

        _tag, off = read_unsigned_varint(body, off)  # final tag buffer

        if off != len(body):
            raise Fail(f"{len(body) - off} extra bytes left after decoding")

        print("vt6  PASS  (unknown topic 'foo' correctly reported)")
    finally:
        sock.close()


# =========================================================
# custom: true concurrency check -- a stalled client must
# NOT block other clients from being served
# =========================================================
def test_true_concurrency():
    import threading
    import time

    payload_prefix = "0000001a0012000467890abc00096b61666b612d636c69"
    payload_suffix = "000a6b61666b612d636c6904302e3100"
    payload = payload_prefix + payload_suffix

    results = {}

    def slow_client():
        s = connect()
        try:
            time.sleep(3)  # stall before sending anything
            s.sendall(bytes.fromhex(payload))
            read_message(s)
        finally:
            s.close()

    def fast_client():
        s = connect()
        try:
            start = time.time()
            s.sendall(bytes.fromhex(payload))
            read_message(s)
            results["fast_client_elapsed"] = time.time() - start
        finally:
            s.close()

    t_slow = threading.Thread(target=slow_client)
    t_slow.start()
    time.sleep(0.3)  # let the slow client connect first
    t_fast = threading.Thread(target=fast_client)
    t_fast.start()
    t_fast.join(timeout=TIMEOUT)
    t_slow.join(timeout=TIMEOUT + 2)

    elapsed = results.get("fast_client_elapsed")
    if elapsed is None:
        raise Fail("fast client never got a response in time")
    if elapsed > 1.0:
        raise Fail(f"fast client took {elapsed:.2f}s -- it was blocked behind the "
                    f"slow client instead of being served independently")
    print(f"true_concurrency  PASS  (fast client served in {elapsed:.2f}s, unaffected by stalled client)")


# =========================================================
# Fill these in once you have real topics in your local
# /tmp/kraft-combined-logs/__cluster_metadata-0/... log.
# Leave a stage's TOPIC as None to skip it until you're ready.
# =========================================================
EA7_TOPIC = None       # e.g. "foo"  (a topic with exactly 1 partition)
KU4_TOPIC = None       # e.g. "bar"  (a topic with exactly 2 partitions)
WQ2_TOPICS = None      # e.g. ["zebra", "apple"]  (two existing topics, any partition count)


def build_describe_topic_partitions_request(correlation_id, topic_names, partition_limit=100):
    """Build a raw DescribeTopicPartitions (v0) request for one or more topics."""
    client_id = b"kafka-cli"
    header = (
        struct.pack(">h", 75)              # request_api_key
        + struct.pack(">h", 0)             # request_api_version
        + struct.pack(">i", correlation_id)
        + struct.pack(">h", len(client_id)) + client_id  # NULLABLE_STRING
        + bytes([0])                       # header TAG_BUFFER
    )

    body = bytes([len(topic_names) + 1])   # topics compact array length
    for name in topic_names:
        name_bytes = name.encode("utf-8")
        body += bytes([len(name_bytes) + 1]) + name_bytes  # compact string
        body += bytes([0])                 # per-topic TAG_BUFFER
    body += struct.pack(">i", partition_limit)  # response_partition_limit
    body += bytes([0xFF])                  # cursor: null (-1 as int8)
    body += bytes([0])                     # body TAG_BUFFER

    message = header + body
    return struct.pack(">i", len(message)) + message


def read_compact_int32_array(buf, off):
    length, off = read_unsigned_varint(buf, off)
    length -= 1
    values = []
    for _ in range(length):
        values.append(struct.unpack(">i", buf[off:off + 4])[0])
        off += 4
    return values, off


def parse_describe_topic_partitions_response(body):
    off = 0
    correlation_id = struct.unpack(">i", body[off:off + 4])[0]
    off += 4
    _tag, off = read_unsigned_varint(body, off)   # header v1 TAG_BUFFER

    throttle = struct.unpack(">i", body[off:off + 4])[0]
    off += 4

    topics_len, off = read_unsigned_varint(body, off)
    topics_len -= 1

    topics = []
    for _ in range(topics_len):
        error_code = struct.unpack(">h", body[off:off + 2])[0]
        off += 2
        name, off = read_compact_string(body, off)
        topic_id = body[off:off + 16]
        off += 16
        is_internal = body[off]
        off += 1

        parts_len, off = read_unsigned_varint(body, off)
        parts_len -= 1
        partitions = []
        for _ in range(parts_len):
            p_error = struct.unpack(">h", body[off:off + 2])[0]
            off += 2
            p_index = struct.unpack(">i", body[off:off + 4])[0]
            off += 4
            leader_id = struct.unpack(">i", body[off:off + 4])[0]
            off += 4
            leader_epoch = struct.unpack(">i", body[off:off + 4])[0]
            off += 4
            _replicas, off = read_compact_int32_array(body, off)
            _isr, off = read_compact_int32_array(body, off)
            _elr, off = read_compact_int32_array(body, off)
            _lke, off = read_compact_int32_array(body, off)
            _offline, off = read_compact_int32_array(body, off)
            _ptag, off = read_unsigned_varint(body, off)
            partitions.append({"error_code": p_error, "partition_index": p_index})

        authz = struct.unpack(">i", body[off:off + 4])[0]
        off += 4
        _ttag, off = read_unsigned_varint(body, off)

        topics.append({
            "error_code": error_code,
            "name": name,
            "topic_id": topic_id,
            "is_internal": is_internal,
            "partitions": partitions,
        })

    next_cursor = struct.unpack(">b", body[off:off + 1])[0]
    off += 1
    _tag, off = read_unsigned_varint(body, off)

    if off != len(body):
        raise Fail(f"{len(body) - off} extra bytes left after decoding")

    return correlation_id, throttle, topics, next_cursor


# =========================================================
# ea7: DescribeTopicPartitions -- single existing topic,
# exactly one partition
# =========================================================
def test_ea7():
    if not EA7_TOPIC:
        print("ea7  SKIP  (set EA7_TOPIC at the top of this file first)")
        return
    sock = connect()
    try:
        req = build_describe_topic_partitions_request(111, [EA7_TOPIC])
        sock.sendall(req)
        raw, size, body = read_message(sock)
        correlation_id, throttle, topics, next_cursor = parse_describe_topic_partitions_response(body)

        if correlation_id != 111:
            raise Fail(f"correlation_id mismatch: got {correlation_id}")
        if len(topics) != 1:
            raise Fail(f"expected 1 topic in response, got {len(topics)}")

        t = topics[0]
        if t["name"] != EA7_TOPIC:
            raise Fail(f"topic_name mismatch: got {t['name']!r}")
        if t["error_code"] != 0:
            raise Fail(f"error_code should be 0, got {t['error_code']}")
        if t["topic_id"] == b"\x00" * 16:
            raise Fail("topic_id is all zeros -- topic not found in your metadata log")
        if len(t["partitions"]) != 1:
            raise Fail(f"expected 1 partition, got {len(t['partitions'])}")
        if t["partitions"][0]["error_code"] != 0:
            raise Fail("partition error_code should be 0")
        if next_cursor != -1:
            raise Fail(f"next_cursor should be -1, got {next_cursor}")

        print(f"ea7  PASS  (topic {EA7_TOPIC!r}, 1 partition, id={t['topic_id'].hex()})")
    finally:
        sock.close()


# =========================================================
# ku4: DescribeTopicPartitions -- single existing topic,
# exactly two partitions
# =========================================================
def test_ku4():
    if not KU4_TOPIC:
        print("ku4  SKIP  (set KU4_TOPIC at the top of this file first)")
        return
    sock = connect()
    try:
        req = build_describe_topic_partitions_request(222, [KU4_TOPIC])
        sock.sendall(req)
        raw, size, body = read_message(sock)
        correlation_id, throttle, topics, next_cursor = parse_describe_topic_partitions_response(body)

        if len(topics) != 1:
            raise Fail(f"expected 1 topic, got {len(topics)}")
        t = topics[0]
        if t["error_code"] != 0:
            raise Fail(f"error_code should be 0, got {t['error_code']}")
        if len(t["partitions"]) != 2:
            raise Fail(f"expected 2 partitions, got {len(t['partitions'])}")
        indexes = sorted(p["partition_index"] for p in t["partitions"])
        if indexes != [0, 1]:
            raise Fail(f"expected partition_index values [0, 1], got {indexes}")
        for p in t["partitions"]:
            if p["error_code"] != 0:
                raise Fail(f"partition {p['partition_index']} has error_code {p['error_code']}")

        print(f"ku4  PASS  (topic {KU4_TOPIC!r}, partitions={indexes})")
    finally:
        sock.close()


# =========================================================
# wq2: DescribeTopicPartitions -- multiple topics, must be
# sorted alphabetically by name in the response
# =========================================================
def test_wq2():
    if not WQ2_TOPICS or len(WQ2_TOPICS) < 2:
        print("wq2  SKIP  (set WQ2_TOPICS to two topic names at the top of this file first)")
        return
    sock = connect()
    try:
        req = build_describe_topic_partitions_request(333, WQ2_TOPICS)
        sock.sendall(req)
        raw, size, body = read_message(sock)
        correlation_id, throttle, topics, next_cursor = parse_describe_topic_partitions_response(body)

        if len(topics) != len(WQ2_TOPICS):
            raise Fail(f"expected {len(WQ2_TOPICS)} topics, got {len(topics)}")

        names = [t["name"] for t in topics]
        if names != sorted(names):
            raise Fail(f"topics not sorted alphabetically: got {names}")

        expected_set = set(WQ2_TOPICS)
        got_set = set(names)
        if got_set != expected_set:
            raise Fail(f"topic name mismatch: got {got_set}, want {expected_set}")

        for t in topics:
            if t["error_code"] != 0:
                raise Fail(f"topic {t['name']!r} has error_code {t['error_code']}")

        print(f"wq2  PASS  (topics sorted correctly: {names})")
    finally:
        sock.close()


STAGES = [
    ("wa6 - correlation id echo", test_correlation_id),
    ("nc5 - unsupported version -> error 35", test_nc5),
    ("pv1 - ApiVersions body", lambda: test_apiversions(False)),
    ("nh4 - serial requests", test_serial_requests),
    ("sk0 - concurrent requests", test_concurrent_requests),
    ("custom - true concurrency (stall test)", test_true_concurrency),
    ("yk1 - ApiVersions incl. DescribeTopicPartitions", lambda: test_apiversions(True)),
    ("vt6 - DescribeTopicPartitions unknown topic", test_unknown_topic),
    ("ea7 - DescribeTopicPartitions single partition", test_ea7),
    ("ku4 - DescribeTopicPartitions multiple partitions", test_ku4),
    ("wq2 - DescribeTopicPartitions multiple topics", test_wq2),
]


def main():
    print(f"Testing broker at {HOST}:{PORT}\n")
    passed, failed = 0, 0
    for name, fn in STAGES:
        try:
            fn()
            passed += 1
        except Fail as e:
            print(f"{name}  FAIL  -> {e}")
            failed += 1
        except (ConnectionRefusedError, socket.timeout) as e:
            print(f"{name}  FAIL  -> could not talk to broker: {e}")
            failed += 1
        except Exception as e:
            print(f"{name}  FAIL  -> unexpected error: {e!r}")
            failed += 1
    print(f"\n{passed} passed, {failed} failed")


if __name__ == "__main__":
    main()