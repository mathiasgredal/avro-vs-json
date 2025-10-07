#!/usr/bin/env python3
"""
Kafka benchmark: JSON vs Avro serialization
Simple, linear workflow for easy verification
"""

import time
import json
import io
import argparse
from pathlib import Path
from confluent_kafka import Producer, Consumer
from fastavro import schemaless_writer, schemaless_reader
import orjson

# ============================================================================
# Configuration
# ============================================================================

TEXT_MESSAGES = [
    "Hello world",
    "Quick brown fox jumps over the lazy dog",
    "Kafka benchmarking message",
    "Avro vs JSON throughput test",
    "Message compression experiment",
]


# ============================================================================
# Helper Functions
# ============================================================================

def build_record(i: int) -> dict:
    """Build a single benchmark record"""
    return {
        "id": str(i),
        "ts_unix_ms": int(time.time() * 1000),
        "category": "bench",
        "value": float(i % 1000) / 1000.0,
        "message": TEXT_MESSAGES[i % len(TEXT_MESSAGES)],
    }


def load_avro_schema() -> dict:
    """Load Avro schema from file"""
    schema_path = Path(__file__).resolve().parents[1] / "schemas" / "event.avsc"
    with open(schema_path) as f:
        return json.load(f)


_bytesio = io.BytesIO()

def serialize_record(record: dict, mode: str, schema: dict = None) -> bytes:
    """Serialize record to JSON or Avro"""
    if mode == "json":
        return orjson.dumps(record)
    else:
        _bytesio.seek(0)
        _bytesio.truncate(0)
        schemaless_writer(_bytesio, schema, record)
        return _bytesio.getvalue()


def deserialize_record(data: bytes, mode: str, schema: dict = None) -> dict:
    """Deserialize record from JSON or Avro"""
    if mode == "json":
        return json.loads(data.decode("utf-8"))
    else:
        return schemaless_reader(io.BytesIO(data), schema)


def sum_message_lengths(record: dict) -> int:
    """Calculate total length of message field"""
    return len(record.get("message", ""))


# ============================================================================
# Main Benchmark
# ============================================================================

def main():
    # Parse arguments
    parser = argparse.ArgumentParser(description="Kafka JSON vs Avro benchmark")
    parser.add_argument("--mode", choices=["json", "avro"], required=True)
    parser.add_argument("--messages", type=int, default=100000)
    parser.add_argument("--batch", type=int, default=100)
    parser.add_argument("--compression", default="gzip")
    parser.add_argument("--brokers", default="localhost:9094")
    parser.add_argument("--topic", default="")
    parser.add_argument("--large", action="store_true")
    args = parser.parse_args()

    topic = args.topic or (f"bench-{args.mode}")
    schema = load_avro_schema() if args.mode == "avro" else None

    global TEXT_MESSAGES
    if args.large:
        TEXT_MESSAGES = [text * 1000 for text in TEXT_MESSAGES]

    # ============================================================================
    # Step 1: Start Consumer (before producing)
    # ============================================================================

    consumer_config = {
        "bootstrap.servers": args.brokers,
        "group.id": f"bench-python-{args.mode}-{int(time.time() * 1000)}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }
    consumer = Consumer(consumer_config)
    consumer.subscribe([topic])

    consumed_count = 0
    total_message_length = 0
    consume_start = None
    consume_end = None

    # ============================================================================
    # Step 2: Produce Messages (with concurrent consumption)
    # ============================================================================

    producer_config = {
        "bootstrap.servers": args.brokers,
        "compression.type": args.compression,
        "batch.num.messages": args.batch,
    }
    producer = Producer(producer_config)

    produce_start = time.time()
    produced_count = 0

    for i in range(args.messages):
        record = build_record(i)
        value = serialize_record(record, args.mode, schema)
        producer.produce(topic=topic, value=value)
        produced_count += 1
        
        # Poll producer to handle delivery callbacks
        if produced_count % args.batch == 0:
            producer.poll(0)

    # Flush all remaining messages
    producer.flush()
    produce_end = time.time()

    # ============================================================================
    # Step 3: Consume Until All Messages Received
    # ============================================================================

    # Poll for messages until we've consumed everything we produced
    while consumed_count < produced_count:
        msg = consumer.poll(timeout=1.0)
        
        if msg is None:
            continue
        if msg.error():
            continue
        
        value = msg.value()
        if not value:
            continue
        
        # Mark consumption start time on first message
        if consume_start is None:
            consume_start = time.time()
        
        try:
            record = deserialize_record(value, args.mode, schema)
            total_message_length += sum_message_lengths(record)
            consumed_count += 1
        except Exception:
            # Skip malformed messages
            continue

    consume_end = time.time()
    consumer.close()

    # ============================================================================
    # Step 4: Report Results
    # ============================================================================

    produce_duration = produce_end - produce_start
    produce_rate = produced_count / produce_duration if produce_duration > 0 else 0
    consume_duration = (consume_end - consume_start) if consume_start and consume_end else 0

    result = {
        "lang": "python",
        "mode": args.mode,
        "topic": topic,
        "produced": produced_count,
        "durationSec": produce_duration,
        "rate": produce_rate,
        "consumed": consumed_count,
        "stringLenSum": total_message_length,
        "consumeDurSec": consume_duration,
    }

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()