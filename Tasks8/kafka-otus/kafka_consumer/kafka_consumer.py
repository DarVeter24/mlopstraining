#!/usr/bin/env python
"""OTUS BigData ML kafka consumer example (adapted for local PLAINTEXT Kafka)"""

import json
import argparse

from kafka import KafkaConsumer


def main():
    argparser = argparse.ArgumentParser(description=__doc__)
    argparser.add_argument(
        "-g", "--group_id", required=True, help="Kafka consumer group_id"
    )
    argparser.add_argument(
        "-b",
        "--bootstrap_server",
        default="localhost:29093",
        help="Kafka server address:port",
    )
    argparser.add_argument(
        "-t", "--topic", default="clicks", help="Kafka topic to consume"
    )

    args = argparser.parse_args()

    consumer = KafkaConsumer(
        bootstrap_servers=args.bootstrap_server,
        group_id=args.group_id,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",  # для чтения с начала топика
    )

    consumer.subscribe(topics=[args.topic])
    print_new_messages(consumer)


def print_new_messages(consumer):
    count = 0
    print("Waiting for new messages. Press Ctrl+C to stop")
    try:
        for msg in consumer:
            print(
                f"{msg.topic}:{msg.partition}:{msg.offset}: key={msg.key} value={msg.value}"
            )
            count += 1
    except KeyboardInterrupt:
        pass
    print(f"Total {count} messages received")


if __name__ == "__main__":
    main()