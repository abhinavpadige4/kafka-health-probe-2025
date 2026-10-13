#!/usr/bin/env python3
"""
Kafka health probe script.

Features:
- Checks broker connectivity
- Checks consumer group lag for a given topic/group
- Retries with exponential backoff on transient errors
- Logs to stdout and probe.log with timestamps
- Configurable via JSON/YAML file passed with --config
- Exits with non-zero status on critical failures
"""

import argparse
import json
import logging
import sys
import time
from pathlib import Path

try:
    import yaml
except ImportError:
    yaml = None

from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition
from kafka.errors import KafkaError, NoBrokersAvailable

# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------
logger = logging.getLogger("kafka_probe")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

# stdout handler
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setFormatter(formatter)
logger.addHandler(stdout_handler)

# file handler
file_handler = logging.FileHandler("probe.log")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def load_config(path: str):
    """Load configuration from JSON or YAML file.

    Expected keys:
        brokers: list of broker host:port strings
        topic: topic name to check
        consumer_group: consumer group id
        retry:
            max_attempts: int (default 5)
            base_delay: seconds (default 1)
    """
    p = Path(path)
    if not p.is_file():
        logger.error(f"Config file {path} does not exist")
        sys.exit(1)
    try:
        if p.suffix in {".yaml", ".yml"}:
            if yaml is None:
                logger.error("PyYAML not installed; cannot parse YAML config")
                sys.exit(1)
            return yaml.safe_load(p.read_text())
        else:
            return json.loads(p.read_text())
    except Exception as e:
        logger.exception(f"Failed to parse config file: {e}")
        sys.exit(1)


def exponential_backoff(attempt, base_delay):
    return base_delay * (2 ** (attempt - 1))


def check_brokers(brokers, max_attempts=5, base_delay=1):
    """Attempt to connect to the cluster and list brokers.
    Returns True on success, False on permanent failure.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            admin = KafkaAdminClient(bootstrap_servers=brokers, request_timeout_ms=5000)
            broker_info = admin.describe_cluster()
            logger.info(f"Connected to cluster. Controller: {broker_info.controller_id}, Brokers: {broker_info.brokers}")
            admin.close()
            return True
        except (NoBrokersAvailable, KafkaError) as e:
            logger.warning(f"Attempt {attempt}/{max_attempts} – broker connection failed: {e}")
            if attempt == max_attempts:
                logger.error("Exceeded maximum broker connection attempts")
                return False
            delay = exponential_backoff(attempt, base_delay)
            logger.info(f"Retrying in {delay}s...")
            time.sleep(delay)
    return False


def get_consumer_group_lag(brokers, topic, group_id, max_attempts=5, base_delay=1):
    """Calculate total lag for a consumer group on a specific topic.
    Returns lag integer or None on failure.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=brokers,
                group_id=group_id,
                enable_auto_commit=False,
                consumer_timeout_ms=5000,
            )
            partitions = consumer.partitions_for_topic(topic)
            if not partitions:
                logger.error(f"Topic {topic} not found or has no partitions")
                consumer.close()
                return None
            tps = [TopicPartition(topic, p) for p in partitions]
            consumer.assign(tps)
            # fetch latest offsets (end)
            end_offsets = consumer.end_offsets(tps)
            # fetch committed offsets for the group
            committed = consumer.committed
            total_lag = 0
            for tp in tps:
                committed_offset = consumer.committed(tp)
                if committed_offset is None:
                    committed_offset = 0
                lag = end_offsets[tp] - committed_offset
                total_lag += max(lag, 0)
            logger.info(f"Consumer group '{group_id}' lag for topic '{topic}': {total_lag}")
            consumer.close()
            return total_lag
        except (KafkaError, Exception) as e:
            logger.warning(f"Attempt {attempt}/{max_attempts} – lag check failed: {e}")
            if attempt == max_attempts:
                logger.error("Exceeded maximum attempts for lag check")
                return None
            delay = exponential_backoff(attempt, base_delay)
            logger.info(f"Retrying in {delay}s...")
            time.sleep(delay)
    return None


def main():
    parser = argparse.ArgumentParser(description="Kafka overnight health probe")
    parser.add_argument("--config", required=True, help="Path to JSON/YAML config file")
    args = parser.parse_args()

    cfg = load_config(args.config)
    brokers = cfg.get("brokers")
    topic = cfg.get("topic")
    group_id = cfg.get("consumer_group")
    retry_cfg = cfg.get("retry", {})
    max_attempts = retry_cfg.get("max_attempts", 5)
    base_delay = retry_cfg.get("base_delay", 1)

    if not (brokers and topic and group_id):
        logger.error("Config must contain 'brokers', 'topic', and 'consumer_group'")
        sys.exit(1)

    # 1. Broker connectivity
    if not check_brokers(brokers, max_attempts, base_delay):
        sys.exit(2)

    # 2. Consumer group lag
    lag = get_consumer_group_lag(brokers, topic, group_id, max_attempts, base_delay)
    if lag is None:
        sys.exit(3)
    # Critical threshold can be added via config; for now just log.

    # Successful run
    logger.info("Kafka health probe completed successfully")
    sys.exit(0)

if __name__ == "__main__":
    main()
