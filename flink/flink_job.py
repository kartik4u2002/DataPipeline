"""
flink_job.py

PyFlink DataStream job suitable for Flink 2.1.0.
Reads JSON messages (strings) from Kafka, extracts "content", computes a
5-minute tumbling processing-time window, and prints a JSON summary per window.

Notes:
- Ensure the Kafka connector jar for Flink 2.x is available to the runtime,
  e.g. /opt/flink/lib/flink-connector-kafka-4.0.1-2.0.jar (mount or copy it).
- This job intentionally uses only DataStream APIs and the built-in print sink
  so it does NOT depend on the newer sink2 API.
"""

import os
import json
from typing import Iterable

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import Time
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.datastream.functions import ProcessWindowFunction, MapFunction
from pyflink.common.typeinfo import Types


# Configuration (via env vars with sensible defaults)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "flink_input_topic")
GROUP_ID = os.getenv("GROUP_ID", "pyflink-consumer-group")
PARALLELISM = int(os.getenv("PARALLELISM", "1"))

# If you want to add the connector jar programmatically, point to the kafka connector jar (optional).
# Recommended: mount the correct connector jar into /opt/flink/lib in your Docker image / compose.
KAFKA_CONNECTOR_JAR = os.getenv(
    "KAFKA_CONNECTOR_JAR",
    "file:///opt/flink/lib/flink-connector-kafka-4.0.1-2.0.jar"  # adjust if different
)


class ExtractContentMap(MapFunction):
    """Parse JSON string and return the 'content' field or None."""
    def map(self, value: str):
        if value is None:
            return None
        try:
            obj = json.loads(value)
            if isinstance(obj, dict):
                return obj.get("content")
            return None
        except Exception:
            return None


class CollectWindowContents(ProcessWindowFunction):
    """
    ProcessWindowFunction that collects non-null content values in a window
    and emits a JSON summary string.
    """
    def process(self, key: str, context, elements: Iterable, out):
        # elements is an iterable of values (strings)
        contents = [e for e in elements if e is not None]
        window_start = context.window().start
        window_end = context.window().end

        result = {
            "window_start_ms": window_start,
            "window_end_ms": window_end,
            "count": len(contents),
            "sample_contents": contents[:10],
        }
        out.collect(json.dumps(result))


def build_stream():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(PARALLELISM)

    # Optionally attach the kafka connector jar that matches Flink 2.x runtime.
    # If you mounted the jar into /opt/flink/lib, this is optional — Flink will pick
    # it up from its classpath. Keep or remove as suits your deployment.
    try:
        env.add_jars(KAFKA_CONNECTOR_JAR)
    except Exception:
        # env.add_jars may not be necessary if jar is already in /opt/flink/lib.
        # Ignore failures here so job can still start if jar was mounted instead.
        pass

    # Kafka consumer properties
    kafka_props = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        # you can add additional kafka consumer props here if needed
    }

    # Build the FlinkKafkaConsumer (deserializes raw JSON strings)
    kafka_consumer = FlinkKafkaConsumer(
        topics=KAFKA_TOPIC,
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )

    # 1. Source
    ds = env.add_source(kafka_consumer).name("kafka-source")

    # 2. Map -> extract 'content'
    contents = ds.map(ExtractContentMap(), output_type=Types.STRING())

    # 3. Filter out None results
    filtered = contents.filter(lambda x: x is not None)

    # 4. Key by a constant so all events go to the same keyed stream (global window)
    keyed = filtered.key_by(lambda x: "all")

    # 5. Tumbling processing-time window of 5 minutes
    windowed = keyed.window(TumblingProcessingTimeWindows.of(Time.minutes(5)))

    # 6. Process with custom window function that emits JSON summary
    summarized = windowed.process(
        CollectWindowContents(),
        output_type=Types.STRING()
    )

    # 7. Print sink (uses older stable sink, avoids sink2 API)
    summarized.print().name("print-sink")

    return env


if __name__ == "__main__":
    env = build_stream()
    # Execute the job. This triggers the job submission when run via 'flink run -py'
    env.execute("kafka-json-content-5min-window")
