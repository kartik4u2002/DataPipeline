"""
flink_job.py

PyFlink DataStream job that reads JSON messages from Kafka, extracts the "content" key,
and computes a tumbling processing-time window of 5 minutes.

To run: submit this as a Flink job (flink run -py flink_job.py) or run in your PyFlink environment.
"""

import os
import json
from typing import List

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer  # Fixed import
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import Time
from pyflink.datastream.window import TumblingProcessingTimeWindows, TimeWindow
from pyflink.datastream.functions import ProcessWindowFunction, MapFunction
from pyflink.common.typeinfo import Types


# Read configuration from environment with safe defaults
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "flink_input_topic")
GROUP_ID = os.getenv("GROUP_ID", "pyflink-consumer-group")


class ExtractContentMap(MapFunction):
    """MapFunction to parse JSON and extract the 'content' key."""

    def map(self, value: str):
        try:
            obj = json.loads(value)
            content = obj.get("content") if isinstance(obj, dict) else None
            return content
        except Exception:
            # Return None if parsing fails or 'content' is missing
            return None


class CollectWindowContents(ProcessWindowFunction):
    """Collects all non-null content strings in the window and emits a summary dict."""

    def process(self, key, context: ProcessWindowFunction.Context, elements, out):
        # elements is an iterable of all items in the window
        contents = [e for e in elements if e is not None]
        
        window_start = context.window().start
        window_end = context.window().end
        
        result = {
            "window_start_ms": window_start,
            "window_end_ms": window_end,
            "count": len(contents),
            "sample_contents": contents[:10],  # Get first 10 samples
        }
        # Emit the result as a JSON string
        out.collect(json.dumps(result))


def build_stream():
    print("gfrihfrihfhrhfiwhihihwefw-111112")
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars(f"file:////opt/flink/lib/flink-sql-connector-kafka-4.0.0-2.0.jar")
    env.set_parallelism(1)

    print("gfrihfrihfhrhfiwhihihwefw-111112-dfefwfe")

    # Create Kafka consumer using bootstrap value from environment
    kafka_props = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,  # Fixed: Use env variable
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest"
    }
    print("gfrihfrihfhrhfiwhihihwefw-111112-dfefwfe-3333")

    kafka_consumer = FlinkKafkaConsumer(
        topics=KAFKA_TOPIC,
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props,
    )
    print("gfrihfrihfhrhfiwhihihwefw-111112-dfefwfe-444")
    


    # 1. Add Kafka as a source
    ds = env.add_source(kafka_consumer).name("kafka-source")

    # 2. Map: Parse JSON and extract "content"
    contents = ds.map(ExtractContentMap(), output_type=Types.STRING())

    # 3. Filter out any messages that failed parsing (are None)
    filtered = contents.filter(lambda x: x is not None)
    
    # 4. Key: Group all messages into a single stream for a global window
    keyed = filtered.key_by(lambda x: "all")
    
    # 5. Window: Define a 5-minute tumbling processing time window
    windowed = keyed.window(TumblingProcessingTimeWindows.of(Time.minutes(5)))
    
    # 6. Process: Apply the custom window function to summarize results
    summarized = windowed.process(
        CollectWindowContents(), 
        output_type=Types.STRING()
    )

    print("gfrihfrihfhrhfiwhihihwefw-111112-dfefwfe-gggg")

    # 7. Sink: Print the resulting JSON summary to the console
    summarized.print().name("print-sink")


    print("gfrihfrihfhrhfiwhihihwefw")

    return env


if __name__ == "__main__":
    env = build_stream()
    # Execute the Flink job
    env.execute("kafka-json-content-5min-window")