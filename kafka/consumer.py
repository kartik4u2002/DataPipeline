# import os
# import json
# import time
# import logging
# from dotenv import load_dotenv
# from kafka import KafkaConsumer, KafkaProducer
# import requests

# # Load .env
# load_dotenv()

# # Configs
# KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka-broker:9092")
# KAFKA_CONSUMER_TOPIC = os.getenv("KAFKA_CONSUMER_TOPIC", "news_topic")
# FLINK_INPUT_TOPIC = os.getenv("FLINK_INPUT_TOPIC", "flink_input_topic")
# FORWARD_MODE = os.getenv("FORWARD_MODE", "kafka").lower()  # "kafka" or "http"
# FLINK_HTTP_ENDPOINT = os.getenv("FLINK_HTTP_ENDPOINT", "")
# CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "news-consumer-group")
# POLL_TIMEOUT_MS = int(os.getenv("POLL_TIMEOUT_MS", "1000"))
# AUTO_COMMIT = os.getenv("AUTO_COMMIT", "true").lower() in ("1", "true", "yes")

# # Logging
# logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
# logger = logging.getLogger("news-consumer")

# def create_consumer():
#     logger.info("Connecting Kafka consumer to broker: %s", KAFKA_BROKER)
#     consumer = KafkaConsumer(
#         KAFKA_CONSUMER_TOPIC,
#         bootstrap_servers=[KAFKA_BROKER],
#         group_id=CONSUMER_GROUP,
#         auto_offset_reset="earliest",
#         enable_auto_commit=AUTO_COMMIT,
#         value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v is not None else None,
#     )
#     return consumer

# def create_producer():
#     logger.info("Connecting Kafka producer to broker: %s", KAFKA_BROKER)
#     producer = KafkaProducer(
#         bootstrap_servers=[KAFKA_BROKER],
#         value_serializer=lambda v: json.dumps(v).encode("utf-8"),
#         key_serializer=lambda k: k.encode("utf-8") if k else None,
#     )
#     return producer

# def forward_to_kafka(producer, message, key=None):
#     try:
#         producer.send(FLINK_INPUT_TOPIC, key=key, value=message)
#         # Optionally block until sent: producer.flush()
#     except Exception as e:
#         logger.exception("Failed to forward message to Kafka: %s", e)

# def forward_to_http(message):
#     if not FLINK_HTTP_ENDPOINT:
#         logger.error("FLINK_HTTP_ENDPOINT not set. Cannot forward to HTTP.")
#         return
#     try:
#         resp = requests.post(FLINK_HTTP_ENDPOINT, json=message, timeout=10)
#         if resp.status_code >= 200 and resp.status_code < 300:
#             logger.debug("HTTP forward success: %s", resp.status_code)
#         else:
#             logger.warning("HTTP forward returned %s: %s", resp.status_code, resp.text)
#     except Exception as e:
#         logger.exception("Failed to forward message to Flink HTTP endpoint: %s", e)

# def main():
#     logger.info("Starting consumer. Forward mode: %s", FORWARD_MODE)
#     consumer = create_consumer()

#     producer = None
#     if FORWARD_MODE == "kafka":
#         producer = create_producer()
#     elif FORWARD_MODE == "http":
#         if not FLINK_HTTP_ENDPOINT:
#             logger.error("FLINK_HTTP_ENDPOINT is not configured. Exiting.")
#             return
#     else:
#         logger.error("Unknown FORWARD_MODE: %s. Use 'kafka' or 'http'.", FORWARD_MODE)
#         return

#     try:
#         for msg in consumer:
#             # msg.value is already deserialized to Python object
#             message_value = msg.value
#             message_key = msg.key.decode("utf-8") if msg.key else None

#             logger.info("Consumed message offset=%s partition=%s key=%s",
#                         msg.offset, msg.partition, message_key)
#             print("\n================= RAW MESSAGE =================")
#             print("Message Key:", message_key)
#             print("Message Type:", type(message_value))
#             print("Message Value (Pretty JSON):")
#             try:
#                 print(json.dumps(message_value, indent=4))
#             except Exception:
#                 print(message_value)
#             print("===============================================\n")
#             # Optionally attach metadata
#             payload = {
#                 "kafka_topic": msg.topic,
#                 "kafka_partition": msg.partition,
#                 "kafka_offset": msg.offset,
#                 "received_at": int(time.time()),
#                 "payload": message_value,
#             }

#             if FORWARD_MODE == "kafka":
#                 forward_to_kafka(producer, payload, key=message_key)
#             else:
#                 forward_to_http(payload)

#             # If not auto-committing, you should commit manually:
#             if not AUTO_COMMIT:
#                 consumer.commit()
#     except KeyboardInterrupt:
#         logger.info("Stopped by user")
#     except Exception as e:
#         logger.exception("Unexpected error in consumer loop: %s", e)
#     finally:
#         logger.info("Closing consumer/producer")
#         try:
#             consumer.close()
#         except Exception:
#             pass
#         if producer:
#             try:
#                 producer.flush()
#                 producer.close()
#             except Exception:
#                 pass

# if __name__ == "__main__":
#     main()


# import os
# import json
# import time
# import logging
# from dotenv import load_dotenv
# from kafka import KafkaConsumer, KafkaProducer
# import requests

# # Load .env
# load_dotenv()

# # Configs
# KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka-broker:9092")
# KAFKA_CONSUMER_TOPIC = os.getenv("KAFKA_CONSUMER_TOPIC", "news_topic")
# FLINK_INPUT_TOPIC = os.getenv("FLINK_INPUT_TOPIC", "flink_input_topic")
# FORWARD_MODE = os.getenv("FORWARD_MODE", "kafka").lower()  # "kafka" or "http"
# FLINK_HTTP_ENDPOINT = os.getenv("FLINK_HTTP_ENDPOINT", "")
# CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "news-consumer-group")
# POLL_TIMEOUT_MS = int(os.getenv("POLL_TIMEOUT_MS", "1000"))
# AUTO_COMMIT = os.getenv("AUTO_COMMIT", "true").lower() in ("1", "true", "yes")

# # Logging
# logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
# logger = logging.getLogger("news-consumer")


# def create_consumer():
#     logger.info("Connecting Kafka consumer to broker: %s", KAFKA_BROKER)
#     consumer = KafkaConsumer(
#         KAFKA_CONSUMER_TOPIC,
#         bootstrap_servers=[KAFKA_BROKER],
#         group_id=CONSUMER_GROUP,
#         auto_offset_reset="earliest",
#         enable_auto_commit=AUTO_COMMIT,
#         value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v is not None else None,
#     )
#     return consumer


# def create_producer():
#     logger.info("Connecting Kafka producer to broker: %s", KAFKA_BROKER)
#     producer = KafkaProducer(
#         bootstrap_servers=[KAFKA_BROKER],
#         value_serializer=lambda v: json.dumps(v).encode("utf-8"),
#         key_serializer=lambda k: k.encode("utf-8") if k else None,
#     )
#     return producer


# def forward_to_kafka(producer, message, key=None):
#     try:
#         producer.send(FLINK_INPUT_TOPIC, key=key, value=message)
#         # Optionally block until sent: producer.flush()
#     except Exception as e:
#         logger.exception("Failed to forward message to Kafka: %s", e)


# def forward_to_http(message):
#     if not FLINK_HTTP_ENDPOINT:
#         logger.error("FLINK_HTTP_ENDPOINT not set. Cannot forward to HTTP.")
#         return
#     try:
#         resp = requests.post(FLINK_HTTP_ENDPOINT, json=message, timeout=10)
#         if 200 <= resp.status_code < 300:
#             logger.debug("HTTP forward success: %s", resp.status_code)
#         else:
#             logger.warning("HTTP forward returned %s: %s", resp.status_code, resp.text)
#     except Exception as e:
#         logger.exception("Failed to forward message to Flink HTTP endpoint: %s", e)


# # ---------------------------
# # Processing helpers & logic
# # ---------------------------
# OUTPUT_KEYS = ["source", "author", "title", "description", "url", "publishedAt", "content"]


# def _safe_load(value):
#     """Parse JSON string values to Python objects when possible."""
#     if isinstance(value, str):
#         try:
#             return json.loads(value)
#         except json.JSONDecodeError:
#             return value
#     return value


# def _get_from_candidates(obj, candidates):
#     """Case-insensitive lookup in dict for first candidate that exists and is non-empty."""
#     if not isinstance(obj, dict):
#         return None
#     lower_map = {k.lower(): k for k in obj.keys()}
#     for cand in candidates:
#         key = cand.lower()
#         if key in lower_map:
#             val = obj[lower_map[key]]
#             if val is not None and val != "":
#                 return val
#     return None


# def _first_available(value, keys):
#     """Given value (dict/list/primitive), try to locate the first existing field from keys."""
#     if value is None:
#         return None
#     # If list, try first element
#     if isinstance(value, list):
#         if not value:
#             return None
#         return _first_available(value[0], keys)
#     # If dict, try keys and some common wrappers
#     if isinstance(value, dict):
#         found = _get_from_candidates(value, keys)
#         if found is not None:
#             return found
#         for wrapper in ("payload", "data", "message", "body", "article", "item"):
#             if wrapper in value and isinstance(value[wrapper], (dict, list)):
#                 res = _first_available(value[wrapper], keys)
#                 if res is not None:
#                     return res
#         return None
#     # Primitive (string/number) fallback
#     if isinstance(value, (str, int, float)):
#         return value
#     return None


# def _extract_source(raw):
#     """Normalize source field (dict with name/id or string)."""
#     if raw is None:
#         return None
#     if isinstance(raw, str):
#         return raw.strip() or None
#     if isinstance(raw, dict):
#         name = _get_from_candidates(raw, ["name", "title", "id"])
#         if name:
#             return name.strip() if isinstance(name, str) else str(name)
#         try:
#             return json.dumps(raw, ensure_ascii=False)
#         except Exception:
#             return str(raw)
#     return str(raw)


# def _extract_published(raw):
#     """Return published value as string (don't try to reformat)."""
#     if raw is None:
#         return None
#     if isinstance(raw, str):
#         return raw.strip() or None
#     if isinstance(raw, (int, float)):
#         return str(raw)
#     return None


# def process_message(message_value):
#     """
#     Accepts a Kafka message value (dict or JSON string) and extracts:
#     source, author, title, description, url, publishedAt, content
#     Returns a dict with those keys (values may be None).
#     """
#     msg = _safe_load(message_value)

#     # unwrap common top-level wrappers
#     if isinstance(msg, dict):
#         for wrapper in ("payload", "data", "message", "body", "article"):
#             if wrapper in msg and isinstance(msg[wrapper], (dict, list)):
#                 msg = msg[wrapper]
#                 break

#     # if list, pick the first article
#     if isinstance(msg, list):
#         msg = msg[0] if msg else {}

#     # if after all this it's not a dict, return empty result
#     if not isinstance(msg, dict):
#         return {k: None for k in OUTPUT_KEYS}

#     # candidate keys
#     source_candidates = ["source", "source_name", "sourceName", "publisher"]
#     author_candidates = ["author", "byline", "creator", "writer"]
#     title_candidates = ["title", "headline", "heading"]
#     description_candidates = ["description", "summary", "abstract"]
#     url_candidates = ["url", "link", "articleUrl", "sourceUrl", "web_url"]
#     published_candidates = ["publishedAt", "published_at", "pubDate", "published", "date"]
#     content_candidates = ["content", "fullText", "article_content", "body", "text"]

#     raw_source = _first_available(msg, source_candidates)
#     raw_author = _first_available(msg, author_candidates)
#     raw_title = _first_available(msg, title_candidates)
#     raw_description = _first_available(msg, description_candidates)
#     raw_url = _first_available(msg, url_candidates)
#     raw_published = _first_available(msg, published_candidates)
#     raw_content = _first_available(msg, content_candidates)

#     # If msg has nested source object like {"source": {"name": "..."}}
#     if "source" in msg and isinstance(msg["source"], dict) and not raw_source:
#         raw_source = _get_from_candidates(msg["source"], ["name", "id", "title"])

#     result = {
#         "source": _extract_source(raw_source),
#         "author": (raw_author.strip() if isinstance(raw_author, str) and raw_author.strip() else None),
#         "title": (raw_title.strip() if isinstance(raw_title, str) and raw_title.strip() else None),
#         "description": (raw_description.strip() if isinstance(raw_description, str) and raw_description.strip() else None),
#         "url": (raw_url.strip() if isinstance(raw_url, str) and raw_url.strip() else None),
#         "publishedAt": _extract_published(raw_published),
#         "content": (raw_content.strip() if isinstance(raw_content, str) and raw_content.strip() else None),
#     }

#     return result


# # ---------------------------
# # End processing helpers
# # ---------------------------


# def main():
#     logger.info("Starting consumer. Forward mode: %s", FORWARD_MODE)
#     consumer = create_consumer()

#     producer = None
#     if FORWARD_MODE == "kafka":
#         producer = create_producer()
#     elif FORWARD_MODE == "http":
#         if not FLINK_HTTP_ENDPOINT:
#             logger.error("FLINK_HTTP_ENDPOINT is not configured. Exiting.")
#             return
#     else:
#         logger.error("Unknown FORWARD_MODE: %s. Use 'kafka' or 'http'.", FORWARD_MODE)
#         return

#     try:
#         for msg in consumer:
#             # msg.value is already deserialized to Python object (per your deserializer)
#             message_value = msg.value
#             message_key = msg.key.decode("utf-8") if msg.key else None

#             logger.info("Consumed message offset=%s partition=%s key=%s",
#                         msg.offset, msg.partition, message_key)

#             # ---- print BEFORE processing (raw)
#             print("\n>>>> BEFORE PROCESSING (raw message) <<<<")
#             print("Message Key:", message_key)
#             print("Message Type:", type(message_value))
#             print("Message Value (Pretty JSON):")
#             try:
#                 # Attempt to pretty-print; if message_value includes non-serializable types this will fail
#                 print(json.dumps(message_value, indent=4, ensure_ascii=False))
#             except Exception:
#                 print(message_value)
#             print("===============================================\n")

#             # ---- process message to extract required fields
#             processed = process_message(message_value)

#             # ---- print AFTER processing (extracted fields)
#             print("\n<<<< AFTER PROCESSING (extracted) >>>>")
#             try:
#                 print(json.dumps(processed, indent=4, ensure_ascii=False))
#             except Exception:
#                 print(processed)
#             print("===============================================\n")

#             # Optionally attach metadata and forward as before
#             payload = {
#                 "kafka_topic": msg.topic,
#                 "kafka_partition": msg.partition,
#                 "kafka_offset": msg.offset,
#                 "received_at": int(time.time()),
#                 "payload": message_value,
#                 "extracted": processed,
#             }

#             if FORWARD_MODE == "kafka":
#                 forward_to_kafka(producer, payload, key=message_key)
#             else:
#                 forward_to_http(payload)

#             # If not auto-committing, you should commit manually:
#             if not AUTO_COMMIT:
#                 consumer.commit()
#     except KeyboardInterrupt:
#         logger.info("Stopped by user")
#     except Exception as e:
#         logger.exception("Unexpected error in consumer loop: %s", e)
#     finally:
#         logger.info("Closing consumer/producer")
#         try:
#             consumer.close()
#         except Exception:
#             pass
#         if producer:
#             try:
#                 producer.flush()
#                 producer.close()
#             except Exception:
#                 pass


# if __name__ == "__main__":
#     main()


# import os
# import json
# import time
# import logging
# from dotenv import load_dotenv
# from kafka import KafkaConsumer
# from pymongo import MongoClient, errors as pymongo_errors

# # Load .env
# load_dotenv()

# # Configs (Kafka)
# KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka-broker:9092")
# KAFKA_CONSUMER_TOPIC = os.getenv("KAFKA_CONSUMER_TOPIC", "news_topic")
# CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "news-consumer-group")
# POLL_TIMEOUT_MS = int(os.getenv("POLL_TIMEOUT_MS", "1000"))
# AUTO_COMMIT = os.getenv("AUTO_COMMIT", "true").lower() in ("1", "true", "yes")

# # Configs (MongoDB)
# MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
# MONGO_DB = os.getenv("MONGO_DB", "news_db")
# MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "articles")

# # Logging
# logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
# logger = logging.getLogger("news-consumer-mongo")


# def create_consumer():
#     logger.info("Connecting Kafka consumer to broker: %s", KAFKA_BROKER)
#     consumer = KafkaConsumer(
#         KAFKA_CONSUMER_TOPIC,
#         bootstrap_servers=[KAFKA_BROKER],
#         group_id=CONSUMER_GROUP,
#         auto_offset_reset="earliest",
#         enable_auto_commit=AUTO_COMMIT,
#         value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v is not None else None,
#     )
#     return consumer


# def create_mongo_client():
#     logger.info("Connecting to MongoDB: %s", MONGO_URI)
#     client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
#     try:
#         # trigger a server selection to fail fast if connection is bad
#         client.admin.command("ping")
#     except Exception as e:
#         logger.exception("Failed to connect/ping MongoDB: %s", e)
#         raise
#     return client


# def forward_to_mongo(collection, payload):
#     """Insert the payload into the given pymongo collection."""
#     try:
#         result = collection.insert_one(payload)
#         logger.info("Inserted document into MongoDB with _id=%s", result.inserted_id)
#     except pymongo_errors.DuplicateKeyError:
#         # Example: if you later set a unique index and duplicates occur
#         logger.warning("Duplicate document not inserted.")
#     except Exception as e:
#         logger.exception("Failed to insert document into MongoDB: %s", e)


# # ---------------------------
# # Processing helpers & logic
# # (unchanged from your original implementation)
# # ---------------------------
# OUTPUT_KEYS = ["source", "author", "title", "description", "url", "publishedAt", "content"]


# def _safe_load(value):
#     """Parse JSON string values to Python objects when possible."""
#     if isinstance(value, str):
#         try:
#             return json.loads(value)
#         except json.JSONDecodeError:
#             return value
#     return value


# def _get_from_candidates(obj, candidates):
#     """Case-insensitive lookup in dict for first candidate that exists and is non-empty."""
#     if not isinstance(obj, dict):
#         return None
#     lower_map = {k.lower(): k for k in obj.keys()}
#     for cand in candidates:
#         key = cand.lower()
#         if key in lower_map:
#             val = obj[lower_map[key]]
#             if val is not None and val != "":
#                 return val
#     return None


# def _first_available(value, keys):
#     """Given value (dict/list/primitive), try to locate the first existing field from keys."""
#     if value is None:
#         return None
#     # If list, try first element
#     if isinstance(value, list):
#         if not value:
#             return None
#         return _first_available(value[0], keys)
#     # If dict, try keys and some common wrappers
#     if isinstance(value, dict):
#         found = _get_from_candidates(value, keys)
#         if found is not None:
#             return found
#         for wrapper in ("payload", "data", "message", "body", "article", "item"):
#             if wrapper in value and isinstance(value[wrapper], (dict, list)):
#                 res = _first_available(value[wrapper], keys)
#                 if res is not None:
#                     return res
#         return None
#     # Primitive (string/number) fallback
#     if isinstance(value, (str, int, float)):
#         return value
#     return None


# def _extract_source(raw):
#     """Normalize source field (dict with name/id or string)."""
#     if raw is None:
#         return None
#     if isinstance(raw, str):
#         return raw.strip() or None
#     if isinstance(raw, dict):
#         name = _get_from_candidates(raw, ["name", "title", "id"])
#         if name:
#             return name.strip() if isinstance(name, str) else str(name)
#         try:
#             return json.dumps(raw, ensure_ascii=False)
#         except Exception:
#             return str(raw)
#     return str(raw)


# def _extract_published(raw):
#     """Return published value as string (don't try to reformat)."""
#     if raw is None:
#         return None
#     if isinstance(raw, str):
#         return raw.strip() or None
#     if isinstance(raw, (int, float)):
#         return str(raw)
#     return None


# def process_message(message_value):
#     """
#     Accepts a Kafka message value (dict or JSON string) and extracts:
#     source, author, title, description, url, publishedAt, content
#     Returns a dict with those keys (values may be None).
#     """
#     msg = _safe_load(message_value)

#     # unwrap common top-level wrappers
#     if isinstance(msg, dict):
#         for wrapper in ("payload", "data", "message", "body", "article"):
#             if wrapper in msg and isinstance(msg[wrapper], (dict, list)):
#                 msg = msg[wrapper]
#                 break

#     # if list, pick the first article
#     if isinstance(msg, list):
#         msg = msg[0] if msg else {}

#     # if after all this it's not a dict, return empty result
#     if not isinstance(msg, dict):
#         return {k: None for k in OUTPUT_KEYS}

#     # candidate keys
#     source_candidates = ["source", "source_name", "sourceName", "publisher"]
#     author_candidates = ["author", "byline", "creator", "writer"]
#     title_candidates = ["title", "headline", "heading"]
#     description_candidates = ["description", "summary", "abstract"]
#     url_candidates = ["url", "link", "articleUrl", "sourceUrl", "web_url"]
#     published_candidates = ["publishedAt", "published_at", "pubDate", "published", "date"]
#     content_candidates = ["content", "fullText", "article_content", "body", "text"]

#     raw_source = _first_available(msg, source_candidates)
#     raw_author = _first_available(msg, author_candidates)
#     raw_title = _first_available(msg, title_candidates)
#     raw_description = _first_available(msg, description_candidates)
#     raw_url = _first_available(msg, url_candidates)
#     raw_published = _first_available(msg, published_candidates)
#     raw_content = _first_available(msg, content_candidates)

#     # If msg has nested source object like {"source": {"name": "..."}}
#     if "source" in msg and isinstance(msg["source"], dict) and not raw_source:
#         raw_source = _get_from_candidates(msg["source"], ["name", "id", "title"])

#     result = {
#         "source": _extract_source(raw_source),
#         "author": (raw_author.strip() if isinstance(raw_author, str) and raw_author.strip() else None),
#         "title": (raw_title.strip() if isinstance(raw_title, str) and raw_title.strip() else None),
#         "description": (raw_description.strip() if isinstance(raw_description, str) and raw_description.strip() else None),
#         "url": (raw_url.strip() if isinstance(raw_url, str) and raw_url.strip() else None),
#         "publishedAt": _extract_published(raw_published),
#         "content": (raw_content.strip() if isinstance(raw_content, str) and raw_content.strip() else None),
#     }

#     return result


# # ---------------------------
# # End processing helpers
# # ---------------------------


# def main():
#     logger.info("Starting Kafka consumer (forwarding to MongoDB)")
#     consumer = create_consumer()

#     # create MongoDB client/collection
#     try:
#         mongo_client = create_mongo_client()
#     except Exception:
#         logger.error("Cannot continue without MongoDB connection. Exiting.")
#         return

#     db = mongo_client[MONGO_DB]
#     collection = db[MONGO_COLLECTION]

#     try:
#         for msg in consumer:
#             # msg.value is already deserialized to Python object (per your deserializer)
#             message_value = msg.value
#             message_key = msg.key.decode("utf-8") if msg.key else None

#             logger.info("Consumed message offset=%s partition=%s key=%s",
#                         msg.offset, msg.partition, message_key)

#             # ---- print BEFORE processing (raw)
#             print("\n>>>> BEFORE PROCESSING (raw message) <<<<")
#             print("Message Key:", message_key)
#             print("Message Type:", type(message_value))
#             print("Message Value (Pretty JSON):")
#             try:
#                 print(json.dumps(message_value, indent=4, ensure_ascii=False))
#             except Exception:
#                 print(message_value)
#             print("===============================================\n")

#             # ---- process message to extract required fields
#             processed = process_message(message_value)

#             # ---- print AFTER processing (extracted fields)
#             print("\n<<<< AFTER PROCESSING (extracted) >>>>")
#             try:
#                 print(json.dumps(processed, indent=4, ensure_ascii=False))
#             except Exception:
#                 print(processed)
#             print("===============================================\n")

#             # Build the payload to store in MongoDB
#             payload = {
#                 "kafka_topic": msg.topic,
#                 "kafka_partition": msg.partition,
#                 "kafka_offset": msg.offset,
#                 "kafka_key": message_key,
#                 "received_at": int(time.time()),
#                 "payload": message_value,
#                 "extracted": processed,
#             }

#             # Forward to MongoDB
#             forward_to_mongo(collection, payload)

#             # If not auto-committing, you should commit manually:
#             if not AUTO_COMMIT:
#                 consumer.commit()
#     except KeyboardInterrupt:
#         logger.info("Stopped by user")
#     except Exception as e:
#         logger.exception("Unexpected error in consumer loop: %s", e)
#     finally:
#         logger.info("Closing consumer and MongoDB client")
#         try:
#             consumer.close()
#         except Exception:
#             pass
#         try:
#             mongo_client.close()
#         except Exception:
#             pass


# if __name__ == "__main__":
#     main()


# consumer.py
import os
import json
import time
import logging
import signal
from dotenv import load_dotenv
from kafka import KafkaConsumer

# local modules
from process import extract as extract_fields
from consumer_db import MongoStore, create_mongo_client

load_dotenv()

# Kafka config
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka-broker:9092")
KAFKA_CONSUMER_TOPIC = os.getenv("KAFKA_CONSUMER_TOPIC", "news_topic")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "news-consumer-group")
AUTO_COMMIT = os.getenv("AUTO_COMMIT", "true").lower() in ("1", "true", "yes")

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("news-consumer")

shutdown_requested = False

def handle_sigterm(signum, frame):
    global shutdown_requested
    shutdown_requested = True
    logger.info("Shutdown requested (signal %s)", signum)

signal.signal(signal.SIGINT, handle_sigterm)
signal.signal(signal.SIGTERM, handle_sigterm)


def create_consumer():
    logger.info("Connecting Kafka consumer to broker: %s", KAFKA_BROKER)
    consumer = KafkaConsumer(
        KAFKA_CONSUMER_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=AUTO_COMMIT,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v is not None else None,
    )
    return consumer


def main():
    logger.info("Starting consumer -> process -> consumerdb pipeline")
    consumer = create_consumer()

    # create Mongo store
    try:
        mongo_client = create_mongo_client()
        store = MongoStore(mongo_client)
    except Exception as e:
        logger.exception("Could not connect to MongoDB. Exiting.")
        return

    try:
        for msg in consumer:
            if shutdown_requested:
                logger.info("Shutdown detected, breaking consumption loop")
                break

            message_value = msg.value
            message_key = msg.key.decode("utf-8") if msg.key else None

            logger.info("Consumed message offset=%s partition=%s key=%s",
                        msg.offset, msg.partition, message_key)

            # debug prints (optional)
            print("\n>>>> BEFORE PROCESSING (raw message) <<<<")
            print("Message Key:", message_key)
            print("Message Type:", type(message_value))
            try:
                print(json.dumps(message_value, indent=4, ensure_ascii=False))
            except Exception:
                print(message_value)
            print("===============================================\n")

            # process
            extracted = extract_fields(message_value)

            print("\n<<<< AFTER PROCESSING (extracted) >>>>")
            try:
                print(json.dumps(extracted, indent=4, ensure_ascii=False))
            except Exception:
                print(extracted)
            print("===============================================\n")

            payload = {
                "kafka_topic": msg.topic,
                "kafka_partition": msg.partition,
                "kafka_offset": msg.offset,
                "kafka_key": message_key,
                "received_at": int(time.time()),
                "payload": message_value,
                "extracted": extracted,
            }

            # store
            try:
                store.insert(payload)
            except Exception:
                logger.exception("Failed to store payload in MongoDB")

            # manual commit if needed
            if not AUTO_COMMIT:
                consumer.commit()

            # continue to next message
            if shutdown_requested:
                break

    except Exception as e:
        logger.exception("Unexpected error in consumer loop: %s", e)
    finally:
        logger.info("Closing consumer and MongoDB client")
        try:
            consumer.close()
        except Exception:
            pass
        try:
            store.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
