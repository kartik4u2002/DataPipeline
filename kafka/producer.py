import json
import os
import time
from dotenv import load_dotenv
from kafka import KafkaProducer
from API.fetch_api_data import fetch_everything

# Load environment variables
load_dotenv()

# Read config from .env
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "news_topic")

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8") if k else None
)

def produce_news():
    print(f"📡 Connecting to Kafka broker at {KAFKA_BROKER}...")
    print(f"📰 Producing news messages to topic '{KAFKA_TOPIC}'")

    # Fetch AI-related news articles
    result = fetch_everything(q="AI", language="en", sort_by="publishedAt", page_size=10)

    if not result.get("ok"):
        print("❌ Failed to fetch news from API:", result.get("error"))
        return

    articles = result["result"].get("articles", [])
    print(f"✅ Retrieved {len(articles)} articles. Sending to Kafka...")

    for i, article in enumerate(articles, 1):
        print(article)
        # Add an ID for tracking
        article["id"] = i

        # Send each article to Kafka topic
        producer.send(KAFKA_TOPIC, key=f"article_{i}", value=article)
        print(f"📤 Sent article {i}: {article.get('title', 'No Title')}")

    # Ensure all messages are delivered
    producer.flush()
    print("🚀 All messages sent successfully!")

if __name__ == "__main__":
    while True:
        produce_news()
        print("⏳ Sleeping for 60 seconds before next fetch...")
        time.sleep(60)
