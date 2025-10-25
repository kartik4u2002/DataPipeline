import json
from fetch_api_data import fetch_everything

def main():
    """
    Fetch all news related to 'AI' using NewsAPI and save to ai_news.json
    """
    print("Fetching AI-related news articles...")

    # Fetch everything related to "AI"
    result = fetch_everything(q="AI", language="en", sort_by="publishedAt", page_size=20)

    # Check if request was successful
    if not result.get("ok", False):
        print("❌ Failed to fetch news.")
        print(result)
        return

    # Extract articles
    articles = result["result"].get("articles", [])

    # Save to ai_news.json
    output_filename = "ai_news.json"
    with open(output_filename, "w", encoding="utf-8") as f:
        json.dump(articles, f, indent=4, ensure_ascii=False)

    print(f"✅ Successfully fetched {len(articles)} AI news articles.")
    print(f"🗂️  Saved to {output_filename}")


if __name__ == "__main__":
    main()
