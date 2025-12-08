# kafka/enrich_mcqs.py
import os, sys
from dotenv import load_dotenv
from pymongo import MongoClient
from nlp_mcq import generate_mcqs  # your existing function

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB", "news_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "articles")

def build_paragraph_from_doc(doc: dict) -> str:
    # prefer a stored paragraph if you have one; else assemble from payload
    payload = doc.get("payload", {})
    parts = [
        doc.get("paragraph", ""),                      # optional pre-saved field
        payload.get("title", ""), 
        payload.get("description", ""), 
        payload.get("content", "")
    ]
    para = ". ".join(p.strip() for p in parts if p).strip()
    return para

def main(limit=100):
    if not MONGO_URI:
        print("Missing MONGO_URI in environment."); sys.exit(1)

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    col = db[MONGO_COLLECTION]

    # pick documents that don't have MCQs yet (adjust selector for your schema)
    cursor = col.find(
        {"$or": [{"extracted.mcqs": {"$exists": False}}, {"extracted.mcq_count": {"$lt": 1}}]},
        {"payload": 1, "paragraph": 1}
    ).limit(limit)

    updated = 0
    for doc in cursor:
        para = build_paragraph_from_doc(doc)
        if not para:
            continue

        mcqs = generate_mcqs(para, max_questions=10)
        col.update_one(
            {"_id": doc["_id"]},
            {"$set": {
                "extracted.paragraph_source": "from_db_batch",
                "extracted.mcqs": mcqs,
                "extracted.mcq_count": len(mcqs)
            }}
        )
        updated += 1

    print(f"MCQ enrichment complete. Updated {updated} documents.")
    client.close()

if __name__ == "__main__":
    main()
