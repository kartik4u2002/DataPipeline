import os
import streamlit as st
from dotenv import load_dotenv
from pymongo import MongoClient
from datetime import datetime, timedelta, timezone
import random

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "news_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "articles")

@st.cache_resource(show_spinner=False)
def get_db():
    """Connect to MongoDB"""
    client = MongoClient(MONGO_URI)
    return client[MONGO_DB]

def fetch_today_mcqs(db, collection_name, min_mcqs=1):
    """
    Return articles with MCQs where payload.publishedAt falls on 'yesterday'
    in Asia/Kolkata, handling both string and BSON Date types.
    """
    ist = timezone(timedelta(hours=5, minutes=30))
     # YESTERDAY in IST
    yesterday_date = (datetime.now(ist).date() - timedelta(days=1))
    yesterday_str = yesterday_date.isoformat()  # 'YYYY-MM-DD'
    quiz_date = (datetime.now(ist) - timedelta(days=1)).strftime('%B %d, %Y (%A)')
    st.title("🧠 News MCQ Quiz")
    st.markdown(f"**Quiz Date (IST):** {quiz_date} — showing articles from yesterday")


    pipeline = [
        {
            "$addFields": {
                "pub": {
                    "$cond": [
                        {"$eq": [{"$type": "$payload.publishedAt"}, "date"]},
                        "$payload.publishedAt",
                        {"$toDate": "$payload.publishedAt"}
                    ]
                }
            }
        },
        {
            "$addFields": {
                "pubLocalDateStr": {
                    "$dateToString": {
                        "date": "$pub",
                        "format": "%Y-%m-%d",
                        "timezone": "+05:30"  # Asia/Kolkata
                    }
                }
            }
        },
        {
            "$match": {
                "pubLocalDateStr": yesterday_str,
                "extracted.mcq_count": {"$gte": min_mcqs}
            }
        },
        {"$sort": {"pub": -1}},
        {
            "$project": {
                "payload.title": 1,
                "payload.description": 1,
                "payload.content": 1,
                "payload.publishedAt": 1,
                "payload.url": 1,
                "payload.source": 1,
                "extracted.mcqs": 1,
                "extracted.mcq_count": 1
            }
        }
    ]

    try:
        return list(db[collection_name].aggregate(pipeline, allowDiskUse=True))
    except Exception:
        # Fallback: compare on UTC strings if server lacks $toDate/$dateToString
        start_ist = datetime.combine(yesterday_date, datetime.min.time(), ist)
        end_ist = datetime.combine(yesterday_date, datetime.max.time().replace(microsecond=999000), ist)

        start_utc = start_ist.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        end_utc = end_ist.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        filt = {
            "extracted.mcq_count": {"$gte": min_mcqs},
            "payload.publishedAt": {"$gte": start_utc, "$lte": end_utc},
        }

        return list(db[collection_name].find(
            filt,
            {
                "payload.title": 1,
                "payload.description": 1,
                "payload.content": 1,
                "payload.publishedAt": 1,
                "payload.url": 1,
                "payload.source": 1,
                "extracted.mcqs": 1,
                "extracted.mcq_count": 1
            }
        ).sort([("payload.publishedAt", -1)]))

def collect_all_mcqs(docs):
    """Extract all MCQs from documents"""
    all_mcqs = []
    for doc in docs:
        payload = doc.get("payload", {}) or {}
        raw_mcqs = (doc.get("extracted", {}) or {}).get("mcqs", [])

        for mcq in raw_mcqs:
            if not isinstance(mcq, dict):
                continue
            if not mcq.get('question') or not mcq.get('answer') or not mcq.get('options'):
                continue
            if len(mcq.get('options', [])) < 2:
                continue

            # Add article metadata
            mcq['article_title'] = payload.get('title', 'Untitled')
            mcq['article_url'] = payload.get('url', '')
            mcq['article_source'] = (payload.get('source', {}) or {}).get('name', 'Unknown')
            mcq['article_date'] = payload.get('publishedAt', '')[:10] if payload.get('publishedAt') else 'Unknown'
            mcq['article_id'] = str(doc.get('_id'))

            all_mcqs.append(mcq)
    return all_mcqs

def reset_quiz():
    """Reset quiz state"""
    st.session_state.idx = 0
    st.session_state.score = 0
    st.session_state.answers = {}
    st.session_state.show_results = False

# ==================== PAGE CONFIG ====================
st.set_page_config(page_title="News MCQ Quiz", page_icon="🧠", layout="wide")

# Custom CSS
st.markdown("""
    <style>
    .quiz-header {
        background: linear-gradient(90deg, #1f77b4 0%, #2ca02c 100%);
        padding: 15px 20px;
        border-radius: 10px;
        color: white;
        margin-bottom: 20px;
        text-align: center;
    }
    .article-badge {
        background-color: #e7f3ff;
        padding: 8px 15px;
        border-radius: 5px;
        font-size: 14px;
        color: #0066cc;
        display: inline-block;
        margin: 10px 5px;
    }
    .stRadio > label {
        font-size: 16px;
        font-weight: 500;
    }
    </style>
""", unsafe_allow_html=True)

# ==================== HEADER ====================
ist = timezone(timedelta(hours=5, minutes=30))
st.title("🧠 News MCQ Quiz")
st.markdown(f"**Today's Date (IST):** {datetime.now(ist).strftime('%B %d, %Y (%A)')}")

# ==================== SIDEBAR ====================
with st.sidebar:
    st.header("⚙️ Settings")
    min_mcqs = st.slider("Minimum MCQs per article", 1, 10, 1)

    st.markdown("---")
    st.header("📊 Stats")
    if "score" in st.session_state and "answers" in st.session_state:
        total_answered = len(st.session_state.answers)
        if total_answered > 0:
            percentage = (st.session_state.score / total_answered) * 100
            st.metric("Score", f"{st.session_state.score}/{total_answered}")
            st.metric("Accuracy", f"{percentage:.1f}%")

    st.markdown("---")
    if st.button("🔄 Reset Quiz", use_container_width=True):
        reset_quiz()
        st.rerun()

# ==================== FETCH DATA ====================
db = get_db()

with st.spinner("Loading today’s MCQs..."):
    docs = fetch_today_mcqs(db, MONGO_COLLECTION, min_mcqs)

# Optional debug peek
with st.expander("🧪 Debug (today filter)"):
    st.write("Matched docs:", len(docs))
    if docs:
        d0 = docs[0]
        st.json({
            "title": d0.get("payload", {}).get("title"),
            "source": (d0.get("payload", {}).get("source") or {}).get("name"),
            "publishedAt": d0.get("payload", {}).get("publishedAt"),
        })

if not docs:
    st.error("❌ No articles with MCQs found for **yesterday (Asia/Kolkata)**!")
    st.info("Check if your producer/consumer ingested any articles yesterday.")

    # Show sample doc for quick sanity
    with st.expander("🔧 Debug: Check Database"):
        try:
            sample = list(db[MONGO_COLLECTION].find().limit(1))
            if sample:
                st.json(sample[0])
            else:
                st.warning("Database collection is empty.")
        except Exception as e:
            st.error(f"Error: {e}")
    st.stop()

# ==================== COLLECT MCQs ====================
all_mcqs = collect_all_mcqs(docs)

if not all_mcqs:
    st.warning("⚠️ Found today's articles but no valid MCQs!")
    st.info(f"Found {len(docs)} articles, but couldn't extract valid MCQs from them.")
    st.stop()

# ==================== INFO BANNER ====================
st.caption("💡 **Tip:** This quiz shows MCQs only from articles published yesterday (Asia/Kolkata).")


# ==================== SESSION INITIALIZATION ====================
if "mcqs" not in st.session_state or "db_key" not in st.session_state or st.session_state.db_key != f"{MONGO_COLLECTION}_TODAY_{min_mcqs}":
    st.session_state.db_key = f"{MONGO_COLLECTION}_TODAY_{min_mcqs}"
    st.session_state.mcqs = all_mcqs.copy()
    random.shuffle(st.session_state.mcqs)
    reset_quiz()

mcqs = st.session_state.mcqs

# ==================== QUIZ CONTROMS ====================
col1, col2 = st.columns([3, 1])
with col1:
    max_questions = st.slider(
        "How many questions?",
        min_value=5,
        max_value=min(len(mcqs), 50),
        value=min(10, len(mcqs))
    )
with col2:
    if st.button("🎲 Shuffle", use_container_width=True):
        random.shuffle(st.session_state.mcqs)
        reset_quiz()
        st.rerun()

mcqs = mcqs[:max_questions]

# ==================== SESSION STATE ====================
if "idx" not in st.session_state:
    st.session_state.idx = 0
if "score" not in st.session_state:
    st.session_state.score = 0
if "answers" not in st.session_state:
    st.session_state.answers = {}
if "show_results" not in st.session_state:
    st.session_state.show_results = False

st.markdown("---")

# ==================== RESULTS SCREEN ====================
total_questions = len(mcqs)

if st.session_state.show_results:
    st.balloons()
    st.markdown('<div class="quiz-header"><h2>🎉 Quiz Complete!</h2></div>', unsafe_allow_html=True)

    score = st.session_state.score
    total = len(st.session_state.answers)
    percentage = (score / total) * 100 if total > 0 else 0

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Final Score", f"{score}/{total}")
    with col2:
        st.metric("Accuracy", f"{percentage:.1f}%")
    with col3:
        if percentage >= 80:
            grade = "🌟 Excellent!"
        elif percentage >= 60:
            grade = "👍 Good!"
        else:
            grade = "📚 Keep Learning!"
        st.metric("Grade", grade)

    col1, col2 = st.columns(2)
    with col1:
        if st.button("🔄 New Quiz", use_container_width=True, type="primary"):
            reset_quiz()
            st.rerun()
    with col2:
        if st.button("📊 Review Answers", use_container_width=True):
            st.session_state.show_review = True
            st.rerun()

    st.markdown("---")

    if st.session_state.get("show_review", False):
        st.markdown("### 📝 Answer Review")
        for j, mj in enumerate(mcqs):
            user_answer = st.session_state.answers.get(j)
            is_correct = user_answer == mj['answer']
            with st.expander(f"{'✅' if is_correct else '❌'} Question {j+1}: {mj['question'][:80]}..."):
                st.markdown(f"**Question:** {mj['question']}")
                st.markdown("**Options:**")
                for opt in mj['options']:
                    if opt == mj['answer']:
                        st.success(f"✅ {opt} (Correct)")
                    elif opt == user_answer:
                        st.error(f"❌ {opt} (Your answer)")
                    else:
                        st.info(f"⚪ {opt}")
                st.markdown(f"""
                <div class="article-badge">
                    📰 {mj['article_source']} | 📅 {mj['article_date']} | 📝 {mj['article_title'][:50]}
                </div>
                """, unsafe_allow_html=True)
                if mj.get('article_url'):
                    st.markdown(f"[🔗 Read full article]({mj['article_url']})")
    st.stop()

# ==================== QUIZ INTERFACE ====================
i = max(0, min(st.session_state.idx, total_questions - 1))
q = mcqs[i]

# Progress
progress = (i + 1) / total_questions
st.progress(progress, text=f"Question {i+1} of {total_questions}")

# Question header
st.markdown(f'<div class="quiz-header"><h3>Question {i+1}</h3></div>', unsafe_allow_html=True)

# Article info
st.markdown(f"""
<div class="article-badge">
    📰 {q['article_source']} | 📅 {q['article_date']}
</div>
<div class="article-badge">
    📝 {q['article_title'][:80]}{'...' if len(q['article_title']) > 80 else ''}
</div>
""", unsafe_allow_html=True)

# Question
st.markdown(f"### {q['question']}")

# Check if already answered
already_answered = i in st.session_state.answers

if already_answered:
    given = st.session_state.answers[i]
    st.markdown("**Your answer:**")
    for option in q["options"]:
        if option == q["answer"]:
            st.success(f"✅ {option} — Correct Answer")
        elif option == given:
            st.error(f"❌ {option} — Your Answer")
        else:
            st.info(f"⚪ {option}")

    if given == q["answer"]:
        st.success("🎉 **Correct!** Well done!")
    else:
        st.error(f"❌ **Incorrect.** The correct answer is: **{q['answer']}**")

    if q.get('source_sentence'):
        with st.expander("💡 Context from article"):
            st.info(q['source_sentence'])
else:
    selected = st.radio(
        "Select your answer:",
        q["options"],
        key=f"q_{i}",
        label_visibility="collapsed"
    )

# ==================== NAVIGATION ====================
st.markdown("---")

col1, col2, col3, col4 = st.columns([1, 1, 1, 1])

with col1:
    if not already_answered:
        if st.button("✅ Submit", use_container_width=True, type="primary"):
            st.session_state.answers[i] = selected
            if selected == q["answer"]:
                st.session_state.score += 1
            st.rerun()
    else:
        st.button("✅ Submitted", disabled=True, use_container_width=True)

with col2:
    if i > 0:
        if st.button("⬅️ Previous", use_container_width=True):
            st.session_state.idx = i - 1
            st.rerun()

with col3:
    if i < total_questions - 1:
        if st.button("Next ➡️", use_container_width=True):
            st.session_state.idx = i + 1
            st.rerun()

with col4:
    if i == total_questions - 1:
        if st.button("🏁 Finish", use_container_width=True, type="primary"):
            st.session_state.show_results = True
            st.rerun()

# ==================== CURRENT STATS ====================
st.markdown("---")

col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Answered", f"{len(st.session_state.answers)}/{total_questions}")
with col2:
    st.metric("Score", f"{st.session_state.score}")
with col3:
    if len(st.session_state.answers) > 0:
        accuracy = (st.session_state.score / len(st.session_state.answers)) * 100
        st.metric("Accuracy", f"{accuracy:.1f}%")

# ==================== QUICK NAVIGATION ====================
with st.expander("🗺️ Jump to Question"):
    cols = st.columns(10)
    for j in range(total_questions):
        col_idx = j % 10
        with cols[col_idx]:
            if j in st.session_state.answers:
                is_correct = st.session_state.answers[j] == mcqs[j]["answer"]
                label = "✅" if is_correct else "❌"
            else:
                label = "⚪"
            if st.button(f"{label} {j+1}", key=f"nav_{j}", use_container_width=True):
                st.session_state.idx = j
                st.rerun()

# ==================== FOOTER ====================
st.markdown("---")
st.caption("💡 **Tip:** This quiz shows MCQs only from articles published today (Asia/Kolkata).")
