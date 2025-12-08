import random
import re
from typing import List, Tuple, Dict, Any, Set
from bs4 import BeautifulSoup
import spacy
from nltk.corpus import wordnet as wn
import nltk
from collections import defaultdict

# Ensure NLTK data is available
try:
    wn.synsets("test")
except LookupError:
    nltk.download("wordnet")
    nltk.download("omw-1.4")

# Load spaCy
nlp = spacy.load("en_core_web_sm")

# Regex patterns
URL_RE = re.compile(r'https?://\S+|www\.\S+', re.IGNORECASE)
JS_JUNK_RE = re.compile(
    r"(onclick\s*=\s*\"[^\"]*\"|target\s*=\s*\"?_blank\"?|return\s+false;?|window\.open\([^)]*\))",
    re.IGNORECASE,
)
READMORE_RE = re.compile(r"\[\+\d+\s+chars\]|\b(Read\s+more|Continue\s+reading)\b.*$", re.IGNORECASE)
EMAIL_RE = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')

def sanitize_paragraph(raw: str) -> str:
    """Enhanced sanitization for News API content."""
    if not raw:
        return ""
    
    # Strip HTML tags
    text = BeautifulSoup(raw, "html.parser").get_text(" ", strip=True)
    
    # Remove URLs
    text = URL_RE.sub("", text)
    
    # Remove emails
    text = EMAIL_RE.sub("", text)
    
    # Remove JS/link artifacts
    text = JS_JUNK_RE.sub("", text)
    
    # Remove "read more [+123 chars]" and similar truncation indicators
    text = READMORE_RE.sub("", text)
    
    # Remove common News API artifacts
    text = re.sub(r'\[Removed\]', '', text)
    text = re.sub(r'\(Photo:.*?\)', '', text)
    text = re.sub(r'\(Image:.*?\)', '', text)
    
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    
    # Remove trailing ellipsis and incomplete sentences
    text = re.sub(r'\s*\.\.\.$', '', text)
    
    return text

# --------------------
# Utilities
# --------------------
def split_sentences(paragraph: str) -> List[str]:
    """Split paragraph into sentences using spaCy."""
    doc = nlp(paragraph)
    return [sent.text.strip() for sent in doc.sents if len(sent.text.strip()) > 10]


def clean_text(s: str) -> str:
    """Clean and normalize text."""
    return re.sub(r"\s+", " ", s).strip()


def is_valid_answer(answer: str) -> bool:
    """Check if an answer is valid for MCQ generation."""
    if not answer or len(answer) < 2:
        return False
    if len(answer) > 100:  # Too long for MCQ
        return False
    if answer.lower() in ['the', 'a', 'an', 'this', 'that', 'these', 'those']:
        return False
    # Check if it's mostly punctuation
    if len(re.sub(r'[^\w\s]', '', answer)) < 2:
        return False
    return True


# --------------------
# Enhanced Candidate Extraction
# --------------------
def extract_candidates(paragraph: str) -> List[Dict[str, Any]]:
    """
    Extract candidate answers with improved filtering and scoring.
    Returns list of dicts with: answer_text, label, sentence, start, end, score
    """
    doc = nlp(paragraph)
    sentences = list(doc.sents)
    candidates = []
    seen = set()

    # Priority scoring for different entity types
    label_scores = {
        'PERSON': 10, 'ORG': 9, 'GPE': 8, 'LOC': 7,
        'DATE': 6, 'EVENT': 8, 'PRODUCT': 7, 'WORK_OF_ART': 7,
        'MONEY': 5, 'PERCENT': 5, 'CARDINAL': 4, 'ORDINAL': 4,
        'QUANTITY': 4, 'TIME': 5
    }

    # Extract named entities
    for ent in doc.ents:
        if not is_valid_answer(ent.text):
            continue
        
        key = ent.text.lower().strip()
        if key in seen:
            continue
        
        sent_text = ent.sent.text.strip()
        score = label_scores.get(ent.label_, 3)
        
        # Boost score if entity appears multiple times (likely important)
        count = paragraph.lower().count(key)
        if count > 1:
            score += min(count, 3)
        
        candidates.append({
            "answer_text": ent.text.strip(),
            "label": ent.label_,
            "sentence": sent_text,
            "start": ent.start_char,
            "end": ent.end_char,
            "score": score
        })
        seen.add(key)

    # Add high-quality noun chunks if needed
    if len(candidates) < 8:
        for sent in sentences:
            for chunk in sent.noun_chunks:
                if not is_valid_answer(chunk.text):
                    continue
                if chunk.root.pos_ == "PRON":
                    continue
                
                key = chunk.text.lower().strip()
                if key in seen:
                    continue
                
                # Skip if it's too generic
                if chunk.root.text.lower() in ['thing', 'way', 'time', 'year', 'day']:
                    continue
                
                candidates.append({
                    "answer_text": chunk.text.strip(),
                    "label": "NOUN_CHUNK",
                    "sentence": sent.text.strip(),
                    "start": chunk.start_char,
                    "end": chunk.end_char,
                    "score": 3
                })
                seen.add(key)

    # Sort by score
    candidates.sort(key=lambda x: x['score'], reverse=True)
    
    return candidates


# --------------------
# Enhanced Distractor Generation
# --------------------
def get_contextual_distractors(answer: str, label: str, all_entities: List[Dict], max_d: int = 3) -> List[str]:
    """Generate distractors from entities of the same type in the context."""
    distractors = []
    answer_lower = answer.lower().strip()
    
    # Get entities of the same label
    same_type = [e['answer_text'] for e in all_entities 
                 if e['label'] == label and e['answer_text'].lower().strip() != answer_lower]
    
    # Shuffle and take some
    random.shuffle(same_type)
    for candidate in same_type[:max_d]:
        if candidate not in distractors:
            distractors.append(candidate)
    
    return distractors


def get_wordnet_distractors(word: str, max_d: int = 3) -> List[str]:
    """Enhanced WordNet-based distractor generation."""
    distractors = set()
    word_lower = word.lower().strip()
    
    # Get the first word if it's a phrase
    main_word = word.split()[0] if ' ' in word else word
    
    syns = wn.synsets(main_word.lower())
    
    for syn in syns[:3]:  # Limit synsets to avoid too much variation
        # Synonyms from the same synset
        for lemma in syn.lemmas()[:2]:
            name = lemma.name().replace("_", " ").title()
            if name.lower() != word_lower and name.isalpha():
                distractors.add(name)
        
        # Hyponyms (more specific terms)
        for hypo in syn.hyponyms()[:2]:
            for lemma in hypo.lemmas()[:1]:
                name = lemma.name().replace("_", " ").title()
                if name.lower() != word_lower and name.isalpha():
                    distractors.add(name)
        
        if len(distractors) >= max_d * 2:
            break
    
    result = list(distractors)[:max_d]
    return result


def numeric_distractors(answer: str, max_d: int = 3) -> List[str]:
    """Generate plausible numeric distractors."""
    distractors = set()
    
    # Extract number
    nums = re.findall(r"[\d,.]+", answer)
    if not nums:
        return []
    
    try:
        base = float(nums[0].replace(",", ""))
    except ValueError:
        return []
    
    # Determine if it's percentage, money, or regular number
    is_percent = '%' in answer
    is_money = any(sym in answer for sym in ['$', '€', '£', '¥', 'USD', 'EUR'])
    is_integer = float(base).is_integer()
    
    # Generate variations
    variations = []
    if is_percent:
        # For percentages, use smaller variations
        variations = [base * 0.7, base * 0.85, base * 1.15, base * 1.3, base * 1.5]
    elif base < 10:
        # Small numbers - use addition/subtraction
        variations = [base - 2, base - 1, base + 1, base + 2, base + 3]
    elif base < 100:
        variations = [base * 0.5, base * 0.8, base * 1.2, base * 1.5, base * 2]
    else:
        # Large numbers
        variations = [base * 0.6, base * 0.8, base * 1.25, base * 1.5, base * 2.5]
    
    for var in variations:
        if var <= 0:
            continue
        
        # Format the distractor
        if is_percent:
            formatted = f"{round(var, 1)}%"
        elif is_money:
            if '$' in answer:
                formatted = f"${round(var, 2) if not is_integer else int(round(var))}"
            else:
                formatted = str(round(var, 2) if not is_integer else int(round(var)))
        else:
            formatted = str(int(round(var)) if is_integer else round(var, 1))
        
        if formatted != answer:
            distractors.add(formatted)
        
        if len(distractors) >= max_d:
            break
    
    return list(distractors)[:max_d]


def date_distractors(answer: str, max_d: int = 3) -> List[str]:
    """Generate date-based distractors."""
    distractors = set()
    
    # Extract year
    years = re.findall(r'\b(19|20)\d{2}\b', answer)
    if years:
        try:
            base_year = int(years[0])
            variations = [base_year - 2, base_year - 1, base_year + 1, base_year + 2]
            for year in variations:
                if 1900 <= year <= 2030:
                    distractors.add(str(year))
                if len(distractors) >= max_d:
                    break
        except ValueError:
            pass
    
    # Month names
    months = ['January', 'February', 'March', 'April', 'May', 'June',
              'July', 'August', 'September', 'October', 'November', 'December']
    
    for month in months:
        if month in answer:
            other_months = [m for m in months if m != month]
            random.shuffle(other_months)
            for alt_month in other_months[:max_d]:
                distractors.add(answer.replace(month, alt_month))
                if len(distractors) >= max_d:
                    break
            break
    
    return list(distractors)[:max_d]


# Categorized fallback pools by domain
FALLBACK_POOLS = {
    'PERSON': [
        "Dr. Sarah Johnson", "Michael Chen", "Emma Rodriguez", "David Kim",
        "Professor James Wilson", "Dr. Maria Garcia", "Robert Brown"
    ],
    'ORG': [
        "TechCorp Industries", "Global Ventures Inc", "Innovation Labs",
        "Sustainable Solutions", "Digital Dynamics", "Future Systems"
    ],
    'GPE': [
        "Singapore", "Berlin", "Toronto", "Sydney", "Dubai",
        "Amsterdam", "Seoul", "Stockholm"
    ],
    'LOC': [
        "Pacific Ocean", "Mediterranean Sea", "Amazon Basin", "Sahara",
        "Arctic Circle", "Great Barrier Reef"
    ]
}


def make_distractors(answer: str, label: str, all_entities: List[Dict], max_d: int = 3) -> List[str]:
    """
    Enhanced distractor generation with multiple strategies.
    """
    answer_clean = answer.strip()
    answer_lower = answer_clean.lower()
    distractors = []
    
    # Strategy 1: Use contextual entities (same label from paragraph)
    contextual = get_contextual_distractors(answer_clean, label, all_entities, max_d)
    for d in contextual:
        if d.lower() != answer_lower and d not in distractors:
            distractors.append(d)
    
    if len(distractors) >= max_d:
        return distractors[:max_d]
    
    # Strategy 2: Date-specific distractors
    if label in ("DATE", "TIME") or re.search(r'\b(19|20)\d{2}\b', answer_clean):
        date_dist = date_distractors(answer_clean, max_d - len(distractors))
        for d in date_dist:
            if d.lower() != answer_lower and d not in distractors:
                distractors.append(d)
    
    if len(distractors) >= max_d:
        return distractors[:max_d]
    
    # Strategy 3: Numeric distractors
    if label in ("CARDINAL", "PERCENT", "MONEY", "QUANTITY") or re.search(r'\d', answer_clean):
        num_dist = numeric_distractors(answer_clean, max_d - len(distractors))
        for d in num_dist:
            if d != answer_clean and d not in distractors:
                distractors.append(d)
    
    if len(distractors) >= max_d:
        return distractors[:max_d]
    
    # Strategy 4: WordNet for common nouns
    if label in ("NOUN_CHUNK", "PRODUCT", "WORK_OF_ART", "EVENT"):
        wn_dist = get_wordnet_distractors(answer_clean, max_d - len(distractors))
        for d in wn_dist:
            if d.lower() != answer_lower and d not in distractors:
                distractors.append(d)
    
    if len(distractors) >= max_d:
        return distractors[:max_d]
    
    # Strategy 5: Use categorized fallback pools
    if label in FALLBACK_POOLS:
        pool = FALLBACK_POOLS[label].copy()
        random.shuffle(pool)
        for d in pool:
            if d.lower() != answer_lower and d not in distractors:
                distractors.append(d)
                if len(distractors) >= max_d:
                    break
    
    # Strategy 6: Final fallback - generic options
    generic_fallbacks = ["None of the above", "Information not provided", "Not mentioned in the article"]
    for d in generic_fallbacks:
        if len(distractors) >= max_d:
            break
        if d not in distractors:
            distractors.append(d)
    
    return distractors[:max_d]


# --------------------
# Enhanced Question Generation
# --------------------
QUESTION_TEMPLATES = {
    'PERSON': [
        "Who {verb}?",
        "According to the article, who {verb}?",
        "Which person {verb}?"
    ],
    'ORG': [
        "Which organization {verb}?",
        "What company {verb}?",
        "Which institution {verb}?"
    ],
    'GPE': [
        "Which country/city {verb}?",
        "Where {verb}?",
        "In which location {verb}?"
    ],
    'LOC': [
        "Where {verb}?",
        "Which location {verb}?",
        "What place {verb}?"
    ],
    'DATE': [
        "When {verb}?",
        "In what year {verb}?",
        "What date {verb}?"
    ],
    'CARDINAL': [
        "How many {noun} {verb}?",
        "What number {verb}?"
    ],
    'PERCENT': [
        "What percentage {verb}?",
        "How much (in percentage) {verb}?"
    ],
    'MONEY': [
        "How much money {verb}?",
        "What amount {verb}?"
    ]
}


def build_question_from_sentence(sentence: str, answer: str, label: str) -> Tuple[str, str]:
    """
    Enhanced question generation with better grammar and multiple strategies.
    """
    s = sentence.strip()
    answer_clean = answer.strip()
    
    # Strategy 1: Try to create fill-in-the-blank style question
    pattern = re.compile(r'\b' + re.escape(answer_clean) + r'\b', flags=re.IGNORECASE)
    match = pattern.search(s)
    
    if match:
        # Replace answer with blank
        question = pattern.sub("______", s, count=1)
        
        # Convert to question format based on label
        if label in ("PERSON",):
            if not question.lower().startswith(("who", "which person")):
                question = "Who " + question[0].lower() + question[1:]
        elif label in ("DATE", "TIME"):
            if not question.lower().startswith(("when", "what year", "in what")):
                question = "When " + question[0].lower() + question[1:]
        elif label in ("GPE", "LOC"):
            if not question.lower().startswith(("where", "in which", "which")):
                question = "Where " + question[0].lower() + question[1:]
        elif label in ("CARDINAL", "PERCENT", "MONEY", "QUANTITY"):
            if not question.lower().startswith(("how", "what")):
                question = "How many/much " + question[0].lower() + question[1:]
        elif label in ("ORG",):
            if not question.lower().startswith(("which", "what")):
                question = "Which organization " + question[0].lower() + question[1:]
        
        # Ensure it ends with ?
        question = question.rstrip('.!') + "?"
        
        # Clean up double question marks
        question = question.replace("??", "?")
        
        return question, answer_clean
    
    # Strategy 2: Template-based questions (fallback)
    templates = QUESTION_TEMPLATES.get(label, [
        "What is mentioned as ______?",
        "According to the article, what ______?"
    ])
    
    # Try to extract a verb or context from the sentence
    doc = nlp(s)
    verbs = [token.text for token in doc if token.pos_ == "VERB"]
    
    if templates and verbs:
        template = random.choice(templates)
        verb_phrase = verbs[0] if verbs else "is mentioned"
        question = template.replace("{verb}", verb_phrase)
    else:
        # Simple fallback
        if label == "PERSON":
            question = f"Who is {answer_clean} according to the article?"
        elif label in ("DATE", "TIME"):
            question = f"When did this event occur?"
        elif label in ("GPE", "LOC"):
            question = f"Where is {answer_clean} located?"
        elif label in ("PERCENT", "CARDINAL", "MONEY"):
            question = f"What is the mentioned {label.lower()} value?"
        else:
            question = f"What is {answer_clean}?"
    
    return question, answer_clean


def validate_mcq(mcq: Dict[str, Any]) -> bool:
    """Validate that an MCQ is well-formed and non-ambiguous."""
    
    # Check all required fields exist
    required = ['question', 'options', 'answer']
    if not all(k in mcq for k in required):
        return False
    
    # Check we have exactly 4 options
    if len(mcq['options']) != 4:
        return False
    
    # Check answer is in options
    if mcq['answer'] not in mcq['options']:
        return False
    
    # Check no duplicate options
    if len(set(mcq['options'])) != len(mcq['options']):
        return False
    
    # Check question is reasonable length
    if len(mcq['question']) < 10 or len(mcq['question']) > 300:
        return False
    
    # Check question ends with ?
    if not mcq['question'].endswith('?'):
        return False
    
    return True


# --------------------
# Main MCQ Generation
# --------------------
def generate_mcqs(paragraph: str, max_questions: int = 10) -> List[Dict[str, Any]]:
    """
    Enhanced MCQ generation with better quality control.
    """
    # Sanitize input
    paragraph = sanitize_paragraph(paragraph)
    paragraph = clean_text(paragraph)
    
    if not paragraph or len(paragraph) < 50:
        return []
    
    # Extract candidates
    candidates = extract_candidates(paragraph)
    
    if not candidates:
        return []
    
    mcqs = []
    used_answers = set()
    used_sentences = set()
    
    # Try to generate MCQs
    for cand in candidates:
        if len(mcqs) >= max_questions:
            break
        
        answer = cand["answer_text"]
        answer_lower = answer.lower().strip()
        
        # Skip if already used
        if answer_lower in used_answers:
            continue
        
        # Skip if sentence already used (avoid redundancy)
        sent_key = cand["sentence"][:50].lower()
        if sent_key in used_sentences:
            continue
        
        label = cand["label"]
        sentence = cand["sentence"]
        
        # Generate question
        try:
            question_text, correct_answer = build_question_from_sentence(sentence, answer, label)
        except Exception:
            continue
        
        # Generate distractors
        distractors = make_distractors(answer, label, candidates, max_d=3)
        
        # Ensure we have 3 unique distractors
        distractors = [d for d in distractors if d.lower() != correct_answer.lower()]
        distractors = list(dict.fromkeys(distractors))  # Remove duplicates
        
        if len(distractors) < 3:
            continue  # Skip if we can't generate enough distractors
        
        # Build options
        options = distractors[:3] + [correct_answer]
        random.shuffle(options)
        
        # Create MCQ
        mcq = {
            "question": question_text,
            "options": options,
            "answer": correct_answer,
            "source_sentence": sentence,
            "label": label
        }
        
        # Validate
        if validate_mcq(mcq):
            mcqs.append(mcq)
            used_answers.add(answer_lower)
            used_sentences.add(sent_key)
    
    return mcqs


# --------------------
# Example usage
# --------------------
if __name__ == "__main__":
    sample_text = """
    The United Nations Climate Change Conference (COP28) took place in Dubai from November 30 to December 12, 2023.
    Over 70,000 participants attended the conference, including world leaders and climate activists.
    The conference resulted in a historic agreement to transition away from fossil fuels.
    Sultan Al Jaber, the CEO of Abu Dhabi National Oil Company, served as the COP28 president.
    Scientists emphasized that global temperatures have risen by 1.1°C since pre-industrial times.
    """
    
    mcqs = generate_mcqs(sample_text, max_questions=5)
    
    print(f"Generated {len(mcqs)} MCQs:\n")
    for i, mcq in enumerate(mcqs, 1):
        print(f"Q{i}: {mcq['question']}")
        for j, opt in enumerate(mcq['options'], 1):
            marker = "✓" if opt == mcq['answer'] else " "
            print(f"  {j}) {opt} {marker}")
        print()