# process.py
import json
from typing import Any, Dict, Optional

OUTPUT_KEYS = ["source", "author", "title", "description", "url", "publishedAt", "content"]

def _safe_load(value: Any) -> Any:
    """Parse JSON string values to Python objects when possible."""
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _get_from_candidates(obj: dict, candidates: list) -> Optional[Any]:
    """Case-insensitive lookup in dict for first candidate that exists and is non-empty."""
    if not isinstance(obj, dict):
        return None
    lower_map = {k.lower(): k for k in obj.keys()}
    for cand in candidates:
        key = cand.lower()
        if key in lower_map:
            val = obj[lower_map[key]]
            if val is not None and val != "":
                return val
    return None


def _first_available(value: Any, keys: list) -> Optional[Any]:
    """Given value (dict/list/primitive), try to locate the first existing field from keys."""
    if value is None:
        return None
    # If list, try first element
    if isinstance(value, list):
        if not value:
            return None
        return _first_available(value[0], keys)
    # If dict, try keys and some common wrappers
    if isinstance(value, dict):
        found = _get_from_candidates(value, keys)
        if found is not None:
            return found
        for wrapper in ("payload", "data", "message", "body", "article", "item"):
            if wrapper in value and isinstance(value[wrapper], (dict, list)):
                res = _first_available(value[wrapper], keys)
                if res is not None:
                    return res
        return None
    # Primitive (string/number) fallback
    if isinstance(value, (str, int, float)):
        return value
    return None


def _extract_source(raw: Any) -> Optional[str]:
    """Normalize source field (dict with name/id or string)."""
    if raw is None:
        return None
    if isinstance(raw, str):
        return raw.strip() or None
    if isinstance(raw, dict):
        name = _get_from_candidates(raw, ["name", "title", "id"])
        if name:
            return name.strip() if isinstance(name, str) else str(name)
        try:
            return json.dumps(raw, ensure_ascii=False)
        except Exception:
            return str(raw)
    return str(raw)


def _extract_published(raw: Any) -> Optional[str]:
    """Return published value as string (don't try to reformat)."""
    if raw is None:
        return None
    if isinstance(raw, str):
        return raw.strip() or None
    if isinstance(raw, (int, float)):
        return str(raw)
    return None


def extract(message_value: Any) -> Dict[str, Optional[str]]:
    """
    Public function that accepts a Kafka message value (dict or JSON string)
    and extracts:
      source, author, title, description, url, publishedAt, content

    IMPORTANT: This function intentionally does NOT extract any `urli_img` / `url_img`.
    """
    msg = _safe_load(message_value)

    # unwrap common top-level wrappers
    if isinstance(msg, dict):
        for wrapper in ("payload", "data", "message", "body", "article"):
            if wrapper in msg and isinstance(msg[wrapper], (dict, list)):
                msg = msg[wrapper]
                break

    # if list, pick the first article
    if isinstance(msg, list):
        msg = msg[0] if msg else {}

    # if after all this it's not a dict, return empty result
    if not isinstance(msg, dict):
        return {k: None for k in OUTPUT_KEYS}

    # candidate keys
    source_candidates = ["source", "source_name", "sourceName", "publisher"]
    author_candidates = ["author", "byline", "creator", "writer"]
    title_candidates = ["title", "headline", "heading"]
    description_candidates = ["description", "summary", "abstract"]
    url_candidates = ["url", "link", "articleUrl", "sourceUrl", "web_url"]
    published_candidates = ["publishedAt", "published_at", "pubDate", "published", "date"]
    content_candidates = ["content", "fullText", "article_content", "body", "text"]

    raw_source = _first_available(msg, source_candidates)
    raw_author = _first_available(msg, author_candidates)
    raw_title = _first_available(msg, title_candidates)
    raw_description = _first_available(msg, description_candidates)
    raw_url = _first_available(msg, url_candidates)
    raw_published = _first_available(msg, published_candidates)
    raw_content = _first_available(msg, content_candidates)

    # If msg has nested source object like {"source": {"name": "..."}}
    if "source" in msg and isinstance(msg["source"], dict) and not raw_source:
        raw_source = _get_from_candidates(msg["source"], ["name", "id", "title"])

    result = {
        "source": _extract_source(raw_source),
        "author": (raw_author.strip() if isinstance(raw_author, str) and raw_author.strip() else None),
        "title": (raw_title.strip() if isinstance(raw_title, str) and raw_title.strip() else None),
        "description": (raw_description.strip() if isinstance(raw_description, str) and raw_description.strip() else None),
        "url": (raw_url.strip() if isinstance(raw_url, str) and raw_url.strip() else None),
        "publishedAt": _extract_published(raw_published),
        "content": (raw_content.strip() if isinstance(raw_content, str) and raw_content.strip() else None),
    }

    # Ensure we DO NOT include image URL fields — remove keys that look like image URLs
    # This guards against common keys like 'url_img', 'image', 'imageUrl', 'urli_img'
    for forbidden in ("url_img", "image", "imageUrl", "urli_img", "image_url"):
        if forbidden in result:
            result.pop(forbidden, None)

    return result
