# API/fetch_api_data.py
import os
import sys
import json
import argparse
from dotenv import load_dotenv

# pip install newsapi-python python-dotenv
try:
    from newsapi import NewsApiClient
except Exception as e:
    raise ImportError(
        "newsapi-python not installed. Run: pip install newsapi-python"
    ) from e

# Try to load .env from the working directory if present (harmless if not)
# Use explicit path to avoid surprising behavior when imported from other workdirs.
_load_env_path = os.path.join(os.getcwd(), ".env")
load_dotenv(dotenv_path=_load_env_path)

# Do NOT read NEWSAPI_KEY at import time and raise.
# Instead, lazily create the NewsApiClient when a fetch function is called.
_NEWSAPI_CLIENT = None

def _get_newsapi_client():
    """
    Lazily initialize and return a NewsApiClient instance.
    Raises RuntimeError with a clear message if NEWSAPI_KEY is not provided.
    """
    global _NEWSAPI_CLIENT
    if _NEWSAPI_CLIENT is not None:
        return _NEWSAPI_CLIENT

    key = os.getenv("NEWSAPI_KEY")
    if not key:
        # Helpful error instructing how to supply the key (env_file or env var)
        raise RuntimeError(
            "NEWSAPI_KEY missing. Provide NEWSAPI_KEY via environment variable or a .env file. "
            "Example .env line: NEWSAPI_KEY=your_key_here\n\n"
            "If you run with docker-compose, add `env_file: - .env` to your service or "
            "use `docker run --env-file .env ...`."
        )

    _NEWSAPI_CLIENT = NewsApiClient(api_key=key)
    return _NEWSAPI_CLIENT


def fetch_top_headlines(q=None, sources=None, category=None, language=None, country=None, page_size=20, page=1):
    """
    Wrapper for NewsApiClient.get_top_headlines
    """
    try:
        client = _get_newsapi_client()
        resp = client.get_top_headlines(q=q, sources=sources, category=category,
                                        language=language, country=country,
                                        page_size=page_size, page=page)
        return {"ok": True, "method": "top_headlines", "result": resp}
    except Exception as e:
        # Preserve original behavior of returning structured error dict
        return {"ok": False, "method": "top_headlines", "error": str(e)}


def fetch_everything(q=None, sources=None, domains=None, from_param=None, to=None,
                     language=None, sort_by=None, page_size=20, page=1):
    """
    Wrapper for NewsApiClient.get_everything
    Note: 'from' is a python reserved word so the library uses 'from_param'
    """
    try:
        client = _get_newsapi_client()
        resp = client.get_everything(q=q, sources=sources, domains=domains,
                                     from_param=from_param, to=to, language=language,
                                     sort_by=sort_by, page_size=page_size, page=page)
        return {"ok": True, "method": "everything", "result": resp}
    except Exception as e:
        return {"ok": False, "method": "everything", "error": str(e)}


def fetch_sources(category=None, language=None, country=None):
    """
    Wrapper for NewsApiClient.get_sources
    """
    try:
        client = _get_newsapi_client()
        resp = client.get_sources(category=category, language=language, country=country)
        return {"ok": True, "method": "sources", "result": resp}
    except Exception as e:
        return {"ok": False, "method": "sources", "error": str(e)}


def parse_args(argv):
    parser = argparse.ArgumentParser(description="Fetch news using newsapi-python and NEWSAPI_KEY from .env")
    sub = parser.add_subparsers(dest="action", required=True, help="Action to perform")

    # top_headlines
    th = sub.add_parser("top_headlines", help="Get top headlines")
    th.add_argument("--q", type=str, help="Keywords or phrase to search for")
    th.add_argument("--sources", type=str, help="Comma-separated identifiers for the news sources or blogs (optional)")
    th.add_argument("--category", type=str, help="Category (business, entertainment, general, health, science, sports, technology)")
    th.add_argument("--language", type=str, help="Language code (eg 'en')")
    th.add_argument("--country", type=str, help="2-letter country code (eg 'us')")
    th.add_argument("--page_size", type=int, default=20)
    th.add_argument("--page", type=int, default=1)

    # everything
    ev = sub.add_parser("everything", help="Search all articles")
    ev.add_argument("--q", type=str, help="Keywords or phrase to search for")
    ev.add_argument("--sources", type=str, help="Comma-separated source identifiers")
    ev.add_argument("--domains", type=str, help="Comma-separated domains (eg bbc.co.uk, techcrunch.com)")
    ev.add_argument("--from", dest="from_param", type=str, help="A date and optional time for the oldest article (YYYY-MM-DD or ISO)")
    ev.add_argument("--to", type=str, help="A date and optional time for the newest article (YYYY-MM-DD or ISO)")
    ev.add_argument("--language", type=str, help="Language code")
    ev.add_argument("--sort_by", type=str, choices=["relevancy", "popularity", "publishedAt"], help="Sort results by")
    ev.add_argument("--page_size", type=int, default=20)
    ev.add_argument("--page", type=int, default=1)

    # sources
    src = sub.add_parser("sources", help="List news sources")
    src.add_argument("--category", type=str, help="Category")
    src.add_argument("--language", type=str, help="Language code")
    src.add_argument("--country", type=str, help="Country code")

    return parser.parse_args(argv)


def main(argv):
    args = parse_args(argv)

    if args.action == "top_headlines":
        res = fetch_top_headlines(q=args.q, sources=args.sources, category=args.category,
                                  language=args.language, country=args.country,
                                  page_size=args.page_size, page=args.page)

    elif args.action == "everything":
        res = fetch_everything(q=args.q, sources=args.sources, domains=args.domains,
                               from_param=args.from_param, to=args.to,
                               language=args.language, sort_by=args.sort_by,
                               page_size=args.page_size, page=args.page)

    elif args.action == "sources":
        res = fetch_sources(category=args.category, language=args.language, country=args.country)

    else:
        res = {"ok": False, "error": "Unknown action"}

    # Print pretty JSON for CLI usage / debugging
    print(json.dumps(res, indent=4, ensure_ascii=False))


if __name__ == "__main__":
    # Run with command-line args (skip the script name)
    main(sys.argv[1:])
