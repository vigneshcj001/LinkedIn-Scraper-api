# backend/main.py
import os
import requests
import logging
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from collections import Counter
import statistics

# ---------- Setup ----------
load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="LinkedIn Scraper API Wrapper")

# Allow your React dev servers; "*" for quick testing.
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "*",  # remove "*" in production
    ],
    allow_methods=["*"],
    allow_headers=["*"],
)

RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY") or ""
RAPIDAPI_HOST = "linkedin-scraper-api-real-time-fast-affordable.p.rapidapi.com"


# ---------- Core Fetch Function ----------
def fetch_from_rapidapi(endpoint: str, params: dict, rapidapi_key: str = None):
    """Generic RapidAPI fetch with error handling."""
    key_to_use = rapidapi_key or RAPIDAPI_KEY
    if not key_to_use:
        raise HTTPException(
            status_code=400, detail="RapidAPI key missing. Set in .env or send via x-rapidapi-key header."
        )

    headers = {
        "x-rapidapi-key": key_to_use,
        "x-rapidapi-host": RAPIDAPI_HOST,
    }

    url = f"https://{RAPIDAPI_HOST}/{endpoint}"
    logger.info(f"➡️ Fetching: {url} with params={params}")

    try:
        res = requests.get(url, headers=headers, params=params, timeout=30)
        logger.info(f"⬅️ RapidAPI {res.status_code}: {res.text[:300]}...")
        if res.status_code == 404:
            raise HTTPException(status_code=404, detail="Profile or endpoint not found.")
        res.raise_for_status()
        return res.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"RapidAPI Error: {e}")
        raise HTTPException(status_code=500, detail=f"RapidAPI error: {e}")


# ---------- API Routes ----------
@app.get("/api/comments")
def get_comments(request: Request, post_url: str, page_number: int = 1, sort_order: str = "Most relevant"):
    rapidapi_key = request.headers.get("x-rapidapi-key")
    return fetch_from_rapidapi(
        "post/comments",
        {"post_url": post_url, "page_number": str(page_number), "sort_order": sort_order},
        rapidapi_key,
    )


@app.get("/api/profile")
def get_profile(request: Request, username: str):
    """
    Fetch a LinkedIn profile.
    You can send either:
    - username (e.g., 'neal-mohan')
    - OR full URL (e.g., 'https://www.linkedin.com/in/neal-mohan/')
    """
    rapidapi_key = request.headers.get("x-rapidapi-key")
    return fetch_from_rapidapi("profile/detail", {"username": username}, rapidapi_key)


@app.get("/api/posts")
def get_posts(request: Request, username: str, page_number: int = 1):
    rapidapi_key = request.headers.get("x-rapidapi-key")
    return fetch_from_rapidapi(
        "profile/posts",
        {"username": username, "page_number": str(page_number)},
        rapidapi_key,
    )


@app.get("/api/analytics/comments")
def comment_analytics(request: Request, post_url: str):
    rapidapi_key = request.headers.get("x-rapidapi-key")
    data = fetch_from_rapidapi("post/comments", {"post_url": post_url}, rapidapi_key)

    comments = data.get("data", {}).get("comments", [])
    if not comments:
        return {"success": False, "error": "No comments found"}

    authors = [c.get("author", {}).get("name") for c in comments if c.get("author", {}).get("name")]
    reactions = [c.get("stats", {}).get("total_reactions", 0) for c in comments]

    most_common_authors = Counter(authors).most_common(5)
    avg_reactions = statistics.mean(reactions) if reactions else 0

    return {
        "success": True,
        "summary": {
            "total_comments": len(comments),
            "unique_commenters": len(set(authors)),
            "average_reactions": avg_reactions,
            "top_commenters": most_common_authors,
            "reaction_histogram": Counter(reactions),
        },
        "comments": comments,
    }


@app.get("/api/company")
def get_company(request: Request, identifier: str):
    """Fetch company details by LinkedIn identifier."""
    rapidapi_key = request.headers.get("x-rapidapi-key")
    return fetch_from_rapidapi("companies/detail", {"identifier": identifier}, rapidapi_key)



@app.get("/")
def root():
    return {"status": "ok", "message": "LinkedIn Scraper API is running 🚀"}

