import os
import io
import time
import requests
import logging
import statistics
import pandas as pd
from collections import Counter
from urllib.parse import urlparse, urlunparse

from fastapi import FastAPI, HTTPException, Request, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from pydantic import BaseModel

# =========================================================
# SETUP
# =========================================================
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("LinkedInScraperAPI")

app = FastAPI(title="LinkedIn Scraper API Wrapper")

# Allow frontend (React, etc.)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "*",
    ],
    allow_methods=["*"],
    allow_headers=["*"],
)

RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY", "")
RAPIDAPI_HOST = "linkedin-scraper-api-real-time-fast-affordable.p.rapidapi.com"

# =========================================================
# RATE LIMITER
# =========================================================
last_call_time = 0


def rate_limit(min_interval=1.2):
    """Ensure at least 1.2 seconds between RapidAPI calls."""
    global last_call_time
    now = time.time()
    elapsed = now - last_call_time
    if elapsed < min_interval:
        sleep_for = min_interval - elapsed
        logger.info(f"⏳ Rate limit enforced, sleeping {sleep_for:.2f}s")
        time.sleep(sleep_for)
    last_call_time = time.time()


# =========================================================
# HELPERS
# =========================================================
def clean_linkedin_url(url: str) -> str:
    parsed = urlparse(url)
    return urlunparse(parsed._replace(query=""))


def fetch_from_rapidapi(endpoint: str, params: dict, rapidapi_key: str = None):
    """Generic fetch with retry + rate limiting"""
    key_to_use = rapidapi_key or RAPIDAPI_KEY
    if not key_to_use:
        raise HTTPException(status_code=400, detail="Missing RapidAPI key.")

    headers = {
        "x-rapidapi-key": key_to_use,
        "x-rapidapi-host": RAPIDAPI_HOST,
    }

    url = f"https://{RAPIDAPI_HOST}/{endpoint}"

    for attempt in range(3):
        try:
            rate_limit()
            logger.info(f"➡️ [{attempt+1}/3] Fetching {url} params={params}")
            res = requests.get(url, headers=headers, params=params, timeout=30)

            if res.status_code == 429:
                wait = 2 ** attempt
                logger.warning(f"⚠️ 429 quota hit — retrying after {wait}s...")
                time.sleep(wait)
                continue

            res.raise_for_status()
            data = res.json()
            if not data:
                raise HTTPException(status_code=502, detail="Empty response.")
            return data

        except requests.exceptions.RequestException as e:
            logger.error(f"Request error: {e}")
            if attempt == 2:
                raise HTTPException(status_code=500, detail=str(e))

    raise HTTPException(status_code=429, detail="RapidAPI quota exceeded.")


def process_csv_upload(file: UploadFile) -> pd.DataFrame:
    """Reads uploaded CSV into a DataFrame."""
    try:
        contents = file.file.read()
        df = pd.read_csv(io.BytesIO(contents))
        return df
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid CSV file: {e}")
    finally:
        file.file.close()


# =========================================================
# ROUTES
# =========================================================
@app.get("/")
async def root():
    return {"status": "✅ LinkedIn Scraper API is running"}


# ---------- PROFILE ----------
@app.get("/api/profile")
def get_profile(request: Request, username: str):
    return fetch_from_rapidapi(
        "profile/detail",
        {"username": username},
        request.headers.get("x-rapidapi-key"),
    )


@app.get("/api/posts")
def get_posts(request: Request, username: str, page_number: int = 1):
    return fetch_from_rapidapi(
        "profile/posts",
        {"username": username, "page_number": page_number},
        request.headers.get("x-rapidapi-key"),
    )


@app.get("/api/comments")
def get_comments(
    request: Request,
    post_url: str,
    page_number: int = 1,
    sort_order: str = "Most relevant",
):
    clean_url = clean_linkedin_url(post_url)
    return fetch_from_rapidapi(
        "post/comments",
        {"post_url": clean_url, "page_number": page_number, "sort_order": sort_order},
        request.headers.get("x-rapidapi-key"),
    )


@app.get("/api/company")
def get_company(request: Request, identifier: str):
    return fetch_from_rapidapi(
        "companies/detail",
        {"identifier": identifier},
        request.headers.get("x-rapidapi-key"),
    )


# ---------- ANALYTICS ----------
@app.get("/api/analytics/comments")
def comment_analytics(request: Request, post_url: str):
    clean_url = clean_linkedin_url(post_url)
    data = fetch_from_rapidapi(
        "post/comments",
        {"post_url": clean_url},
        request.headers.get("x-rapidapi-key"),
    )

    comments = data.get("data", {}).get("comments", [])
    if not comments:
        return {"success": False, "error": "No comments found"}

    authors = [c.get("author", {}).get("name") for c in comments if c.get("author", {}).get("name")]
    reactions = [c.get("stats", {}).get("total_reactions", 0) for c in comments]

    return {
        "success": True,
        "summary": {
            "total_comments": len(comments),
            "unique_commenters": len(set(authors)),
            "average_reactions": statistics.mean(reactions) if reactions else 0,
            "top_commenters": Counter(authors).most_common(5),
        },
    }


# ---------- REACTIONS ----------
class ReactionRequest(BaseModel):
    post_url: str
    page_number: str = "1"
    reaction_type: str = "ALL"


@app.post("/api/post/reactions")
async def get_post_reactions(request_data: ReactionRequest, request: Request):
    post_url = request_data.post_url.strip()
    if not post_url:
        raise HTTPException(status_code=400, detail="Missing post_url")

    key = request.headers.get("x-rapidapi-key", RAPIDAPI_KEY)
    params = {
        "post_url": post_url,
        "page_number": request_data.page_number,
        "reaction_type": request_data.reaction_type,
    }
    return fetch_from_rapidapi("post/reactions", params, key)

# =========================================================
# BULK UPLOAD ROUTES
# =========================================================
@app.post("/api/upload/profiles")
async def upload_usernames_csv(file: UploadFile = File(...), request: Request = None):
    """
    Upload a CSV file containing LinkedIn usernames and fetch their profile data.
    """
    logger.info(f"📂 Received file: {file.filename if file else 'No file'}")
    df = process_csv_upload(file)

    if "username" not in df.columns:
        raise HTTPException(status_code=400, detail="CSV must contain a 'username' column.")

    results = []
    key = request.headers.get("x-rapidapi-key", RAPIDAPI_KEY)

    for username in df["username"]:
        username = str(username).strip()
        if not username:
            continue

        try:
            logger.info(f"📥 Fetching LinkedIn profile for {username}")
            data = fetch_from_rapidapi("profile/detail", {"username": username}, key)
            results.append({"username": username, "data": data})
        except HTTPException as e:
            logger.error(f"❌ Error fetching {username}: {e.detail}")
            results.append({"username": username, "error": e.detail})

        time.sleep(1.5)

    return {"success": True, "count": len(results), "results": results}



@app.post("/api/upload/posts")
async def upload_posts_csv(file: UploadFile = File(...), request: Request = None):
    """
    Upload a CSV containing LinkedIn usernames and fetch their posts.
    """
    logger.info(f"📂 Received posts CSV: {file.filename if file else 'No file'}")
    df = process_csv_upload(file)

    if "username" not in df.columns:
        raise HTTPException(status_code=400, detail="CSV must contain a 'username' column.")

    results = []
    key = request.headers.get("x-rapidapi-key", RAPIDAPI_KEY)

    for username in df["username"]:
        username = str(username).strip()
        if not username:
            continue

        try:
            logger.info(f"📥 Fetching posts for {username}")
            data = fetch_from_rapidapi("profile/posts", {"username": username, "page_number": 1}, key)
            results.append({"username": username, "data": data})
        except HTTPException as e:
            logger.error(f"❌ Error fetching posts for {username}: {e.detail}")
            results.append({"username": username, "error": e.detail})

        time.sleep(1.5)

    return {"success": True, "count": len(results), "results": results}


@app.post("/api/upload/comments")
async def upload_comments_csv(file: UploadFile = File(...), request: Request = None):
    """
    Upload a CSV containing LinkedIn post URLs and fetch their comments.
    """
    logger.info(f"📂 Received comments CSV: {file.filename if file else 'No file'}")
    df = process_csv_upload(file)

    if "post_url" not in df.columns:
        raise HTTPException(status_code=400, detail="CSV must contain a 'post_url' column.")

    results = []
    key = request.headers.get("x-rapidapi-key", RAPIDAPI_KEY)

    for post_url in df["post_url"]:
        clean_url = clean_linkedin_url(str(post_url).strip())
        if not clean_url:
            continue

        try:
            logger.info(f"💬 Fetching comments for {clean_url}")
            data = fetch_from_rapidapi("post/comments", {"post_url": clean_url}, key)
            results.append({"post_url": clean_url, "data": data})
        except HTTPException as e:
            logger.error(f"❌ Error fetching comments for {clean_url}: {e.detail}")
            results.append({"post_url": clean_url, "error": e.detail})

        time.sleep(1.5)

    return {"success": True, "count": len(results), "results": results}


@app.post("/api/upload/companies")
async def upload_companies_csv(file: UploadFile = File(...), request: Request = None):
    """
    Upload a CSV containing LinkedIn company identifiers and fetch their details.
    """
    logger.info(f"📂 Received companies CSV: {file.filename if file else 'No file'}")
    df = process_csv_upload(file)

    if "identifier" not in df.columns:
        raise HTTPException(status_code=400, detail="CSV must contain an 'identifier' column.")

    results = []
    key = request.headers.get("x-rapidapi-key", RAPIDAPI_KEY)

    for identifier in df["identifier"]:
        identifier = str(identifier).strip()
        if not identifier:
            continue

        try:
            logger.info(f"🏢 Fetching company details for {identifier}")
            data = fetch_from_rapidapi("companies/detail", {"identifier": identifier}, key)
            results.append({"identifier": identifier, "data": data})
        except HTTPException as e:
            logger.error(f"❌ Error fetching company {identifier}: {e.detail}")
            results.append({"identifier": identifier, "error": e.detail})

        time.sleep(1.5)

    return {"success": True, "count": len(results), "results": results}


@app.post("/api/upload/comment-analytics")
async def upload_comment_analytics_csv(file: UploadFile = File(...), request: Request = None):
    """
    Upload a CSV containing post URLs and compute comment analytics for each.
    """
    logger.info(f"📂 Received comment analytics CSV: {file.filename if file else 'No file'}")
    df = process_csv_upload(file)

    if "post_url" not in df.columns:
        raise HTTPException(status_code=400, detail="CSV must contain a 'post_url' column.")

    results = []
    key = request.headers.get("x-rapidapi-key", RAPIDAPI_KEY)

    for post_url in df["post_url"]:
        clean_url = clean_linkedin_url(str(post_url).strip())
        if not clean_url:
            continue

        try:
            logger.info(f"📊 Analyzing comments for {clean_url}")
            data = fetch_from_rapidapi("post/comments", {"post_url": clean_url}, key)
            comments = data.get("data", {}).get("comments", [])
            if not comments:
                results.append({"post_url": clean_url, "error": "No comments found"})
                continue

            authors = [c.get("author", {}).get("name") for c in comments if c.get("author", {}).get("name")]
            reactions = [c.get("stats", {}).get("total_reactions", 0) for c in comments]

            summary = {
                "total_comments": len(comments),
                "unique_commenters": len(set(authors)),
                "average_reactions": statistics.mean(reactions) if reactions else 0,
                "top_commenters": Counter(authors).most_common(5),
            }
            results.append({"post_url": clean_url, "summary": summary})

        except HTTPException as e:
            logger.error(f"❌ Error analyzing {clean_url}: {e.detail}")
            results.append({"post_url": clean_url, "error": e.detail})

        time.sleep(1.5)

    return {"success": True, "count": len(results), "results": results}


@app.post("/api/upload/reactions")
async def upload_reactions_csv(file: UploadFile = File(...), request: Request = None):
    """
    Upload a CSV containing LinkedIn post URLs and fetch their reactions.
    """
    logger.info(f"📂 Received reactions CSV: {file.filename if file else 'No file'}")
    df = process_csv_upload(file)

    if "post_url" not in df.columns:
        raise HTTPException(status_code=400, detail="CSV must contain a 'post_url' column.")

    results = []
    key = request.headers.get("x-rapidapi-key", RAPIDAPI_KEY)

    for post_url in df["post_url"]:
        clean_url = clean_linkedin_url(str(post_url).strip())
        if not clean_url:
            continue

        try:
            logger.info(f"💡 Fetching reactions for {clean_url}")
            data = fetch_from_rapidapi("post/reactions", {"post_url": clean_url}, key)
            results.append({"post_url": clean_url, "data": data})
        except HTTPException as e:
            logger.error(f"❌ Error fetching reactions for {clean_url}: {e.detail}")
            results.append({"post_url": clean_url, "error": e.detail})

        time.sleep(1.5)

    return {"success": True, "count": len(results), "results": results}
