"""
mango_saver.py

Async storage helper for Mango TV music repo.
- Uses Motor (AsyncIOMotorClient) to store songs & videos in MongoDB.
- Ensures each track/video is stored only once using unique indexes.
- Provides helpers to: fetch from DB if exists, otherwise call a provided fetch callback, save record, and return record.

Usage (short): import this file in your bot and call:

    # to get a song (returns db record)
    record = await get_track_by_youtube_id("YtID123", fetch_if_missing=fetch_fn)

    # to get a video
    record = await get_video_by_youtube_id("YtVID456", fetch_if_missing=fetch_video_fn)

fetch_if_missing should be an async function that takes the id and returns a dict with the required fields (see below).

Required pip packages: motor

Record formats saved:
- Song: {"youtube_id": str, "title": str, "thumbnail": str, "singer": str, "type": "song", "saved_at": datetime}
- Video: {"video_id": str, "title": str, "thumbnail": str, "singer": str, "url": str, "type": "video", "saved_at": datetime}

The module will create unique indexes on 'youtube_id' and 'video_id' to avoid duplicates.

"""

import os
import asyncio
from datetime import datetime
from typing import Optional, Callable, Awaitable

from motor.motor_asyncio import AsyncIOMotorClient

# Configuration: either set MONGO_URL env var or pass your client in init_db
MONGO_URL = os.environ.get("MONGO_URL", "mongodb://localhost:27017")
DB_NAME = os.environ.get("MANGO_DB", "mango_tv_db")

# Collections
COL_SONGS = "mango_songs"
COL_VIDEOS = "mango_videos"

# Global client (initialized by init_db or automatically on import)
_client: Optional[AsyncIOMotorClient] = None
_db = None


def _ensure_client():
    global _client, _db
    if _client is None:
        _client = AsyncIOMotorClient(MONGO_URL)
        _db = _client[DB_NAME]


async def init_db(mongo_url: Optional[str] = None, db_name: Optional[str] = None):
    """Initialize DB client and create indexes. Call this at bot startup.

    Args:
        mongo_url: override MONGO_URL
        db_name: override DB_NAME
    """
    global _client, _db, MONGO_URL, DB_NAME
    if mongo_url:
        MONGO_URL = mongo_url
    if db_name:
        DB_NAME = db_name

    _ensure_client()

    # create indexes to ensure uniqueness and faster lookups
    await _db[COL_SONGS].create_index("youtube_id", unique=True, background=True)
    await _db[COL_VIDEOS].create_index("video_id", unique=True, background=True)
    # optional index on title or singer if needed later
    await _db[COL_SONGS].create_index("title")
    await _db[COL_VIDEOS].create_index("title")


async def save_song(youtube_id: str, title: str, thumbnail: str, singer: str) -> dict:
    """Save song record if not exists. Returns the stored document.

    If a duplicate exists, it returns the existing document.
    """
    _ensure_client()
    coll = _db[COL_SONGS]
    now = datetime.utcnow()
    doc = {
        "youtube_id": youtube_id,
        "title": title,
        "thumbnail": thumbnail,
        "singer": singer,
        "type": "song",
        "saved_at": now,
    }
    try:
        res = await coll.insert_one(doc)
        doc["_id"] = res.inserted_id
        return doc
    except Exception:
        # likely duplicate key error; return existing
        existing = await coll.find_one({"youtube_id": youtube_id})
        return existing


async def save_video(video_id: str, title: str, thumbnail: str, singer: str, url: str) -> dict:
    """Save video record if not exists. Returns the stored document.
    """
    _ensure_client()
    coll = _db[COL_VIDEOS]
    now = datetime.utcnow()
    doc = {
        "video_id": video_id,
        "title": title,
        "thumbnail": thumbnail,
        "singer": singer,
        "url": url,
        "type": "video",
        "saved_at": now,
    }
    try:
        res = await coll.insert_one(doc)
        doc["_id"] = res.inserted_id
        return doc
    except Exception:
        existing = await coll.find_one({"video_id": video_id})
        return existing


async def get_song_by_youtube_id(youtube_id: str) -> Optional[dict]:
    _ensure_client()
    return await _db[COL_SONGS].find_one({"youtube_id": youtube_id})


async def get_video_by_video_id(video_id: str) -> Optional[dict]:
    _ensure_client()
    return await _db[COL_VIDEOS].find_one({"video_id": video_id})


# High-level helpers that accept a fetch callback. The fetch callback should only be called
# when the record is not present in DB. This enforces "one-time save" behavior.

async def get_track_by_youtube_id(youtube_id: str, fetch_if_missing: Optional[Callable[[str], Awaitable[dict]]] = None) -> dict:
    """
    Get song record by youtube_id. If not found and fetch_if_missing is provided,
    it will call fetch_if_missing(youtube_id) to get details from YouTube, save to DB, and return.

    fetch_if_missing must be an async function that returns a dict containing at least:
      {"youtube_id": str, "title": str, "thumbnail": str, "singer": str}

    Example fetch_if_missing result keys can be adapted — this function uses the keys above.
    """
    found = await get_song_by_youtube_id(youtube_id)
    if found:
        return found
    if fetch_if_missing is None:
        raise LookupError("Song not found in DB and no fetch_if_missing provided")

    # fetch from YouTube (or other source) using provided callback
    fetched = await fetch_if_missing(youtube_id)
    # normalize fields
    yid = fetched.get("youtube_id") or youtube_id
    title = fetched.get("title", "Unknown Title")
    thumbnail = fetched.get("thumbnail", "")
    singer = fetched.get("singer", fetched.get("artist", "Unknown"))

    return await save_song(yid, title, thumbnail, singer)


async def get_video_by_id_with_cache(video_id: str, fetch_if_missing: Optional[Callable[[str], Awaitable[dict]]] = None) -> dict:
    """
    Same pattern for videos. fetch_if_missing must return at least:
      {"video_id": str, "title": str, "thumbnail": str, "singer": str, "url": str}
    """
    found = await get_video_by_video_id(video_id)
    if found:
        return found
    if fetch_if_missing is None:
        raise LookupError("Video not found in DB and no fetch_if_missing provided")

    fetched = await fetch_if_missing(video_id)
    vid = fetched.get("video_id") or video_id
    title = fetched.get("title", "Unknown Title")
    thumbnail = fetched.get("thumbnail", "")
    singer = fetched.get("singer", fetched.get("artist", "Unknown"))
    url = fetched.get("url", "")

    return await save_video(vid, title, thumbnail, singer, url)


# Example small helper to demonstrate how to integrate in your play flow.
# (Do not call this in library import — it's safe to import.)

async def example_integration(youtube_id: str, is_video: bool, fetch_cb: Callable[[str], Awaitable[dict]]):
    """Example wrapper: returns DB record (song or video) using caching logic."""
    if is_video:
        return await get_video_by_id_with_cache(youtube_id, fetch_if_missing=fetch_cb)
    else:
        return await get_track_by_youtube_id(youtube_id, fetch_if_missing=fetch_cb)


# If the module is run directly, create indexes (useful for testing)
if __name__ == "__main__":
    async def _main():
        await init_db()
        print("Indexes created and DB initialized (MONGO_URL=%s DB=%s)" % (MONGO_URL, DB_NAME))

    asyncio.run(_main())
