import aiosqlite
import datetime
from typing import Optional, List, Dict, Any

DB_PATH = "profiles.db"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS profiles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tg_id INTEGER UNIQUE,
    server TEXT,
    nickname TEXT,
    uid TEXT,
    adventure_rank TEXT,
    playstyle TEXT,
    languages TEXT,
    platforms TEXT,
    playtime TEXT,
    bio TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
"""

# likes table SQL (references profiles.tg_id)
CREATE_LIKES_SQL = """
CREATE TABLE IF NOT EXISTS likes (
    owner_id INTEGER NOT NULL,
    viewer_id INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (owner_id, viewer_id),
    FOREIGN KEY (owner_id) REFERENCES profiles(tg_id) ON DELETE CASCADE
);
"""

async def init_db():
    """
    Initialize DB: ensures profiles and likes tables exist.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(CREATE_TABLE_SQL)
        await db.execute(CREATE_LIKES_SQL)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_likes_owner ON likes(owner_id);")
        await db.commit()

# ---------------- Profiles API ----------------

async def save_profile(tg_id: int, data: Dict[str, Any]) -> None:
    """Insert or update a profile (upsert)."""
    now = datetime.datetime.utcnow().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT 1 FROM profiles WHERE tg_id = ? LIMIT 1", (tg_id,))
        exists = await cur.fetchone()
        await cur.close()
        if exists:
            await db.execute("""
                UPDATE profiles
                SET server=?, nickname=?, uid=?, adventure_rank=?, languages=?, playtime=?, bio=?, platforms=?, playstyle=?
                WHERE tg_id=?
            """, (
                data.get("server",""), data.get("nickname",""), data.get("uid",""),
                data.get("adventure_rank",""), data.get("languages",""), data.get("playtime",""),
                data.get("bio",""), data.get("platforms",""), data.get("playstyle",""), tg_id
            ))
        else:
            await db.execute("""
                INSERT INTO profiles (tg_id, server, nickname, uid, adventure_rank, languages, playtime, bio, platforms, playstyle, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                tg_id, data.get("server",""), data.get("nickname",""), data.get("uid",""),
                data.get("adventure_rank",""), data.get("languages",""), data.get("playtime",""),
                data.get("bio",""), data.get("platforms",""), data.get("playstyle",""), now
            ))
        await db.commit()

async def get_profile_by_tg(tg_id: int) -> Optional[Dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT tg_id, server, nickname, uid, adventure_rank, playstyle, languages, platforms, playtime, bio, created_at
            FROM profiles WHERE tg_id = ? LIMIT 1
        """, (tg_id,))
        row = await cur.fetchone()
        await cur.close()
        if not row:
            return None
        keys = ["tg_id","server","nickname","uid","adventure_rank","playstyle","languages","platforms","playtime","bio","created_at"]
        return dict(zip(keys, row))

async def delete_profile(tg_id: int) -> None:
    """
    Delete profile and cascade delete likes (owner likes) via FK.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM profiles WHERE tg_id = ?", (tg_id,))
        await db.commit()

async def count_profiles(server: str) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT COUNT(*) FROM profiles WHERE server = ?", (server,))
        row = await cur.fetchone()
        await cur.close()
        return int(row[0]) if row else 0

async def list_profiles(server: str, limit: int = 10, offset: int = 0) -> List[Dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT tg_id, server, nickname, uid, adventure_rank, playstyle, languages, platforms, playtime, bio, created_at
            FROM profiles
            WHERE server = ?
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?
        """, (server, limit, offset))
        rows = await cur.fetchall()
        await cur.close()
        keys = ["tg_id","server","nickname","uid","adventure_rank","playstyle","languages","platforms","playtime","bio","created_at"]
        return [dict(zip(keys, r)) for r in rows]

# ---------------- Likes API (now in same DB) ----------------

async def add_like(viewer_id: int, owner_id: int) -> bool:
    """
    Add like (owner_id — профиль получателя, viewer_id — кто лайкнул).
    Returns True if inserted, False if already existed.
    """
    created = datetime.datetime.utcnow().isoformat()
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("INSERT INTO likes (owner_id, viewer_id, created_at) VALUES (?, ?, ?)", (owner_id, viewer_id, created))
            await db.commit()
            return True
    except aiosqlite.IntegrityError:
        return False

async def has_liked(viewer_id: int, owner_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT 1 FROM likes WHERE viewer_id = ? AND owner_id = ? LIMIT 1", (viewer_id, owner_id))
        row = await cur.fetchone()
        await cur.close()
        return row is not None

async def get_likes_count(owner_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT COUNT(*) FROM likes WHERE owner_id = ?", (owner_id,))
        row = await cur.fetchone()
        await cur.close()
        return int(row[0]) if row else 0