import os
import sqlite3
from contextlib import contextmanager
from typing import Optional, Tuple, Iterable, List, Any

DEFAULT_HOME = os.path.join(os.path.expanduser("~"), ".whatsfind")
DEFAULT_DB_PATH = os.path.join(DEFAULT_HOME, "whatsfind.db")

SCHEMA_SQL = '''
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS chats(
  id INTEGER PRIMARY KEY,
  title TEXT
);
CREATE TABLE IF NOT EXISTS participants(
  id INTEGER PRIMARY KEY,
  chat_id INTEGER,
  name TEXT,
  UNIQUE(chat_id, name)
);
CREATE TABLE IF NOT EXISTS messages(
  id INTEGER PRIMARY KEY,
  chat_id INTEGER,
  ts INTEGER,           -- epoch ms UTC
  sender TEXT,          -- NULL for system
  type TEXT CHECK(type IN ('message','system')) NOT NULL DEFAULT 'message',
  text TEXT,
  has_media INTEGER DEFAULT 0,
  media_path TEXT
);
CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts
  USING fts5(text, content='messages', content_rowid='id', tokenize='porter');
CREATE TRIGGER IF NOT EXISTS messages_ai AFTER INSERT ON messages BEGIN
  INSERT INTO messages_fts(rowid, text) VALUES (new.id, new.text);
END;
CREATE TRIGGER IF NOT EXISTS messages_ad AFTER DELETE ON messages BEGIN
  INSERT INTO messages_fts(messages_fts, rowid, text) VALUES ('delete', old.id, old.text);
END;
CREATE TRIGGER IF NOT EXISTS messages_au AFTER UPDATE ON messages BEGIN
  INSERT INTO messages_fts(messages_fts, rowid, text) VALUES ('delete', old.id, old.text);
  INSERT INTO messages_fts(rowid, text) VALUES (new.id, new.text);
END;
'''

def ensure_db(path: Optional[str] = None) -> str:
    db_path = path or DEFAULT_DB_PATH
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.executescript(SCHEMA_SQL)
    return db_path

@contextmanager
def connect(db_path: Optional[str] = None):
    path = db_path or DEFAULT_DB_PATH
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

def bulk_insert_messages(conn: sqlite3.Connection, rows: Iterable[Tuple]):
    conn.executemany(
        "INSERT INTO messages (chat_id, ts, sender, type, text, has_media, media_path) VALUES (?,?,?,?,?,?,?)", 
        rows
    )

def check_chat_has_messages(conn: sqlite3.Connection, chat_id: int) -> bool:
    """Check if a chat already has messages (to prevent duplicate imports)"""
    cur = conn.execute("SELECT COUNT(*) as count FROM messages WHERE chat_id = ?", (chat_id,))
    result = cur.fetchone()
    return result["count"] > 0 if result else False

def clear_chat_messages(conn: sqlite3.Connection, chat_id: int):
    """Clear all messages for a specific chat"""
    conn.execute("DELETE FROM messages WHERE chat_id = ?", (chat_id,))

def clear_all_data(conn):
    """Clear all data from the database"""
    conn.execute("DELETE FROM messages")
    conn.execute("DELETE FROM chats")
    conn.execute("DELETE FROM participants")
    # Reset auto-increment counters if sqlite_sequence table exists
    result = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sqlite_sequence'").fetchone()
    if result:
        conn.execute("DELETE FROM sqlite_sequence")

def upsert_chat(conn: sqlite3.Connection, title: str) -> int:
    cur = conn.execute("SELECT id FROM chats WHERE title = ?", (title,))
    r = cur.fetchone()
    if r:
        return r["id"]
    cur = conn.execute("INSERT INTO chats(title) VALUES (?)", (title,))
    return cur.lastrowid

def upsert_participant(conn: sqlite3.Connection, chat_id: int, name: str) -> int:
    cur = conn.execute("SELECT id FROM participants WHERE chat_id=? AND name=?", (chat_id, name))
    r = cur.fetchone()
    if r:
        return r["id"]
    cur = conn.execute("INSERT INTO participants(chat_id, name) VALUES (?,?)", (chat_id, name))
    return cur.lastrowid

def search(conn: sqlite3.Connection, query_text: str, chat_id: Optional[int], sender: Optional[str], t1: Optional[int], t2: Optional[int], has_media: Optional[bool], limit:int=100, offset:int=0) -> List[sqlite3.Row]:
    q = """
    SELECT m.* FROM messages m
    JOIN messages_fts f ON f.rowid = m.id
    WHERE f.text MATCH :q
    """
    params: dict[str, Any] = {"q": query_text}
    if chat_id:
        q += " AND m.chat_id = :chat_id"
        params["chat_id"] = chat_id
    if sender:
        q += " AND m.sender = :sender"
        params["sender"] = sender
    if t1 is not None and t2 is not None:
        q += " AND m.ts BETWEEN :t1 AND :t2"
        params["t1"] = t1; params["t2"] = t2
    if has_media is not None:
        q += " AND m.has_media = :hm"
        params["hm"] = 1 if has_media else 0
    q += " ORDER BY m.ts DESC LIMIT :limit OFFSET :offset"
    params["limit"] = limit; params["offset"] = offset
    return list(conn.execute(q, params))

def list_facets(conn: sqlite3.Connection):
    chats = list(conn.execute("SELECT id, title FROM chats ORDER BY title"))
    senders = list(conn.execute("SELECT DISTINCT sender FROM messages WHERE sender IS NOT NULL ORDER BY sender"))
    years = list(conn.execute("SELECT DISTINCT strftime('%Y', datetime(ts/1000,'unixepoch')) AS y FROM messages ORDER BY y"))
    return chats, [s["sender"] for s in senders], [y["y"] for y in years if y["y"] is not None]

def get_thread(conn: sqlite3.Connection, message_id: int, context:int=25):
    row = conn.execute("SELECT * FROM messages WHERE id=?", (message_id,)).fetchone()
    if not row:
        return [], None
    chat_id = row["chat_id"]
    ts = row["ts"]
    before = conn.execute(
        "SELECT * FROM messages WHERE chat_id=? AND ts<=? ORDER BY ts DESC LIMIT ?", (chat_id, ts, context)
    ).fetchall()
    after = conn.execute(
        "SELECT * FROM messages WHERE chat_id=? AND ts>? ORDER BY ts ASC LIMIT ?", (chat_id, ts, context)
    ).fetchall()
    thread = list(reversed(before)) + [row] + list(after)
    return thread, row

def get_chat_messages(conn: sqlite3.Connection, chat_id: int, limit: int = 50, offset: int = 0) -> List[sqlite3.Row]:
    """
    Get messages from a specific chat in reverse chronological order (newest first) with pagination.
    """
    return list(conn.execute(
        "SELECT * FROM messages WHERE chat_id = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
        (chat_id, limit, offset)
    ))

def get_chat_message_count(conn: sqlite3.Connection, chat_id: int) -> int:
    """
    Get the total number of messages in a specific chat.
    """
    result = conn.execute("SELECT COUNT(*) as count FROM messages WHERE chat_id = ?", (chat_id,)).fetchone()
    return result["count"] if result else 0

def get_all_chats_with_stats(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    """
    Get all chats with message counts and date ranges.
    """
    return list(conn.execute("""
        SELECT 
            c.id, 
            c.title,
            COUNT(m.id) as message_count,
            MIN(m.ts) as first_message_ts,
            MAX(m.ts) as last_message_ts,
            COUNT(DISTINCT m.sender) as participant_count
        FROM chats c
        LEFT JOIN messages m ON c.id = m.chat_id
        GROUP BY c.id, c.title
        ORDER BY MAX(m.ts) DESC
    """))
