"""Durable local state for the LINE connector: event journal, identity
registry and per-group rolling summaries.

Everything lives in one SQLite file under ``HERMES_HOME/line_platform/``
(directory 0700, file 0600). This module has no dependency on the Hermes
runtime, so the same state can be read by other tools (nightly review,
audits) without a running gateway.

Isolation rule: every read that returns conversation content or
membership takes ``(account_id, chat_id)``. There is deliberately no
"search all groups" call.
"""

from __future__ import annotations

import json
import os
import re
import secrets
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

DB_FILENAME = "line_platform.db"
SCHEMA_VERSION = 1
# Approval codes avoid look-alike characters (0/O, 1/I/L).
_CODE_ALPHABET = "ABCDEFGHJKMNPQRSTUVWXYZ23456789"
CODE_LENGTH = 6


def hermes_home() -> Path:
    return Path(os.getenv("HERMES_HOME") or (Path.home() / ".hermes"))


def default_data_dir() -> Path:
    """``LINE_CONNECTOR_HOME`` for the standalone service, otherwise a
    folder inside ``HERMES_HOME``."""
    explicit = os.getenv("LINE_CONNECTOR_HOME")
    if explicit:
        return Path(explicit).expanduser()
    return hermes_home() / "line_platform"


def _now_ms() -> int:
    return int(time.time() * 1000)


_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);

CREATE TABLE IF NOT EXISTS events (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id  TEXT NOT NULL,
    chat_type   TEXT NOT NULL,            -- 'group' | 'room' | 'dm'
    chat_id     TEXT NOT NULL,
    user_id     TEXT,
    direction   TEXT NOT NULL,            -- 'in' | 'out'
    event_type  TEXT NOT NULL,            -- 'message' | 'join' | 'memberJoined' | ...
    event_id    TEXT,                     -- LINE webhookEventId (dedupe key)
    message_id  TEXT,
    msg_type    TEXT,                     -- 'text' | 'image' | 'audio' | 'sticker' | ...
    text        TEXT NOT NULL DEFAULT '',
    media_ref   TEXT,
    reply_to_message_id TEXT,
    ts_ms       INTEGER NOT NULL,
    recorded_ms INTEGER NOT NULL,
    raw_json    TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS events_event_id
    ON events(account_id, event_id) WHERE event_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS events_chat ON events(account_id, chat_id, seq);
CREATE INDEX IF NOT EXISTS events_message ON events(account_id, message_id);

-- Chats the webhook has seen but that are not journaled (not allow-listed).
-- IDs and counters only, never content: lets the admin discover a new
-- group's ID without recording what strangers said.
CREATE TABLE IF NOT EXISTS seen_chats (
    account_id TEXT NOT NULL,
    chat_id    TEXT NOT NULL,
    chat_type  TEXT NOT NULL,
    first_ms   INTEGER NOT NULL,
    last_ms    INTEGER NOT NULL,
    count      INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (account_id, chat_id)
);

CREATE TABLE IF NOT EXISTS persons (
    account_id TEXT NOT NULL,
    user_id    TEXT NOT NULL,
    local_name TEXT NOT NULL,
    role       TEXT NOT NULL,
    relation   TEXT NOT NULL DEFAULT '',   -- relation to the owner, if registered
    note       TEXT NOT NULL DEFAULT '',
    status     TEXT NOT NULL DEFAULT 'active',
    created_ms INTEGER NOT NULL,
    updated_ms INTEGER NOT NULL,
    PRIMARY KEY (account_id, user_id)
);

CREATE TABLE IF NOT EXISTS memberships (
    account_id  TEXT NOT NULL,
    group_id    TEXT NOT NULL,
    user_id     TEXT NOT NULL,
    group_name  TEXT NOT NULL,             -- how this person is addressed in this group
    status      TEXT NOT NULL,             -- 'approved' | 'denied'
    decided_by  TEXT NOT NULL,
    decided_ms  INTEGER NOT NULL,
    PRIMARY KEY (account_id, group_id, user_id)
);

CREATE TABLE IF NOT EXISTS member_requests (
    account_id  TEXT NOT NULL,
    group_id    TEXT NOT NULL,
    user_id     TEXT NOT NULL,
    code        TEXT NOT NULL UNIQUE,
    display_hint TEXT NOT NULL DEFAULT '', -- LINE displayName: a hint, never authorization
    created_ms  INTEGER NOT NULL,
    PRIMARY KEY (account_id, group_id, user_id)
);

CREATE TABLE IF NOT EXISTS group_summaries (
    account_id TEXT NOT NULL,
    group_id   TEXT NOT NULL,
    summary    TEXT NOT NULL,
    upto_seq   INTEGER NOT NULL,
    updated_ms INTEGER NOT NULL,
    PRIMARY KEY (account_id, group_id)
);

-- Runtime settings changed from the admin interface (survive restarts).
CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);

-- Turns waiting for an external agent (Muse) to answer.
CREATE TABLE IF NOT EXISTS turns (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id  TEXT NOT NULL,
    group_id    TEXT NOT NULL,
    user_id     TEXT,
    seq         INTEGER,
    sender      TEXT NOT NULL DEFAULT '',
    question    TEXT NOT NULL,
    system_prompt TEXT NOT NULL DEFAULT '',
    context     TEXT NOT NULL DEFAULT '',
    status      TEXT NOT NULL DEFAULT 'pending',  -- pending | claimed | answered | expired
    created_ms  INTEGER NOT NULL,
    updated_ms  INTEGER NOT NULL,
    answer      TEXT
);
CREATE INDEX IF NOT EXISTS turns_status ON turns(status, id);
"""


@dataclass(frozen=True)
class EventRow:
    seq: int
    account_id: str
    chat_type: str
    chat_id: str
    user_id: Optional[str]
    direction: str
    event_type: str
    message_id: Optional[str]
    msg_type: Optional[str]
    text: str
    media_ref: Optional[str]
    reply_to_message_id: Optional[str]
    ts_ms: int


@dataclass(frozen=True)
class Identity:
    """What the context layer may know about a sender."""

    user_id: str
    local_name: str
    role: str
    relation: str
    group_name: str

    def card(self) -> str:
        name = self.group_name or self.local_name
        parts = [name]
        if self.local_name and self.local_name != name:
            parts.append(f"本名／慣稱 {self.local_name}")
        parts.append(f"角色 {self.role}")
        if self.relation:
            parts.append(f"與管理者的關係 {self.relation}")
        return "；".join(parts)


@dataclass(frozen=True)
class MemberRequest:
    account_id: str
    group_id: str
    user_id: str
    code: str
    display_hint: str
    created_ms: int


_EVENT_COLS = (
    "seq, account_id, chat_type, chat_id, user_id, direction, event_type, "
    "message_id, msg_type, text, media_ref, reply_to_message_id, ts_ms"
)


class Store:
    """Thread-safe SQLite wrapper. One instance per gateway process."""

    def __init__(self, path: Optional[os.PathLike | str] = None):
        if path is None:
            path = default_data_dir() / DB_FILENAME
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(self.path.parent, 0o700)
        except OSError:
            pass
        # Create the file with 0600 before SQLite opens it, so there is no
        # window where the journal is world-readable.
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
        os.close(fd)
        os.chmod(self.path, 0o600)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(_SCHEMA)
        self.fts_enabled = self._init_fts()
        self._conn.execute(
            "INSERT OR IGNORE INTO meta(key, value) VALUES('schema_version', ?)",
            (str(SCHEMA_VERSION),),
        )
        for suffix in ("-wal", "-shm"):
            side = Path(str(self.path) + suffix)
            if side.exists():
                try:
                    os.chmod(side, 0o600)
                except OSError:
                    pass

    def _init_fts(self) -> bool:
        """FTS5 with the trigram tokenizer handles Chinese without a word
        segmenter. Older SQLite builds lack it; search then falls back to
        LIKE, which is slower but returns the same rows."""
        try:
            self._conn.executescript(
                """
                CREATE VIRTUAL TABLE IF NOT EXISTS events_fts
                    USING fts5(text, content='events', content_rowid='seq', tokenize='trigram');
                CREATE TRIGGER IF NOT EXISTS events_fts_ai AFTER INSERT ON events BEGIN
                    INSERT INTO events_fts(rowid, text) VALUES (new.seq, new.text);
                END;
                """
            )
            return True
        except sqlite3.OperationalError:
            return False

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------------
    # Journal
    # ------------------------------------------------------------------

    def record_event(
        self,
        *,
        account_id: str,
        chat_type: str,
        chat_id: str,
        user_id: Optional[str],
        direction: str = "in",
        event_type: str = "message",
        event_id: Optional[str] = None,
        message_id: Optional[str] = None,
        msg_type: Optional[str] = None,
        text: str = "",
        media_ref: Optional[str] = None,
        reply_to_message_id: Optional[str] = None,
        ts_ms: Optional[int] = None,
        raw: Any = None,
    ) -> Optional[int]:
        """Append one event. Returns its ``seq``, or ``None`` when LINE
        redelivered an event that is already journaled."""
        raw_json = None
        if raw is not None:
            if isinstance(raw, dict):
                # A reply token is a short-lived send credential; it has no
                # audit value and must not be kept.
                raw = {k: v for k, v in raw.items() if k != "replyToken"}
            raw_json = json.dumps(raw, ensure_ascii=False, separators=(",", ":"))
        with self._lock:
            try:
                cur = self._conn.execute(
                    "INSERT INTO events(account_id, chat_type, chat_id, user_id, direction, event_type,"
                    " event_id, message_id, msg_type, text, media_ref, reply_to_message_id, ts_ms,"
                    " recorded_ms, raw_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        account_id, chat_type, chat_id, user_id, direction, event_type,
                        event_id, message_id, msg_type, text or "", media_ref, reply_to_message_id,
                        int(ts_ms if ts_ms is not None else _now_ms()), _now_ms(), raw_json,
                    ),
                )
            except sqlite3.IntegrityError:
                return None
            return int(cur.lastrowid)

    def set_media_ref(self, seq: int, media_ref: str) -> None:
        with self._lock:
            self._conn.execute("UPDATE events SET media_ref=? WHERE seq=?", (media_ref, seq))

    def note_seen_chat(self, account_id: str, chat_id: str, chat_type: str) -> None:
        now = _now_ms()
        with self._lock:
            self._conn.execute(
                "INSERT INTO seen_chats(account_id, chat_id, chat_type, first_ms, last_ms, count)"
                " VALUES (?,?,?,?,?,1) ON CONFLICT(account_id, chat_id)"
                " DO UPDATE SET last_ms=excluded.last_ms, count=count+1",
                (account_id, chat_id, chat_type, now, now),
            )

    def _rows(self, sql: str, params: Iterable[Any]) -> list[EventRow]:
        with self._lock:
            return [EventRow(**dict(r)) for r in self._conn.execute(sql, tuple(params)).fetchall()]

    def recent_messages(
        self, account_id: str, chat_id: str, *, limit: int = 20, before_seq: Optional[int] = None
    ) -> list[EventRow]:
        """Latest messages of one chat, oldest first."""
        sql = (
            f"SELECT {_EVENT_COLS} FROM events WHERE account_id=? AND chat_id=?"
            " AND event_type='message'"
        )
        params: list[Any] = [account_id, chat_id]
        if before_seq is not None:
            sql += " AND seq<?"
            params.append(before_seq)
        sql += " ORDER BY seq DESC LIMIT ?"
        params.append(int(limit))
        return list(reversed(self._rows(sql, params)))

    def messages_after(self, account_id: str, chat_id: str, after_seq: int, *, limit: int = 500) -> list[EventRow]:
        return self._rows(
            f"SELECT {_EVENT_COLS} FROM events WHERE account_id=? AND chat_id=? AND event_type='message'"
            " AND seq>? ORDER BY seq ASC LIMIT ?",
            (account_id, chat_id, int(after_seq), int(limit)),
        )

    def count_messages_after(self, account_id: str, chat_id: str, after_seq: int) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) FROM events WHERE account_id=? AND chat_id=? AND event_type='message'"
                " AND direction='in' AND seq>?",
                (account_id, chat_id, int(after_seq)),
            ).fetchone()
        return int(row[0])

    def messages_on(
        self, account_id: str, chat_id: str, day: str, *, utc_offset_hours: float = 8.0
    ) -> list[EventRow]:
        """Read-only: every message of one chat on a local calendar day
        (``YYYY-MM-DD``). This is the query the nightly review calls."""
        tz = timezone(timedelta(hours=utc_offset_hours))
        start = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=tz)
        start_ms = int(start.timestamp() * 1000)
        end_ms = start_ms + 86_400_000
        return self._rows(
            f"SELECT {_EVENT_COLS} FROM events WHERE account_id=? AND chat_id=? AND event_type='message'"
            " AND ts_ms>=? AND ts_ms<? ORDER BY ts_ms ASC, seq ASC",
            (account_id, chat_id, start_ms, end_ms),
        )

    def find_message(self, account_id: str, chat_id: str, message_id: str) -> Optional[EventRow]:
        rows = self._rows(
            f"SELECT {_EVENT_COLS} FROM events WHERE account_id=? AND chat_id=? AND message_id=? LIMIT 1",
            (account_id, chat_id, message_id),
        )
        return rows[0] if rows else None

    def last_outbound(self, account_id: str, chat_id: str) -> Optional[EventRow]:
        rows = self._rows(
            f"SELECT {_EVENT_COLS} FROM events WHERE account_id=? AND chat_id=? AND direction='out'"
            " ORDER BY seq DESC LIMIT 1",
            (account_id, chat_id),
        )
        return rows[0] if rows else None

    def account_for_chat(self, chat_id: str) -> Optional[str]:
        """Which account last heard from this chat (routes outbound sends)."""
        with self._lock:
            row = self._conn.execute(
                "SELECT account_id FROM events WHERE chat_id=? AND direction='in' ORDER BY seq DESC LIMIT 1",
                (chat_id,),
            ).fetchone()
        return str(row[0]) if row else None

    def search(
        self, account_id: str, chat_id: str, terms: list[str], *, limit: int = 8,
        before_seq: Optional[int] = None,
    ) -> list[EventRow]:
        """Search one chat's journal. Any term may match; newest first."""
        terms = [t for t in dict.fromkeys(t.strip() for t in terms) if t]
        if not terms:
            return []
        seqs: dict[int, None] = {}
        with self._lock:
            long_terms = [t for t in terms if len(t) >= 3]
            short_terms = [t for t in terms if len(t) < 3]
            if self.fts_enabled and long_terms:
                match = " OR ".join('"' + t.replace('"', '""') + '"' for t in long_terms)
                try:
                    for row in self._conn.execute(
                        "SELECT e.seq FROM events_fts f JOIN events e ON e.seq=f.rowid"
                        " WHERE f.text MATCH ? AND e.account_id=? AND e.chat_id=?"
                        " AND e.event_type='message' ORDER BY e.seq DESC LIMIT ?",
                        (match, account_id, chat_id, limit * 4),
                    ):
                        seqs[int(row[0])] = None
                except sqlite3.OperationalError:
                    short_terms = terms
            elif long_terms:
                short_terms = terms
            for term in short_terms:
                like = "%" + term.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
                for row in self._conn.execute(
                    "SELECT seq FROM events WHERE account_id=? AND chat_id=? AND event_type='message'"
                    " AND text LIKE ? ESCAPE '\\' ORDER BY seq DESC LIMIT ?",
                    (account_id, chat_id, like, limit * 4),
                ):
                    seqs[int(row[0])] = None
        ordered = sorted(seqs, reverse=True)
        if before_seq is not None:
            ordered = [s for s in ordered if s < before_seq]
        ordered = ordered[:limit]
        if not ordered:
            return []
        marks = ",".join("?" for _ in ordered)
        rows = self._rows(
            f"SELECT {_EVENT_COLS} FROM events WHERE account_id=? AND chat_id=? AND seq IN ({marks})"
            " ORDER BY seq ASC",
            (account_id, chat_id, *ordered),
        )
        return rows

    def stats(self, account_id: str) -> dict[str, Any]:
        with self._lock:
            chats = [
                dict(r)
                for r in self._conn.execute(
                    "SELECT chat_id, chat_type, COUNT(*) AS events,"
                    " SUM(CASE WHEN event_type='message' AND direction='in' THEN 1 ELSE 0 END) AS inbound,"
                    " SUM(CASE WHEN direction='out' THEN 1 ELSE 0 END) AS outbound,"
                    " MIN(ts_ms) AS first_ms, MAX(ts_ms) AS last_ms"
                    " FROM events WHERE account_id=? GROUP BY chat_id, chat_type ORDER BY last_ms DESC",
                    (account_id,),
                ).fetchall()
            ]
            seen = [
                dict(r)
                for r in self._conn.execute(
                    "SELECT chat_id, chat_type, count, last_ms FROM seen_chats WHERE account_id=?"
                    " ORDER BY last_ms DESC",
                    (account_id,),
                ).fetchall()
            ]
        return {"chats": chats, "unjournaled": seen, "fts": self.fts_enabled, "path": str(self.path)}

    # ------------------------------------------------------------------
    # Identity registry
    # ------------------------------------------------------------------

    def get_person(self, account_id: str, user_id: str) -> Optional[dict[str, Any]]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM persons WHERE account_id=? AND user_id=?", (account_id, user_id)
            ).fetchone()
        return dict(row) if row else None

    def membership_status(self, account_id: str, group_id: str, user_id: str) -> Optional[str]:
        with self._lock:
            row = self._conn.execute(
                "SELECT status FROM memberships WHERE account_id=? AND group_id=? AND user_id=?",
                (account_id, group_id, user_id),
            ).fetchone()
        return str(row[0]) if row else None

    def identity(self, account_id: str, group_id: str, user_id: str) -> Optional[Identity]:
        """Approved identity of ``user_id`` in ``group_id``; ``None`` when the
        person is not an approved member of *this* group."""
        with self._lock:
            row = self._conn.execute(
                "SELECT p.local_name, p.role, p.relation, m.group_name FROM memberships m"
                " JOIN persons p ON p.account_id=m.account_id AND p.user_id=m.user_id"
                " WHERE m.account_id=? AND m.group_id=? AND m.user_id=? AND m.status='approved'"
                " AND p.status='active'",
                (account_id, group_id, user_id),
            ).fetchone()
        if not row:
            return None
        return Identity(user_id, row["local_name"], row["role"], row["relation"], row["group_name"])

    def group_members(self, account_id: str, group_id: str) -> list[Identity]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT m.user_id, p.local_name, p.role, p.relation, m.group_name FROM memberships m"
                " JOIN persons p ON p.account_id=m.account_id AND p.user_id=m.user_id"
                " WHERE m.account_id=? AND m.group_id=? AND m.status='approved' AND p.status='active'"
                " ORDER BY m.decided_ms ASC",
                (account_id, group_id),
            ).fetchall()
        return [Identity(r["user_id"], r["local_name"], r["role"], r["relation"], r["group_name"]) for r in rows]

    def get_request(self, account_id: str, group_id: str, user_id: str) -> Optional[MemberRequest]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM member_requests WHERE account_id=? AND group_id=? AND user_id=?",
                (account_id, group_id, user_id),
            ).fetchone()
        return MemberRequest(**dict(row)) if row else None

    def create_request(
        self, account_id: str, group_id: str, user_id: str, display_hint: str = ""
    ) -> tuple[MemberRequest, bool]:
        """Return ``(request, created)``. Idempotent per (group, user). The
        blocked message's text is never stored on the request."""
        with self._lock:
            existing = self.get_request(account_id, group_id, user_id)
            if existing:
                return existing, False
            for _ in range(20):
                code = "".join(secrets.choice(_CODE_ALPHABET) for _ in range(CODE_LENGTH))
                try:
                    self._conn.execute(
                        "INSERT INTO member_requests(account_id, group_id, user_id, code, display_hint,"
                        " created_ms) VALUES (?,?,?,?,?,?)",
                        (account_id, group_id, user_id, code, display_hint[:100], _now_ms()),
                    )
                    break
                except sqlite3.IntegrityError:
                    continue
            else:  # pragma: no cover - 31^6 codes, 20 tries
                raise RuntimeError("could not allocate an approval code")
            return self.get_request(account_id, group_id, user_id), True  # type: ignore[return-value]

    def pending_requests(self, account_id: str) -> list[MemberRequest]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM member_requests WHERE account_id=? ORDER BY created_ms ASC", (account_id,)
            ).fetchall()
        return [MemberRequest(**dict(r)) for r in rows]

    def request_by_code(self, account_id: str, code: str) -> Optional[MemberRequest]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM member_requests WHERE account_id=? AND code=?",
                (account_id, code.strip().upper()),
            ).fetchone()
        return MemberRequest(**dict(row)) if row else None

    def approve_request(
        self,
        account_id: str,
        code: str,
        *,
        decided_by: str,
        local_name: Optional[str] = None,
        role: Optional[str] = None,
        relation: Optional[str] = None,
        group_name: Optional[str] = None,
    ) -> Identity:
        """Approve a pending request.

        With ``local_name``/``role`` omitted the person must already be
        registered under this account (from another group); the existing
        identity is reused and only the membership is added.
        """
        with self._lock:
            request = self.request_by_code(account_id, code)
            if not request:
                raise KeyError("unknown approval code")
            person = self.get_person(account_id, request.user_id)
            now = _now_ms()
            if person is None:
                if not local_name or not role:
                    raise ValueError("local name and role are required for a new person")
                self._conn.execute(
                    "INSERT INTO persons(account_id, user_id, local_name, role, relation, note, status,"
                    " created_ms, updated_ms) VALUES (?,?,?,?,?,'','active',?,?)",
                    (account_id, request.user_id, local_name, role, relation or "", now, now),
                )
            elif role or relation is not None or (local_name and not group_name):
                # A full "name | role" approval on a known person updates the
                # global record; a bare name only sets this group's nickname.
                self._conn.execute(
                    "UPDATE persons SET local_name=?, role=?, relation=?, status='active', updated_ms=?"
                    " WHERE account_id=? AND user_id=?",
                    (
                        local_name if (local_name and role) else person["local_name"],
                        role or person["role"],
                        relation if relation is not None else person["relation"],
                        now, account_id, request.user_id,
                    ),
                )
            person = self.get_person(account_id, request.user_id)
            assert person is not None
            nickname = group_name or (local_name if local_name else person["local_name"])
            self._conn.execute(
                "INSERT INTO memberships(account_id, group_id, user_id, group_name, status, decided_by,"
                " decided_ms) VALUES (?,?,?,?,'approved',?,?) ON CONFLICT(account_id, group_id, user_id)"
                " DO UPDATE SET group_name=excluded.group_name, status='approved',"
                " decided_by=excluded.decided_by, decided_ms=excluded.decided_ms",
                (account_id, request.group_id, request.user_id, nickname, decided_by, now),
            )
            self._conn.execute(
                "DELETE FROM member_requests WHERE account_id=? AND code=?", (account_id, request.code)
            )
            identity = self.identity(account_id, request.group_id, request.user_id)
            assert identity is not None
            return identity

    def deny_request(self, account_id: str, code: str, *, decided_by: str) -> MemberRequest:
        with self._lock:
            request = self.request_by_code(account_id, code)
            if not request:
                raise KeyError("unknown approval code")
            self._conn.execute(
                "INSERT INTO memberships(account_id, group_id, user_id, group_name, status, decided_by,"
                " decided_ms) VALUES (?,?,?,'','denied',?,?) ON CONFLICT(account_id, group_id, user_id)"
                " DO UPDATE SET status='denied', decided_by=excluded.decided_by,"
                " decided_ms=excluded.decided_ms",
                (account_id, request.group_id, request.user_id, decided_by, _now_ms()),
            )
            self._conn.execute(
                "DELETE FROM member_requests WHERE account_id=? AND code=?", (account_id, request.code)
            )
            return request

    def register_member(
        self, account_id: str, group_id: str, user_id: str, *, local_name: str, role: str,
        relation: str = "", group_name: str = "", decided_by: str = "config",
    ) -> None:
        """Direct registration (used to seed the admin, and by tests)."""
        now = _now_ms()
        with self._lock:
            self._conn.execute(
                "INSERT INTO persons(account_id, user_id, local_name, role, relation, note, status,"
                " created_ms, updated_ms) VALUES (?,?,?,?,?,'','active',?,?)"
                " ON CONFLICT(account_id, user_id) DO UPDATE SET local_name=excluded.local_name,"
                " role=excluded.role, relation=excluded.relation, status='active',"
                " updated_ms=excluded.updated_ms",
                (account_id, user_id, local_name, role, relation, now, now),
            )
            self._conn.execute(
                "INSERT INTO memberships(account_id, group_id, user_id, group_name, status, decided_by,"
                " decided_ms) VALUES (?,?,?,?,'approved',?,?) ON CONFLICT(account_id, group_id, user_id)"
                " DO UPDATE SET group_name=excluded.group_name, status='approved',"
                " decided_by=excluded.decided_by, decided_ms=excluded.decided_ms",
                (account_id, group_id, user_id, group_name or local_name, decided_by, now),
            )

    # ------------------------------------------------------------------
    # Rolling summaries
    # ------------------------------------------------------------------

    def get_summary(self, account_id: str, group_id: str) -> Optional[dict[str, Any]]:
        with self._lock:
            row = self._conn.execute(
                "SELECT summary, upto_seq, updated_ms FROM group_summaries WHERE account_id=? AND group_id=?",
                (account_id, group_id),
            ).fetchone()
        return dict(row) if row else None

    def set_summary(self, account_id: str, group_id: str, summary: str, upto_seq: int) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO group_summaries(account_id, group_id, summary, upto_seq, updated_ms)"
                " VALUES (?,?,?,?,?) ON CONFLICT(account_id, group_id) DO UPDATE SET"
                " summary=excluded.summary, upto_seq=excluded.upto_seq, updated_ms=excluded.updated_ms",
                (account_id, group_id, summary, int(upto_seq), _now_ms()),
            )


    # ------------------------------------------------------------------
    # Settings and the external-agent turn queue
    # ------------------------------------------------------------------

    def get_setting(self, key: str) -> Optional[str]:
        with self._lock:
            row = self._conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return str(row[0]) if row else None

    def set_setting(self, key: str, value: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO settings(key, value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )

    def settings_with_prefix(self, prefix: str) -> list[tuple[str, str]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT key, value FROM settings WHERE substr(key, 1, ?) = ? ORDER BY key", (len(prefix), prefix)
            ).fetchall()
        return [(str(k), str(v)) for k, v in rows]

    def enqueue_turn(
        self, *, account_id: str, group_id: str, user_id: Optional[str], seq: Optional[int], sender: str,
        question: str, system_prompt: str, context: str,
    ) -> int:
        now = _now_ms()
        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO turns(account_id, group_id, user_id, seq, sender, question, system_prompt, context,"
                " status, created_ms, updated_ms) VALUES (?,?,?,?,?,?,?,?,'pending',?,?)",
                (account_id, group_id, user_id, seq, sender, question, system_prompt, context, now, now),
            )
            return int(cur.lastrowid)

    def get_turn(self, turn_id: int) -> Optional[dict[str, Any]]:
        with self._lock:
            row = self._conn.execute("SELECT * FROM turns WHERE id=?", (int(turn_id),)).fetchone()
        return dict(row) if row else None

    def list_turns(self, status: str = "pending", *, limit: int = 20) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM turns WHERE status=? ORDER BY id ASC LIMIT ?", (status, int(limit))
            ).fetchall()
        return [dict(r) for r in rows]

    def claim_turn(self, turn_id: int) -> bool:
        """Atomically move a turn pending → claimed; False if someone else has it."""
        with self._lock:
            cur = self._conn.execute(
                "UPDATE turns SET status='claimed', updated_ms=? WHERE id=? AND status='pending'",
                (_now_ms(), int(turn_id)),
            )
            return cur.rowcount == 1

    def finish_turn(self, turn_id: int, answer: Optional[str], status: str = "answered") -> bool:
        with self._lock:
            cur = self._conn.execute(
                "UPDATE turns SET status=?, answer=?, updated_ms=? WHERE id=? AND status IN ('pending','claimed')",
                (status, answer, _now_ms(), int(turn_id)),
            )
            return cur.rowcount == 1

    def expire_turns(self, older_than_ms: int) -> int:
        with self._lock:
            cur = self._conn.execute(
                "UPDATE turns SET status='expired', updated_ms=? WHERE status IN ('pending','claimed')"
                " AND created_ms<?",
                (_now_ms(), _now_ms() - int(older_than_ms)),
            )
            return cur.rowcount


_LINE_USER_ID = re.compile(r"^U[0-9a-f]{32}$")
_LINE_GROUP_ID = re.compile(r"^[CR][0-9a-f]{32}$")


def valid_user_id(value: Any) -> bool:
    return isinstance(value, str) and bool(_LINE_USER_ID.match(value))


def valid_group_id(value: Any) -> bool:
    return isinstance(value, str) and bool(_LINE_GROUP_ID.match(value))
