"""Per-group markdown documents: the knowledge base and the pinned facts.

Both are plain files a person edits by hand. They are loaded when the
gateway starts and reloaded whenever the file changes on disk (checked by
mtime and size on each use) or when an admin sends ``/kb reload``.

Knowledge-base format — markdown with an optional front-matter block::

    ---
    kb_version: 2026-11-25.1
    title: Disney Adventure 家族行程
    ---
    ## 行程
    ...

``kb_version`` is free text chosen by the editor; the loader also records
a content hash, so every answer can be traced to an exact file state.
Only put information here that every member of the group may see.
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from .store import default_data_dir

logger = logging.getLogger(__name__)

MAX_DOC_CHARS = 60_000
_SAFE = re.compile(r"[^A-Za-z0-9._-]+")
_FRONT_MATTER = re.compile(r"\A---\s*\n(.*?)\n---\s*\n", re.DOTALL)


@dataclass
class Document:
    path: Path
    body: str = ""
    meta: dict[str, str] = field(default_factory=dict)
    sha: str = ""
    exists: bool = False
    error: str = ""
    _stamp: tuple[int, int] = (0, 0)

    @property
    def version(self) -> str:
        label = self.meta.get("kb_version") or self.meta.get("version") or ""
        return f"{label} ({self.sha})" if label else self.sha


def _safe_part(value: str) -> str:
    return _SAFE.sub("_", value).strip("._") or "x"


def group_dir(account_id: str, group_key: str, base: Optional[Path] = None) -> Path:
    return (base or default_data_dir()) / "groups" / _safe_part(account_id) / _safe_part(group_key)


def parse_document(text: str) -> tuple[dict[str, str], str]:
    meta: dict[str, str] = {}
    match = _FRONT_MATTER.match(text)
    if match:
        for line in match.group(1).splitlines():
            if ":" in line and not line.lstrip().startswith("#"):
                key, value = line.split(":", 1)
                meta[key.strip().lower()] = value.strip().strip("\"'")
        text = text[match.end():]
    return meta, text.strip()


class DocumentCache:
    """Loads and re-checks documents. A file that becomes unreadable or
    oversized keeps serving its last good content and reports the error."""

    def __init__(self) -> None:
        self._docs: dict[Path, Document] = {}

    def get(self, path: Path, *, force: bool = False) -> Document:
        path = Path(os.path.expanduser(str(path)))
        doc = self._docs.get(path)
        try:
            st = path.stat()
        except FileNotFoundError:
            if doc is None or doc.exists:
                doc = Document(path=path)
                self._docs[path] = doc
            return doc
        except OSError as exc:
            doc = doc or Document(path=path)
            doc.error = f"stat failed: {exc.__class__.__name__}"
            self._docs[path] = doc
            return doc
        stamp = (st.st_mtime_ns, st.st_size)
        if doc is not None and doc.exists and doc._stamp == stamp and not force:
            return doc
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as exc:
            doc = doc or Document(path=path)
            doc.error = f"read failed: {exc.__class__.__name__}"
            doc._stamp = stamp
            self._docs[path] = doc
            logger.error("[line_platform] cannot read %s: %s", path, doc.error)
            return doc
        meta, body = parse_document(text)
        if len(body) > MAX_DOC_CHARS:
            doc = doc or Document(path=path)
            doc.error = f"too large ({len(body)} chars > {MAX_DOC_CHARS}); keeping previous version"
            doc._stamp = stamp
            self._docs[path] = doc
            logger.error("[line_platform] %s %s", path, doc.error)
            return doc
        new = Document(
            path=path, body=body, meta=meta,
            sha=hashlib.sha256(text.encode("utf-8")).hexdigest()[:12], exists=True, _stamp=stamp,
        )
        if doc is None or doc.sha != new.sha:
            logger.info("[line_platform] loaded %s version=%s chars=%d", path.name, new.version, len(body))
        self._docs[path] = new
        return new
