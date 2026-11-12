"""归并数据持久化：同一 SQLite 数据库保存读数与进度。

每次写入使用 ``BEGIN IMMEDIATE`` 串行化，读数和进度在同一个事务中提交；
查询在单条 SQL 内同时读取进度与读数，保证看到同一份已提交快照。
"""

from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager
from typing import Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS readings (
    meter_id     TEXT    NOT NULL,
    role         TEXT    NOT NULL,
    second       INTEGER NOT NULL,
    value        INTEGER NOT NULL,
    word_hi      INTEGER NOT NULL,
    word_lo      INTEGER NOT NULL,
    PRIMARY KEY (meter_id, role, second)
);

CREATE INDEX IF NOT EXISTS readings_meter_second
    ON readings (meter_id, second);

CREATE TABLE IF NOT EXISTS progress (
    meter_id        TEXT    NOT NULL,
    role            TEXT    NOT NULL,
    through_second  INTEGER NOT NULL,
    PRIMARY KEY (meter_id, role)
);
"""

_INIT_LOCK = threading.Lock()
_INITIALIZED: set[str] = set()
_MEMORY_LOCK = threading.Lock()
_MEMORY_ANCHOR: sqlite3.Connection | None = None
_MEMORY_URI = "file:modbus-meter-merge?mode=memory&cache=shared"


def db_path() -> str:
    """数据库文件路径；测试或部署可通过环境变量覆盖。"""
    return os.getenv("MERGE_DB_PATH", os.getenv("MERGE_DB", "merge.db"))


def _configure(conn: sqlite3.Connection, *, file_based: bool) -> None:
    conn.execute("PRAGMA busy_timeout=30000")
    if file_based:
        # WAL：读取不会被正在提交的写事务阻塞，写事务之间仍串行。
        conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")


def _create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)


def _ensure_file_initialized(path: str) -> None:
    if path in _INITIALIZED and os.path.exists(path):
        return
    with _INIT_LOCK:
        if path in _INITIALIZED and os.path.exists(path):
            return
        is_uri = path.startswith("file:")
        if not is_uri:
            parent = os.path.dirname(os.path.abspath(path))
            if parent:
                os.makedirs(parent, exist_ok=True)
        conn = sqlite3.connect(path, uri=is_uri, timeout=30, isolation_level=None)
        try:
            _configure(conn, file_based=True)
            _create_schema(conn)
        finally:
            conn.close()
        _INITIALIZED.add(path)


def _ensure_memory_initialized() -> None:
    """``:memory:`` 下用共享缓存 URI，并保留一个锚连接防止库被回收。"""
    global _MEMORY_ANCHOR
    with _MEMORY_LOCK:
        if _MEMORY_ANCHOR is None:
            anchor = sqlite3.connect(_MEMORY_URI, uri=True, timeout=30, isolation_level=None)
            _configure(anchor, file_based=False)
            _create_schema(anchor)
            _MEMORY_ANCHOR = anchor


@contextmanager
def connect() -> Iterator[sqlite3.Connection]:
    """打开一个连接；调用方负责事务边界。"""
    path = db_path()
    if path == ":memory:":
        _ensure_memory_initialized()
        target, uri = _MEMORY_URI, True
    else:
        _ensure_file_initialized(path)
        target, uri = path, path.startswith("file:")

    conn = sqlite3.connect(target, uri=uri, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    try:
        yield conn
    finally:
        conn.close()
