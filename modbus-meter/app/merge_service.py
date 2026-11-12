"""主备两路读数的归并、进度声明与查询。

归并规则（与接口文档一致）：
- 同一 ``(表号, 角色, UTC 轮询秒)`` 只允许一个值；同值重报跳过，异值整批拒收；
- 进度按 ``(表号, 角色)`` 单调前进，声明“已报齐到某秒（含）”；
- 进度声明之后补报该范围内此前没有的读数，整批拒收；
- 读数与进度在同一事务中提交；
- 查询只返回两路都已报齐、且至少一路有读数的时点。
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Iterable

from .codec import decode_unsigned_pair
from .db import connect

PRIMARY = "primary"
BACKUP = "backup"

_ROLE_ALIASES = {
    "primary": PRIMARY,
    "main": PRIMARY,
    "master": PRIMARY,
    "host": PRIMARY,
    "active": PRIMARY,
    "leader": PRIMARY,
    "p": PRIMARY,
    "a": PRIMARY,
    "主": PRIMARY,
    "主路": PRIMARY,
    "主网关": PRIMARY,
    "主机": PRIMARY,
    "主用": PRIMARY,
    "primary_gateway": PRIMARY,
    "main_gateway": PRIMARY,
    "gateway_a": PRIMARY,
    "gw_a": PRIMARY,
    "backup": BACKUP,
    "standby": BACKUP,
    "secondary": BACKUP,
    "follower": BACKUP,
    "standby_gateway": BACKUP,
    "backup_gateway": BACKUP,
    "secondary_gateway": BACKUP,
    "b": BACKUP,
    "备": BACKUP,
    "备路": BACKUP,
    "备用": BACKUP,
    "副": BACKUP,
    "从": BACKUP,
    "gateway_b": BACKUP,
    "gw_b": BACKUP,
}


def normalize_role(role: str) -> str:
    """把主备角色的常见写法归一为 ``primary`` / ``backup``。"""
    key = str(role).strip().lower()
    try:
        return _ROLE_ALIASES[key]
    except KeyError as exc:
        raise ValueError(
            f"未知角色 {role!r}；请使用 primary/backup（也接受 main/standby）"
        ) from exc


class MergeError(Exception):
    """归并业务错误基类。"""

    code = "merge_error"


class MergeConflict(MergeError):
    """整批拒收：同秒异值、进度回退或补报已报齐范围。"""

    code = "merge_conflict"


class MergeRequestError(MergeError):
    """请求本身没有可提交的内容。"""

    code = "merge_request_error"


@dataclass(frozen=True)
class ReadingInput:
    second: int
    word_hi: int
    word_lo: int

    @property
    def value(self) -> int:
        return decode_unsigned_pair(self.word_hi, self.word_lo)


@dataclass(frozen=True)
class BatchInput:
    meter_id: str
    role: str
    readings: Iterable[ReadingInput]
    progress: int | None = None


@dataclass(frozen=True)
class ProgressInput:
    meter_id: str
    role: str
    through_second: int


@dataclass(frozen=True)
class BatchResult:
    meter_id: str
    role: str
    received: int
    inserted: int
    skipped_duplicates: int
    progress_through: int | None
    progress_updated: bool


@dataclass(frozen=True)
class ProgressResult:
    meter_id: str
    role: str
    progress_through: int
    progress_updated: bool


@dataclass(frozen=True)
class MergedRecord:
    second: int
    status: str
    value: int | None
    source: str | None
    primary_value: int | None
    backup_value: int | None
    ingest_count: int
    needs_review: bool


@dataclass(frozen=True)
class QueryResult:
    meter_id: str
    items: list[MergedRecord]

    @property
    def ingest_count(self) -> int:
        return sum(item.ingest_count for item in self.items)

    @property
    def conflict_count(self) -> int:
        return sum(1 for item in self.items if item.status == "conflict")


def _current_progress(
    conn: sqlite3.Connection, meter_id: str, role: str
) -> int | None:
    row = conn.execute(
        "SELECT through_second FROM progress WHERE meter_id = ? AND role = ?",
        (meter_id, role),
    ).fetchone()
    return None if row is None else int(row["through_second"])


def _existing_value(
    conn: sqlite3.Connection, meter_id: str, role: str, second: int
) -> int | None:
    row = conn.execute(
        "SELECT value FROM readings WHERE meter_id = ? AND role = ? AND second = ?",
        (meter_id, role, second),
    ).fetchone()
    return None if row is None else int(row["value"])


def _rollback(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("ROLLBACK")
    except sqlite3.OperationalError:
        # 事务可能已因错误自动结束；无需再回滚。
        pass


def submit_batch(batch: BatchInput) -> BatchResult:
    """在同一事务内校验、写入读数并推进进度；任一冲突则整批回滚。"""
    meter_id = batch.meter_id
    role = batch.role
    readings = list(batch.readings)

    if not readings and batch.progress is None:
        raise MergeRequestError("批次既没有读数也没有进度声明，无事可做")

    with connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            old_progress = _current_progress(conn, meter_id, role)

            inserted = 0
            skipped = 0
            # 同一批内也会出现重复读数，先按秒归并后再逐条比对。
            seen_in_batch: dict[int, int] = {}

            for reading in readings:
                value = reading.value
                second = reading.second

                if second in seen_in_batch:
                    if seen_in_batch[second] != value:
                        raise MergeConflict(
                            f"同一路同秒出现异值：表 {meter_id!r} 角色 {role} "
                            f"UTC 秒 {second} 在本批内既有值 {seen_in_batch[second]} "
                            f"又有值 {value}，整批拒收"
                        )
                    skipped += 1
                    continue
                seen_in_batch[second] = value

                existing = _existing_value(conn, meter_id, role, second)
                if existing is not None:
                    if existing != value:
                        raise MergeConflict(
                            f"同一路同秒出现异值：表 {meter_id!r} 角色 {role} "
                            f"UTC 秒 {second} 已报值 {existing}，本批报 {value}，整批拒收"
                        )
                    # 同秒同值的重报：幂等跳过。
                    skipped += 1
                    continue

                # 没有旧值，但该秒已被本路声明为“已报齐”：属于范围内补报，拒收。
                if old_progress is not None and second <= old_progress:
                    raise MergeConflict(
                        f"补报本路先前已报齐范围内的新读数：表 {meter_id!r} "
                        f"角色 {role} 已报齐到 {old_progress}（含），"
                        f"本批补报 UTC 秒 {second}，整批拒收"
                    )

                conn.execute(
                    "INSERT INTO readings "
                    "(meter_id, role, second, value, word_hi, word_lo) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        meter_id,
                        role,
                        second,
                        value,
                        reading.word_hi,
                        reading.word_lo,
                    ),
                )
                inserted += 1

            progress_updated = False
            new_progress = old_progress
            if batch.progress is not None:
                if old_progress is None:
                    conn.execute(
                        "INSERT INTO progress (meter_id, role, through_second) "
                        "VALUES (?, ?, ?)",
                        (meter_id, role, batch.progress),
                    )
                    progress_updated = True
                    new_progress = batch.progress
                elif batch.progress > old_progress:
                    conn.execute(
                        "UPDATE progress SET through_second = ? "
                        "WHERE meter_id = ? AND role = ?",
                        (batch.progress, meter_id, role),
                    )
                    progress_updated = True
                    new_progress = batch.progress

            conn.execute("COMMIT")
        except BaseException:
            _rollback(conn)
            raise

    return BatchResult(
        meter_id=meter_id,
        role=role,
        received=len(readings),
        inserted=inserted,
        skipped_duplicates=skipped,
        progress_through=new_progress,
        progress_updated=progress_updated,
    )


def declare_progress(progress: ProgressInput) -> ProgressResult:
    """单独声明本路已报齐到某秒（含）；进度只进不退。"""
    meter_id = progress.meter_id
    role = progress.role
    second = progress.through_second

    with connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            old_progress = _current_progress(conn, meter_id, role)

            updated = False
            current = old_progress
            if old_progress is None:
                conn.execute(
                    "INSERT INTO progress (meter_id, role, through_second) "
                    "VALUES (?, ?, ?)",
                    (meter_id, role, second),
                )
                updated = True
                current = second
            elif second > old_progress:
                conn.execute(
                    "UPDATE progress SET through_second = ? "
                    "WHERE meter_id = ? AND role = ?",
                    (second, meter_id, role),
                )
                updated = True
                current = second
            # second <= old_progress：进度只进不退，保持原值（相同值也幂等）。

            conn.execute("COMMIT")
        except BaseException:
            _rollback(conn)
            raise

    assert current is not None
    return ProgressResult(
        meter_id=meter_id,
        role=role,
        progress_through=current,
        progress_updated=updated,
    )


_QUERY_SQL = """
SELECT
    s.second          AS second,
    p.value           AS primary_value,
    b.value           AS backup_value
FROM (
    SELECT DISTINCT second
    FROM readings
    WHERE meter_id = :meter_id
) AS s
JOIN progress AS pp
  ON pp.meter_id = :meter_id
 AND pp.role = 'primary'
 AND pp.through_second >= s.second
JOIN progress AS pb
  ON pb.meter_id = :meter_id
 AND pb.role = 'backup'
 AND pb.through_second >= s.second
LEFT JOIN readings AS p
  ON p.meter_id = :meter_id
 AND p.role = 'primary'
 AND p.second = s.second
LEFT JOIN readings AS b
  ON b.meter_id = :meter_id
 AND b.role = 'backup'
 AND b.second = s.second
WHERE p.value IS NOT NULL OR b.value IS NOT NULL
ORDER BY s.second
"""


def query_merged(meter_id: str) -> QueryResult:
    """按表号查询两路均报齐的时点，并给出合并 / 单路 / 待核对结果。

    整段读取在一个 SQL 语句内完成，进度和读数来自同一份已提交快照；
    已经可查的时点不会被并发的进度推进或补报改变。
    """
    with connect() as conn:
        rows = conn.execute(_QUERY_SQL, {"meter_id": meter_id}).fetchall()

    items: list[MergedRecord] = []
    for row in rows:
        second = int(row["second"])
        primary = row["primary_value"]
        backup = row["backup_value"]
        primary = None if primary is None else int(primary)
        backup = None if backup is None else int(backup)

        if primary is not None and backup is not None:
            if primary == backup:
                items.append(
                    MergedRecord(
                        second=second,
                        status="merged",
                        value=primary,
                        source=None,
                        primary_value=primary,
                        backup_value=backup,
                        ingest_count=1,
                        needs_review=False,
                    )
                )
            else:
                items.append(
                    MergedRecord(
                        second=second,
                        status="conflict",
                        value=None,
                        source=None,
                        primary_value=primary,
                        backup_value=backup,
                        ingest_count=0,
                        needs_review=True,
                    )
                )
        else:
            source = PRIMARY if primary is not None else BACKUP
            value = primary if primary is not None else backup
            items.append(
                MergedRecord(
                    second=second,
                    status="single_source",
                    value=value,
                    source=source,
                    primary_value=primary,
                    backup_value=backup,
                    ingest_count=1,
                    needs_review=False,
                )
            )

    return QueryResult(meter_id=meter_id, items=items)
