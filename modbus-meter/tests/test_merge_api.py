"""归并 API 集成测试：无需 Modbus 设备，直接走 ASGI。"""

from __future__ import annotations

import asyncio

import httpx
import pytest

from app.main import app


@pytest.fixture(autouse=True)
def _isolated_db(tmp_path, monkeypatch):
    monkeypatch.setenv("MERGE_DB_PATH", str(tmp_path / "merge.db"))


async def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    )


async def _batch(client: httpx.AsyncClient, **body):
    return await client.post("/merge/batches", json=body)


async def _query(client: httpx.AsyncClient, meter_id: str):
    return await client.get(f"/merge/meters/{meter_id}")


async def test_demo_backup_late_conflict_is_pending_review() -> None:
    async with await _client() as client:
        # 第 1 步：主路报数并报齐
        resp = await _batch(
            client,
            meter_id="M-1001",
            role="primary",
            readings=[{"second": 1760000000, "words": [1, 34464]}],
            progress=1760000000,
        )
        assert resp.status_code == 200
        assert resp.json()["inserted"] == 1

        # 备路尚未报齐：此时查询为空
        assert (await _query(client, "M-1001")).json()["items"] == []

        # 第 2 步：备路迟报异值并报齐
        resp = await _batch(
            client,
            meter_id="M-1001",
            role="backup",
            readings=[{"second": 1760000000, "words": [1, 34465]}],
            progress=1760000000,
        )
        assert resp.status_code == 200

        # 第 3 步：查到待核对记录，两值并列且不给入库计数
        data = (await _query(client, "M-1001")).json()
        assert data["total"] == 1
        assert data["items"] == data["readings"]
        assert data["ingest_count"] == 0
        assert data["conflict_count"] == 1
        item = data["items"][0]
        assert item["second"] == 1760000000
        assert item["status"] == "conflict"
        assert item["value"] is None
        assert item["primary_value"] == 100000
        assert item["backup_value"] == 100001
        assert item["ingest_count"] == 0
        assert item["needs_review"] is True


async def test_same_value_merges_into_one_and_single_source_is_annotated() -> None:
    async with await _client() as client:
        # 两路同秒同值 -> 合成一条
        assert (
            await _batch(
                client,
                meter_id="M-2",
                role="primary",
                readings=[{"second": 10, "words": [1, 2]}],
                progress=10,
            )
        ).status_code == 200
        assert (
            await _batch(
                client,
                meter_id="M-2",
                role="backup",
                readings=[{"second": 10, "words": [1, 2]}],
                progress=20,
            )
        ).status_code == 200

        # 第 12 秒只有主路有读数；主路随批报齐到 20
        assert (
            await _batch(
                client,
                meter_id="M-2",
                role="primary",
                readings=[{"second": 12, "words": [0, 65535]}],
                progress=20,
            )
        ).status_code == 200
        data = (await _query(client, "M-2")).json()
        items = {item["second"]: item for item in data["items"]}
        assert items[10]["status"] == "merged"
        assert items[10]["value"] == 65538
        assert items[10]["ingest_count"] == 1
        assert items[12]["status"] == "single_source"
        assert items[12]["value"] == 65535
        assert items[12]["source"] == "primary"
        assert items[12]["ingest_count"] == 1
        # 备路在 12 秒没有读数，也视为“已报齐且无读数”，可单路采用
        assert items[12]["backup_value"] is None
        assert data["ingest_count"] == 2


async def test_only_list_seconds_covered_by_both_progress() -> None:
    async with await _client() as client:
        assert (
            await _batch(
                client,
                meter_id="M-3",
                role="primary",
                readings=[
                    {"second": 5, "words": [0, 1]},
                    {"second": 9, "words": [0, 2]},
                ],
                progress=10,
            )
        ).status_code == 200
        # 备路只报齐到 5：第 9 秒尚不可查
        assert (
            await _batch(
                client,
                meter_id="M-3",
                role="backup",
                readings=[{"second": 5, "words": [0, 1]}],
                progress=5,
            )
        ).status_code == 200

        data = (await _query(client, "M-3")).json()
        assert [item["second"] for item in data["items"]] == [5]

        # 单独声明备路进度到 10（含）后，第 9 秒出现单路记录
        resp = await client.post(
            "/merge/progress",
            json={"meter_id": "M-3", "role": "backup", "through_second": 10},
        )
        assert resp.status_code == 200
        data = (await _query(client, "M-3")).json()
        assert [item["second"] for item in data["items"]] == [5, 9]
        assert data["items"][1]["source"] == "primary"


async def test_same_value_repeat_is_skipped() -> None:
    async with await _client() as client:
        body = {
            "meter_id": "M-4",
            "role": "primary",
            "readings": [{"second": 7, "words": [3, 4]}],
            "progress": 7,
        }
        first = (await _batch(client, **body)).json()
        assert first["inserted"] == 1
        assert first["skipped_duplicates"] == 0
        second = (await _batch(client, **body)).json()
        assert second["inserted"] == 0
        assert second["skipped_duplicates"] == 1

        # 完成范围内的同值重报也跳过，不视为“补报新读数”
        third = await _batch(
            client,
            meter_id="M-4",
            role="primary",
            readings=[{"second": 3, "words": [3, 4]}],
        )
        # 第 3 秒在已报齐范围但之前没有读数 -> 是补报新读数，拒收
        assert third.status_code == 409


async def test_same_second_different_value_rejects_whole_batch_atomically() -> None:
    async with await _client() as client:
        assert (
            await _batch(
                client,
                meter_id="M-5",
                role="primary",
                readings=[{"second": 1, "words": [0, 1]}],
                progress=1,
            )
        ).status_code == 200

        resp = await _batch(
            client,
            meter_id="M-5",
            role="primary",
            readings=[
                {"second": 2, "words": [0, 2]},  # 本可写入
                {"second": 1, "words": [0, 99]},  # 同秒异值
            ],
        )
        assert resp.status_code == 409
        assert "同秒出现异值" in resp.json()["detail"]

        # 整批回滚：第 2 秒不应存在
        data = (await _query(client, "M-5")).json()
        assert data["items"] == []


async def test_late_new_reading_within_reported_range_rejects_whole_batch() -> None:
    async with await _client() as client:
        assert (
            await _batch(
                client,
                meter_id="M-6",
                role="primary",
                readings=[{"second": 5, "words": [0, 5]}],
                progress=10,
            )
        ).status_code == 200

        # 备路先报齐到 10
        assert (
            await _batch(
                client,
                meter_id="M-6",
                role="backup",
                progress=10,
            )
        ).status_code == 200

        before = (await _query(client, "M-6")).json()
        assert before["total"] == 1

        # 主路补报第 8 秒（在已报齐范围内），整批拒收
        resp = await _batch(
            client,
            meter_id="M-6",
            role="primary",
            readings=[
                {"second": 11, "words": [0, 11]},  # 本可写入
                {"second": 8, "words": [0, 8]},  # 范围内补报
            ],
        )
        assert resp.status_code == 409
        assert "已报齐范围内" in resp.json()["detail"]

        after = (await _query(client, "M-6")).json()
        assert after == before  # 已可查结果不变


async def test_progress_regression_is_ignored() -> None:
    async with await _client() as client:
        assert (
            await _batch(
                client,
                meter_id="M-7",
                role="backup",
                progress=20,
            )
        ).status_code == 200
        # 较低进度：只进不退，保持原进度，不报错
        resp = await client.post(
            "/merge/progress",
            json={"meter_id": "M-7", "role": "backup", "through_second": 19},
        )
        assert resp.status_code == 200
        assert resp.json()["progress_through"] == 20
        assert resp.json()["progress_updated"] is False

        # 随批声明较低进度同样被忽略
        resp = await _batch(
            client,
            meter_id="M-7",
            role="backup",
            readings=[{"second": 25, "words": [0, 25]}],
            progress=19,
        )
        assert resp.status_code == 200
        assert resp.json()["progress_through"] == 20
        assert resp.json()["progress_updated"] is False

        # 同值重报不算回退
        resp = await client.post(
            "/merge/progress",
            json={"meter_id": "M-7", "role": "backup", "through_second": 20},
        )
        assert resp.status_code == 200
        assert resp.json()["progress_updated"] is False


async def test_concurrent_late_reports_do_not_change_queryable_result() -> None:
    async with await _client() as client:
        # 两路都报齐到 10，且第 5 秒有相同值
        for role in ("primary", "backup"):
            assert (
                await _batch(
                    client,
                    meter_id="M-8",
                    role=role,
                    readings=[{"second": 5, "words": [0, 5]}],
                    progress=10,
                )
            ).status_code == 200
        before = (await _query(client, "M-8")).json()
        assert before["items"][0]["status"] == "merged"

        # 并发尝试在已报齐的第 5 秒补报 / 改写：都应被拒，查询结果不变
        responses = await asyncio.gather(
            _batch(
                client,
                meter_id="M-8",
                role="primary",
                readings=[{"second": 5, "words": [0, 6]}],
            ),
            _batch(
                client,
                meter_id="M-8",
                role="backup",
                readings=[{"second": 5, "words": [0, 7]}],
            ),
        )
        assert all(resp.status_code == 409 for resp in responses)
        assert (await _query(client, "M-8")).json() == before


async def test_aliases_and_hi_lo_words() -> None:
    async with await _client() as client:
        # 常见字段别名
        resp = await client.post(
            "/merge",
            json={
                "meter": "M-9",
                "gateway_role": "main",
                "records": [{"poll_second": 3, "word_hi": 1, "word_lo": 2}],
                "reported_through": 3,
            },
        )
        assert resp.status_code == 200
        assert resp.json()["role"] == "primary"

        resp = await client.get("/merge/M-9")
        assert resp.status_code == 200
        assert resp.json()["items"] == []  # 只有主路报齐
