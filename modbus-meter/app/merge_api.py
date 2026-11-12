"""归并 API 的 HTTP 层。"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, HTTPException, Query
from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from .merge_service import (
    BatchInput,
    MergeConflict,
    MergeError,
    ProgressInput,
    ReadingInput,
    declare_progress,
    normalize_role,
    query_merged,
    submit_batch,
)

router = APIRouter(tags=["主备归并"])

_DEMO_METER = "M-1001"
_DEMO_SECOND = 1760000000


# ---------------------------------------------------------------------------
# 请求模型（字段名兼容常见写法，响应统一使用 canonical 名称）
# ---------------------------------------------------------------------------

_SECOND_ALIASES = AliasChoices(
    "second",
    "poll_second",
    "poll_sec",
    "utc_second",
    "utc_poll_second",
    "utc_poll_sec",
    "epoch_second",
    "timestamp",
    "ts",
    "time",
    "read_second",
    "sample_second",
    "秒",
)
_WORDS_ALIASES = AliasChoices(
    "words",
    "registers",
    "raw_words",
    "raw_registers",
    "register_words",
    "word_values",
    "寄存器字",
)
_METER_ALIASES = AliasChoices(
    "meter_id",
    "meterId",
    "meter",
    "meter_no",
    "meter_number",
    "meter_code",
    "table_id",
    "device_id",
    "point_id",
    "表号",
)
_ROLE_ALIASES = AliasChoices(
    "role", "gateway_role", "path_role", "role_type", "gateway", "角色"
)
_READINGS_ALIASES = AliasChoices(
    "readings", "records", "items", "data", "values", "读数"
)
_PROGRESS_ALIASES = AliasChoices(
    "progress",
    "progress_through",
    "reported_through",
    "reported_through_second",
    "reported_upto",
    "reported_up_to",
    "complete_through",
    "completed_through",
    "completed_second",
    "through_second",
    "progress_second",
    "reported_second",
    "through",
    "upto",
    "up_to",
    "second",
    "poll_second",
    "utc_second",
    "已报齐到",
)
_THROUGH_ALIASES = AliasChoices(
    "through_second",
    "progress",
    "progress_through",
    "reported_through",
    "reported_through_second",
    "reported_upto",
    "reported_up_to",
    "complete_through",
    "completed_through",
    "completed_second",
    "progress_second",
    "reported_second",
    "through",
    "upto",
    "up_to",
    "second",
    "poll_second",
    "utc_second",
    "已报齐到",
)


class ReadingIn(BaseModel):
    """一条读数：UTC 轮询秒 + 两个 16 位无符号寄存器字（高字在前）。"""

    model_config = ConfigDict(extra="ignore")

    second: int = Field(
        ...,
        ge=0,
        validation_alias=_SECOND_ALIASES,
        description="约定的 UTC 轮询秒（Unix 秒）",
        examples=[1760000000],
    )
    words: list[int] = Field(
        ...,
        min_length=2,
        max_length=2,
        validation_alias=_WORDS_ALIASES,
        description="两个 16 位无符号寄存器字，按高字在前解码",
        examples=[[1, 34464]],
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_words(cls, data: Any) -> Any:
        """兼容 word_hi/word_lo 或 {"hi": ..., "lo": ...} 形式。"""
        if not isinstance(data, dict):
            return data

        for key in (
            "words",
            "registers",
            "raw_words",
            "raw_registers",
            "register_words",
            "word_values",
            "寄存器字",
        ):
            if key not in data:
                continue
            words = data[key]
            if isinstance(words, dict):
                hi = words.get("hi", words.get("high", words.get("word_hi")))
                lo = words.get("lo", words.get("low", words.get("word_lo")))
                if hi is not None and lo is not None:
                    data = dict(data)
                    data[key] = [hi, lo]
            elif isinstance(words, tuple):
                data = dict(data)
                data[key] = list(words)

        if not any(
            key in data
            for key in (
                "words",
                "registers",
                "raw_words",
                "raw_registers",
                "register_words",
                "word_values",
                "寄存器字",
            )
        ):
            hi = data.get("word_hi", data.get("hi"))
            lo = data.get("word_lo", data.get("lo"))
            if hi is not None and lo is not None:
                data = dict(data)
                data["words"] = [hi, lo]
        return data

    @field_validator("words")
    @classmethod
    def _validate_words(cls, words: list[int]) -> list[int]:
        if len(words) != 2:
            raise ValueError("必须提供两个 16 位寄存器字")
        for word in words:
            if isinstance(word, bool) or not isinstance(word, int):
                raise ValueError("寄存器字必须是整数")
            if word < 0 or word > 0xFFFF:
                raise ValueError("寄存器字必须是无符号 16 位整数（0..65535）")
        return words


class BatchIn(BaseModel):
    """一批同一表号、同一网关角色的读数，可选附本路进度。"""

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    meter_id: str = Field(
        ...,
        min_length=1,
        validation_alias=_METER_ALIASES,
        description="表号",
        examples=[_DEMO_METER],
    )
    role: str = Field(
        ...,
        validation_alias=_ROLE_ALIASES,
        description="本批网关角色：primary（主）/ backup（备）",
        examples=["primary"],
    )
    readings: list[ReadingIn] = Field(
        default_factory=list,
        validation_alias=_READINGS_ALIASES,
        description="读数列表；可只声明进度而留空",
    )
    progress: int | None = Field(
        default=None,
        ge=0,
        validation_alias=_PROGRESS_ALIASES,
        description="本路已报齐到该 UTC 秒（含）；只能前进",
        examples=[_DEMO_SECOND],
    )

    @field_validator("meter_id", mode="before")
    @classmethod
    def _coerce_meter_id(cls, value: Any) -> Any:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return str(value)
        return value

    @field_validator("role")
    @classmethod
    def _normalize_role(cls, role: str) -> str:
        return normalize_role(role)

    @model_validator(mode="after")
    def _not_empty(self) -> "BatchIn":
        if not self.readings and self.progress is None:
            raise ValueError("批次至少要有读数或进度声明之一")
        return self


class ProgressValueIn(BaseModel):
    """单独声明进度时的秒值（路径版接口使用）。"""

    model_config = ConfigDict(extra="ignore")

    through_second: int = Field(..., ge=0, validation_alias=_THROUGH_ALIASES)


class ProgressIn(ProgressValueIn):
    """单独声明某表号、某角色的本路进度。"""

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    meter_id: str = Field(..., min_length=1, validation_alias=_METER_ALIASES)
    role: str = Field(..., validation_alias=_ROLE_ALIASES)

    @field_validator("meter_id", mode="before")
    @classmethod
    def _coerce_meter_id(cls, value: Any) -> Any:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return str(value)
        return value

    @field_validator("role")
    @classmethod
    def _normalize_role(cls, role: str) -> str:
        return normalize_role(role)


# ---------------------------------------------------------------------------
# 响应模型
# ---------------------------------------------------------------------------


class BatchResultOut(BaseModel):
    meter_id: str
    role: str
    received: int = Field(..., description="本批收到的读数条数")
    inserted: int = Field(..., description="新写入的读数条数")
    skipped_duplicates: int = Field(..., description="同秒同值重报、被跳过的条数")
    progress_through: int | None = Field(
        ..., description="提交后本路已报齐到的 UTC 秒（含）"
    )
    progress_updated: bool = Field(..., description="本次是否推进了进度")


class ProgressResultOut(BaseModel):
    meter_id: str
    role: str
    progress_through: int
    progress_updated: bool


class MergedItemOut(BaseModel):
    second: int = Field(..., description="UTC 轮询秒")
    status: str = Field(
        ...,
        description="merged（两值相同）/ single_source（仅一路有值）/ conflict（两值不同，待核对）",
    )
    value: int | None = Field(
        None, description="可入库的无符号计数；conflict 时为空"
    )
    source: str | None = Field(
        None, description="仅一路有值时注明来源：primary 或 backup"
    )
    primary_value: int | None = None
    backup_value: int | None = None
    ingest_count: int = Field(
        ..., description="本条可入库计数：合并/单路为 1，待核对为 0"
    )
    needs_review: bool = Field(..., description="是否需要人工核对")


class MergeQueryOut(BaseModel):
    meter_id: str
    items: list[MergedItemOut] = Field(
        ..., description="两路均已报齐且至少一路有读数的时点，按时间升序"
    )
    readings: list[MergedItemOut] = Field(
        ...,
        description="与 items 内容相同的同义字段，便于按读数列表直接消费",
    )
    total: int = Field(..., description="返回的时点总数")
    ingest_count: int = Field(..., description="可入库记录的合计计数")
    conflict_count: int = Field(..., description="两值不同、待核对的时点数")
    pending_review_count: int = Field(..., description="conflict_count 的同义字段")
    merged_count: int = Field(..., description="两值相同、已合成的时点数")
    single_source_count: int = Field(..., description="仅一路有值、已注明来源的时点数")


# ---------------------------------------------------------------------------
# 转换与错误处理
# ---------------------------------------------------------------------------


def _to_batch_input(req: BatchIn) -> BatchInput:
    return BatchInput(
        meter_id=req.meter_id,
        role=req.role,
        readings=[
            ReadingInput(second=r.second, word_hi=r.words[0], word_lo=r.words[1])
            for r in req.readings
        ],
        progress=req.progress,
    )


def _to_query_out(meter_id: str) -> MergeQueryOut:
    meter_id = meter_id.strip()
    result = query_merged(meter_id)
    items = [MergedItemOut(**item.__dict__) for item in result.items]
    return MergeQueryOut(
        meter_id=meter_id,
        items=items,
        readings=items,
        total=len(result.items),
        ingest_count=result.ingest_count,
        conflict_count=result.conflict_count,
        pending_review_count=result.conflict_count,
        merged_count=sum(1 for item in result.items if item.status == "merged"),
        single_source_count=sum(
            1 for item in result.items if item.status == "single_source"
        ),
    )


def _raise_merge_error(exc: MergeError) -> None:
    if isinstance(exc, MergeConflict):
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    raise HTTPException(status_code=422, detail=str(exc)) from exc


_DEMO_RESPONSE = {
    "meter_id": _DEMO_METER,
    "items": [
        {
            "second": _DEMO_SECOND,
            "status": "conflict",
            "value": None,
            "source": None,
            "primary_value": 100000,
            "backup_value": 100001,
            "ingest_count": 0,
            "needs_review": True,
        }
    ],
    "readings": [
        {
            "second": _DEMO_SECOND,
            "status": "conflict",
            "value": None,
            "source": None,
            "primary_value": 100000,
            "backup_value": 100001,
            "ingest_count": 0,
            "needs_review": True,
        }
    ],
    "total": 1,
    "ingest_count": 0,
    "conflict_count": 1,
    "pending_review_count": 1,
    "merged_count": 0,
    "single_source_count": 0,
}


# ---------------------------------------------------------------------------
# 批次提交（读数 + 可选进度，同一事务）
# ---------------------------------------------------------------------------

_BATCH_EXAMPLES = {
    "primary_batch": {
        "summary": "第 1 步：主路报数并声明已报齐",
        "description": (
            "主路在 UTC 秒 1760000000 读到 0x0001/0x86A0（高字在前 = 100000），"
            "并声明本路已报齐到该秒。"
        ),
        "value": {
            "meter_id": _DEMO_METER,
            "role": "primary",
            "readings": [{"second": _DEMO_SECOND, "words": [1, 34464]}],
            "progress": _DEMO_SECOND,
        },
    },
    "backup_late_conflict": {
        "summary": "第 2 步：备路迟报异值（得到待核对记录）",
        "description": (
            "备路稍后在同一个 UTC 秒读到 0x0001/0x86A1（= 100001）并报齐。"
            "随后查询 M-1001 可看到两值并列、ingest_count 为 0 的待核对记录。"
        ),
        "value": {
            "meter_id": _DEMO_METER,
            "role": "backup",
            "readings": [{"second": _DEMO_SECOND, "words": [1, 34465]}],
            "progress": _DEMO_SECOND,
        },
    },
    "backup_same_value": {
        "summary": "备路同秒同值：查询时合成一条",
        "description": "两路值相同，查询结果 status=merged、ingest_count=1。",
        "value": {
            "meter_id": _DEMO_METER,
            "role": "backup",
            "readings": [{"second": _DEMO_SECOND, "words": [1, 34464]}],
            "progress": _DEMO_SECOND,
        },
    },
    "progress_only": {
        "summary": "只推进进度（也可改用 /merge/progress）",
        "value": {
            "meter_id": _DEMO_METER,
            "role": "primary",
            "progress": _DEMO_SECOND + 1,
        },
    },
}


@router.post(
    "/merge/batches",
    response_model=BatchResultOut,
    summary="提交一批本路读数（可同时声明进度）",
    description=(
        "每批只属于一个表号和一个网关角色。同一路同秒同值重报被跳过；"
        "同秒异值或补报本路已报齐范围内的新读数，整批拒收（409）。"
        "进度只进不退，较低的声明不会推进进度。"
        "读数与进度在同一 SQLite 事务中提交。"
    ),
    responses={
        409: {"description": "同秒异值 / 范围内补报，整批拒收"},
        422: {"description": "请求体校验失败，或批次没有读数也没有进度"},
    },
)
@router.post("/merge/batch", include_in_schema=False)
@router.post("/merge/readings", include_in_schema=False)
@router.post("/merge/reports", include_in_schema=False)
@router.post("/merge/ingest", include_in_schema=False)
@router.post("/merge", include_in_schema=False)
@router.post("/batches", include_in_schema=False)
@router.post("/readings", include_in_schema=False)
def submit_batch_endpoint(
    req: BatchIn = Body(..., openapi_examples=_BATCH_EXAMPLES)
) -> BatchResultOut:
    try:
        result = submit_batch(_to_batch_input(req))
    except MergeError as exc:
        _raise_merge_error(exc)
    return BatchResultOut(**result.__dict__)


# ---------------------------------------------------------------------------
# 单独声明进度
# ---------------------------------------------------------------------------


_PROGRESS_EXAMPLES = {
    "declare_primary_progress": {
        "summary": "单独声明主路已报齐到该秒（含）",
        "value": {
            "meter_id": _DEMO_METER,
            "role": "primary",
            "through_second": _DEMO_SECOND,
        },
    }
}


@router.post(
    "/merge/progress",
    response_model=ProgressResultOut,
    summary="单独声明本路已报齐到哪一秒（含）",
    description="进度只进不退；相同值幂等，较低的值保持原进度、不会回退。",
)
@router.put(
    "/merge/progress",
    response_model=ProgressResultOut,
    include_in_schema=False,
)
@router.post("/merge/progress/declare", include_in_schema=False)
@router.post("/merge/declare-progress", include_in_schema=False)
@router.post("/progress", include_in_schema=False)
@router.put("/progress", include_in_schema=False)
def declare_progress_endpoint(
    req: ProgressIn = Body(..., openapi_examples=_PROGRESS_EXAMPLES)
) -> ProgressResultOut:
    try:
        result = declare_progress(
            ProgressInput(
                meter_id=req.meter_id,
                role=req.role,
                through_second=req.through_second,
            )
        )
    except MergeError as exc:
        _raise_merge_error(exc)
    return ProgressResultOut(**result.__dict__)


@router.post(
    "/merge/progress/{meter_id}/{role}",
    response_model=ProgressResultOut,
    include_in_schema=False,
)
@router.put(
    "/merge/progress/{meter_id}/{role}",
    response_model=ProgressResultOut,
    include_in_schema=False,
)
def declare_progress_path_endpoint(
    meter_id: str, role: str, req: ProgressValueIn
) -> ProgressResultOut:
    try:
        canonical_role = normalize_role(role)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    try:
        result = declare_progress(
            ProgressInput(
                meter_id=meter_id.strip(),
                role=canonical_role,
                through_second=req.through_second,
            )
        )
    except MergeError as exc:
        _raise_merge_error(exc)
    return ProgressResultOut(**result.__dict__)


# ---------------------------------------------------------------------------
# 按表号查询
# ---------------------------------------------------------------------------

_QUERY_DESCRIPTION = (
    "只列两路均已报齐到该秒（含）、且至少一路有读数的时点。"
    "两值相同合成一条（status=merged）；只有一路有值采用该值并注明来源"
    "（status=single_source）；两值不同并列各自的值待核对"
    "（status=conflict，value 为空、ingest_count=0）。\n\n"
    "**Swagger 演示「备路迟报后查到待核对记录」**：\n"
    "1. 在 `POST /merge/batches` 选择 `primary_batch` 样例执行；\n"
    "2. 再选择 `backup_late_conflict` 样例执行（同一表号、同一秒、异值）；\n"
    "3. 在 `GET /merge/meters/M-1001` 执行，即可看到 status=conflict、"
    "primary_value=100000、backup_value=100001、ingest_count=0 的记录。\n\n"
    "并发补报不会改变已经可查的时点：查询在单条 SQL 中读取同一份已提交快照，"
    "且已报齐范围内的新读数会被整批拒收。"
)


@router.get(
    "/merge/meters/{meter_id}",
    response_model=MergeQueryOut,
    summary="按表号查询已归并的两路读数",
    description=_QUERY_DESCRIPTION,
    responses={
        200: {
            "description": "查询结果",
            "content": {"application/json": {"example": _DEMO_RESPONSE}},
        }
    },
)
def query_merged_endpoint(meter_id: str) -> MergeQueryOut:
    return _to_query_out(meter_id)


@router.get(
    "/merge/meters/{meter_id}/merged",
    response_model=MergeQueryOut,
    include_in_schema=False,
)
@router.get("/merge/meter/{meter_id}", response_model=MergeQueryOut, include_in_schema=False)
@router.get("/merge/query/{meter_id}", response_model=MergeQueryOut, include_in_schema=False)
@router.get(
    "/merge/readings/{meter_id}", response_model=MergeQueryOut, include_in_schema=False
)
@router.get("/merged/{meter_id}", response_model=MergeQueryOut, include_in_schema=False)
@router.get("/readings/{meter_id}", response_model=MergeQueryOut, include_in_schema=False)
def query_merged_alias_endpoint(meter_id: str) -> MergeQueryOut:
    return _to_query_out(meter_id)


@router.get("/merge", response_model=MergeQueryOut, include_in_schema=False)
@router.get("/merged", response_model=MergeQueryOut, include_in_schema=False)
@router.get("/readings", response_model=MergeQueryOut, include_in_schema=False)
@router.get("/merge/readings", response_model=MergeQueryOut, include_in_schema=False)
def query_merged_by_query_endpoint(
    meter_id: str = Query(..., min_length=1, validation_alias=_METER_ALIASES)
) -> MergeQueryOut:
    return _to_query_out(meter_id)


@router.get(
    "/merge/{meter_id}",
    response_model=MergeQueryOut,
    include_in_schema=False,
)
def query_merged_dynamic_alias_endpoint(meter_id: str) -> MergeQueryOut:
    # 该别名放在所有静态 GET 路由之后；避免把 /merge/meters/... 当成表号。
    return _to_query_out(meter_id)
