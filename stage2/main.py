"""
VerticalAgent Stage 2 —— 会话持久化 + 历史回放
================================================

实现目标（来自 Implementation Plan 阶段 2 子任务 2A+2B）:
    ✅ 1. SQLite 持久化（SQLAlchemy 2.x ORM）
    ✅ 2. sessions + messages 两张表，含 reasoning / tokens / provider / latency 等元数据
    ✅ 3. 对话历史翻页（GET /sessions?page=1&page_size=20）
    ✅ 4. 对话历史关键词搜索（GET /sessions?q=keyword，命中 title 或消息内容）
    ✅ 5. 加载历史会话（GET /sessions/{sid}/messages）—— 完整 reasoning + tokens
    ✅ 6. 继承 stage1 全部能力：多 Provider 路由 + SSE 流式 + 熔断器 + Token 计量
    ✅ 7. 重启不丢数据（DB 文件落地 data/stage2.db）

不在 Stage 2 范围（明确不做）:
    ❌ 多租户隔离（Stage 2D）
    ❌ RAG / 知识库（Stage 2C 后续）
    ❌ Milvus / 向量库（10 万级再做）
    ❌ Skills / MCP（阶段 3）
    ❌ 鉴权 / 登录

技术栈:
    - Python 3.10+
    - FastAPI
    - SQLAlchemy 2.x ORM
    - SQLite（默认） / PostgreSQL（换 URL 即可）
    - OpenAI Python SDK（兼容 DeepSeek / Qwen / Moonshot / GPT）
    - Pydantic v2
    - uvicorn

与 stage1 的差异:
    + 数据层：内存 → SQLite（重启不丢）
    + 历史回放：仅当前会话 → 全部会话可查可搜可翻页
    + 元数据：仅消息内容 → + reasoning + tokens + provider + latency

启动:
    1. pip install -r requirements.txt
    2. cp .env.example .env && 编辑填 API Key
    3. python main.py
    4. 浏览器 http://localhost:8000
"""

import os
import json
import time
import logging
from enum import Enum
from collections import deque
from threading import Lock
from contextlib import asynccontextmanager
from typing import Optional, List, Generator
from dataclasses import dataclass

from fastapi import FastAPI, HTTPException, Depends, Query, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from dotenv import load_dotenv
from openai import OpenAI, APIError, APIConnectionError, RateLimitError, APITimeoutError, AuthenticationError, BadRequestError
from sqlalchemy.orm import Session as DbSession

from database import get_db, init_db, SessionLocal
from repository import SessionRepository, DocumentRepository, ChunkRepository
from embedding import EmbeddingClient, load_embedding_config_from_env
from rag import chunk_text, cosine_search, build_rag_prompt, parse_embedding
from document_parser import parse_document, is_supported_file, supported_extensions

# 自动加载 main.py 旁边的 .env（无论从哪个目录启动都能加载到）
load_dotenv(dotenv_path=os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

# ============== 配置 ==============

@dataclass
class ProviderConfig:
    name: str
    api_key: str
    base_url: str
    model: str
    timeout: int = 30
    failure_threshold: int = 5
    recovery_time: int = 60


class Settings:
    def __init__(self):
        self.max_history = int(os.getenv("MAX_HISTORY", "10"))
        self.host = os.getenv("HOST", "0.0.0.0")
        self.port = int(os.getenv("PORT", "8000"))
        self.default_temperature = float(os.getenv("DEFAULT_TEMPERATURE", "0.7"))
        self.max_tokens = int(os.getenv("MAX_TOKENS", "2048"))
        self.global_max_retries = int(os.getenv("GLOBAL_MAX_RETRIES", "2"))

        # 加载多 provider 配置
        # LLM_PROVIDERS=deepseek,qwen  →  列表中第一个为 primary
        provider_names = [n.strip() for n in os.getenv("LLM_PROVIDERS", "deepseek").split(",") if n.strip()]
        if not provider_names:
            raise ValueError("❌ LLM_PROVIDERS 未配置")

        self.providers_config: List[ProviderConfig] = []
        for name in provider_names:
            api_key = os.getenv(f"{name.upper()}_API_KEY", "")
            base_url = os.getenv(f"{name.upper()}_BASE_URL", "")
            model = os.getenv(f"{name.upper()}_MODEL", "")
            if not (api_key and base_url and model):
                raise ValueError(
                    f"❌ Provider '{name}' 配置不完整！\n"
                    f"需要在 .env 中设置：\n"
                    f"  {name.upper()}_API_KEY=...\n"
                    f"  {name.upper()}_BASE_URL=...\n"
                    f"  {name.upper()}_MODEL=..."
                )
            self.providers_config.append(ProviderConfig(
                name=name,
                api_key=api_key,
                base_url=base_url,
                model=model,
                timeout=int(os.getenv(f"{name.upper()}_TIMEOUT", "30")),
                failure_threshold=int(os.getenv(f"{name.upper()}_FAILURE_THRESHOLD", "5")),
                recovery_time=int(os.getenv(f"{name.upper()}_RECOVERY_TIME", "60")),
            ))


settings = Settings()

# ============== 日志 ==============

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger("stage1")

# ============== 熔断器 ==============

class BreakerState(Enum):
    CLOSED = "closed"      # 正常
    OPEN = "open"          # 熔断（拒绝请求）
    HALF_OPEN = "half_open"  # 半开（放一个探测请求）


class CircuitBreaker:
    """熔断器：CLOSED → OPEN → HALF_OPEN → CLOSED/OPEN"""

    def __init__(self, name: str, failure_threshold: int = 5, recovery_time: int = 60):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_time = recovery_time
        self.state = BreakerState.CLOSED
        self.recent: deque = deque(maxlen=20)  # 最近 20 次结果（0=成功，1=失败）
        self.opened_at: Optional[float] = None
        self.lock = Lock()

    def allow(self) -> bool:
        with self.lock:
            if self.state == BreakerState.CLOSED:
                return True
            if self.state == BreakerState.OPEN:
                # 检查是否到恢复时间
                if self.opened_at and time.time() - self.opened_at >= self.recovery_time:
                    self.state = BreakerState.HALF_OPEN
                    log.info(f"🔄 [{self.name}] OPEN → HALF_OPEN")
                    return True
                return False
            # HALF_OPEN：放行一个探测请求
            return True

    def record_success(self):
        with self.lock:
            self.recent.append(0)
            if self.state == BreakerState.HALF_OPEN:
                self.state = BreakerState.CLOSED
                self.opened_at = None
                self.recent.clear()
                log.info(f"✅ [{self.name}] HALF_OPEN → CLOSED")

    def record_failure(self):
        with self.lock:
            self.recent.append(1)
            if self.state == BreakerState.HALF_OPEN:
                self.state = BreakerState.OPEN
                self.opened_at = time.time()
                log.warning(f"🔴 [{self.name}] HALF_OPEN → OPEN")
            elif self.state == BreakerState.CLOSED:
                failures = sum(self.recent)
                if failures >= self.failure_threshold:
                    self.state = BreakerState.OPEN
                    self.opened_at = time.time()
                    log.warning(
                        f"🔴 [{self.name}] CLOSED → OPEN "
                        f"({failures} failures in {len(self.recent)} requests)"
                    )

    def get_stats(self) -> dict:
        with self.lock:
            if not self.recent:
                return {"state": self.state.value, "error_rate": 0, "samples": 0}
            return {
                "state": self.state.value,
                "error_rate": round(sum(self.recent) / len(self.recent), 3),
                "samples": len(self.recent),
            }


# ============== LLM Provider ==============

class LLMProvider:
    """单个 LLM Provider（DeepSeek / Qwen / Moonshot / GPT 等 OpenAI 兼容 API）"""

    def __init__(self, config: ProviderConfig, breaker: CircuitBreaker):
        self.config = config
        self.breaker = breaker
        self.client = OpenAI(api_key=config.api_key, base_url=config.base_url, timeout=config.timeout)

    # ----- 非流式 -----
    def chat(self, messages: list, temperature: float, max_tokens: int) -> dict:
        if not self.breaker.allow():
            return {
                "ok": False,
                "error_type": "circuit_open",
                "error_msg": f"[{self.config.name}] circuit breaker is OPEN",
                "provider": self.config.name,
            }

        try:
            start = time.time()
            resp = self.client.chat.completions.create(
                model=self.config.model,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
            )
            latency = time.time() - start
            self.breaker.record_success()
            log.info(
                f"✅ [{self.config.name}/{self.config.model}] "
                f"prompt={resp.usage.prompt_tokens} completion={resp.usage.completion_tokens} "
                f"latency={latency:.2f}s"
            )
            return {
                "ok": True,
                "reply": resp.choices[0].message.content,
                "model": self.config.model,
                "provider": self.config.name,
                "usage": {
                    "prompt_tokens": resp.usage.prompt_tokens,
                    "completion_tokens": resp.usage.completion_tokens,
                    "total_tokens": resp.usage.total_tokens,
                },
                "latency_ms": int(latency * 1000),
            }
        except (RateLimitError, APITimeoutError, APIConnectionError, APIError) as e:
            self.breaker.record_failure()
            log.warning(f"❌ [{self.config.name}] {type(e).__name__}: {e}")
            return {"ok": False, "error_type": type(e).__name__, "error_msg": str(e), "provider": self.config.name}
        except AuthenticationError as e:
            self.breaker.record_failure()
            log.error(f"🔑 [{self.config.name}] auth failed: {e}")
            return {"ok": False, "error_type": "auth_error", "error_msg": str(e), "provider": self.config.name}
        except BadRequestError as e:
            log.warning(f"⚠️ [{self.config.name}] bad request: {e}")
            return {"ok": False, "error_type": "bad_request", "error_msg": str(e), "provider": self.config.name}
        except Exception as e:
            self.breaker.record_failure()
            log.exception(f"❌ [{self.config.name}] unknown error: {e}")
            return {"ok": False, "error_type": "unknown", "error_msg": f"{type(e).__name__}: {e}", "provider": self.config.name}

    # ----- 流式 -----
    def chat_stream(self, messages: list, temperature: float, max_tokens: int) -> Generator[dict, None, None]:
        if not self.breaker.allow():
            yield {
                "ok": False,
                "error_type": "circuit_open",
                "error_msg": f"[{self.config.name}] circuit breaker is OPEN",
                "provider": self.config.name,
            }
            return

        try:
            stream = self.client.chat.completions.create(
                model=self.config.model,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
                stream=True,
            )
            self.breaker.record_success()
            full_reply = ""
            full_reasoning = ""  # 累积思考过程
            content_buffer = ""  # 用于检测跨 chunk 的 <think>...</think>
            completion_tokens = 0

            def emit_text(text):
                """辅助函数：发 content_delta"""
                nonlocal full_reply, completion_tokens
                if not text:
                    return
                full_reply += text
                completion_tokens += 1
                return {
                    "ok": True,
                    "is_final": False,
                    "event": "content_delta",
                    "delta": text,
                    "provider": self.config.name,
                    "model": self.config.model,
                }

            def emit_reasoning(text):
                """辅助函数：发 reasoning_delta"""
                nonlocal full_reasoning
                if not text:
                    return
                full_reasoning += text
                return {
                    "ok": True,
                    "is_final": False,
                    "event": "reasoning_delta",
                    "delta": text,
                    "provider": self.config.name,
                    "model": self.config.model,
                }

            for chunk in stream:
                if not chunk.choices:
                    continue
                delta = chunk.choices[0].delta

                # 兼容 OpenAI SDK 1.x：先尝试独立的 reasoning_content 字段（DeepSeek-R1 / o1 风格）
                delta_dict = {}
                try:
                    delta_dict = delta.model_dump(exclude_unset=False)
                except Exception:
                    pass

                standalone_reasoning = delta_dict.get("reasoning_content") or ""

                # 2) 处理 content（可能在 <think>...</think> 内）
                content = delta.content or ""

                # 如果有独立的 reasoning_content 字段，先发它
                if standalone_reasoning:
                    yield emit_reasoning(standalone_reasoning)

                # 把 content 加入 buffer，循环抽离 <think>...</think>
                if content:
                    content_buffer += content
                    # 处理 buffer 中所有完整的 <think>...</think>
                    while True:
                        ts = content_buffer.find("<think>")
                        if ts == -1:
                            break  # 没有 think 开始标签
                        te = content_buffer.find("</think>", ts)
                        if te == -1:
                            break  # 还没结束标签，等下个 chunk
                        # 抽取
                        before = content_buffer[:ts]
                        thinking = content_buffer[ts + len("<think>") : te]
                        after = content_buffer[te + len("</think>") :]
                        # 输出
                        if before:
                            yield emit_text(before)
                        if thinking:
                            yield emit_reasoning(thinking)
                        content_buffer = after

            # 流结束时，buffer 里残留的（可能是 <think> 没闭合的尾巴）作为 content 输出
            if content_buffer:
                yield emit_text(content_buffer)

            yield {
                "ok": True,
                "is_final": True,
                "event": "done",
                "delta": "",
                "reply": full_reply,
                "reasoning": full_reasoning,  # 完整思考内容
                "provider": self.config.name,
                "model": self.config.model,
                "usage": {
                    "prompt_tokens": 0,  # 流式 OpenAI SDK 通常不返回
                    "completion_tokens": completion_tokens,
                    "total_tokens": completion_tokens,
                },
            }
        except Exception as e:
            self.breaker.record_failure()
            log.warning(f"❌ [{self.config.name}] stream error: {type(e).__name__}: {e}")
            yield {"ok": False, "error_type": type(e).__name__, "error_msg": str(e), "provider": self.config.name}


# ============== Router ==============

class LLMRouter:
    """多 Provider 路由器：选 primary + 自动 fallback"""

    def __init__(self, providers: List[LLMProvider]):
        self.providers = providers  # 第一个为 primary
        self.by_name = {p.config.name: p for p in providers}

    def pick(self, provider_name: Optional[str], model_name: Optional[str]) -> Optional[LLMProvider]:
        if provider_name:
            return self.by_name.get(provider_name)
        if model_name:
            for p in self.providers:
                if p.config.model == model_name:
                    return p
        return self.providers[0]  # 默认 primary

    def build_chain(self, primary: LLMProvider) -> List[LLMProvider]:
        # 选中的放第一位，其他按配置顺序作为 fallback
        chain = [primary]
        for p in self.providers:
            if p is not primary:
                chain.append(p)
        return chain

    # ----- 非流式：自动 fallback -----
    def chat_with_fallback(self, messages, provider_name=None, model_name=None, **kwargs) -> dict:
        primary = self.pick(provider_name, model_name)
        chain = self.build_chain(primary)
        attempts = []
        for p in chain:
            result = p.chat(messages, **kwargs)
            attempts.append({
                "provider": p.config.name,
                "ok": result["ok"],
                "error_type": result.get("error_type"),
            })
            if result["ok"]:
                result["attempts"] = attempts
                result["fallback_used"] = len(attempts) > 1
                return result
            # 鉴权错误 / 内容错误不重试（无意义）
            if result.get("error_type") in ("auth_error", "bad_request"):
                break
        return {
            "ok": False,
            "error_type": "all_providers_failed",
            "error_msg": f"All {len(chain)} providers failed",
            "attempts": attempts,
        }

    # ----- 流式：自动 fallback -----
    def chat_stream_with_fallback(self, messages, provider_name=None, model_name=None, **kwargs) -> Generator[dict, None, None]:
        primary = self.pick(provider_name, model_name)
        chain = self.build_chain(primary)
        for i, p in enumerate(chain):
            used_fallback = i > 0
            failed = False
            for chunk in p.chat_stream(messages, **kwargs):
                if not chunk["ok"]:
                    failed = True
                    break
                if used_fallback:
                    chunk["fallback_used"] = True
                yield chunk
            if not failed:
                return
            # 鉴权错误不重试
            if i == 0:
                # 试 fallback
                continue
        yield {
            "ok": False,
            "error_type": "all_providers_failed",
            "error_msg": f"All {len(chain)} providers failed",
        }

    def list_health(self) -> list:
        return [
            {
                "provider": p.config.name,
                "model": p.config.model,
                **p.breaker.get_stats(),
            }
            for p in self.providers
        ]


# ============== 初始化 ==============

providers: List[LLMProvider] = []
for cfg in settings.providers_config:
    breaker = CircuitBreaker(
        name=cfg.name,
        failure_threshold=cfg.failure_threshold,
        recovery_time=cfg.recovery_time,
    )
    providers.append(LLMProvider(cfg, breaker))

router = LLMRouter(providers)


# ============== Pydantic Models ==============

class ChatRequest(BaseModel):
    message: str = Field(..., min_length=1, max_length=8000)
    session_id: Optional[str] = None
    model: Optional[str] = Field(None, description="指定模型，如 deepseek-chat")
    provider: Optional[str] = Field(None, description="指定 provider，如 deepseek")
    temperature: float = Field(default=settings.default_temperature, ge=0, le=2)
    system_prompt: Optional[str] = None
    stream: bool = Field(default=False)


class ChatResponse(BaseModel):
    session_id: str
    reply: str
    provider: str
    model: str
    usage: dict
    latency_ms: int
    attempts: list
    fallback_used: bool


# ============== Embedding 客户端 (Stage 2C) ==============

embedding_client: Optional[EmbeddingClient] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 初始化数据库（创建表 + 索引）
    init_db()
    # 初始化 embedding 客户端（如果 .env 配置了 EMBEDDING_PROVIDER）
    global embedding_client
    embed_cfg = load_embedding_config_from_env()
    if embed_cfg:
        embedding_client = EmbeddingClient(embed_cfg)
        log.info(f"✅ Embedding: {embed_cfg.provider}/{embed_cfg.model} @ {embed_cfg.base_url}")
    else:
        log.warning("⚠️ EMBEDDING_PROVIDER 未配置，RAG 检索不可用（仅能用 chat）")

    log.info("=" * 60)
    log.info("🚀 VerticalAgent Stage 2 启动（SQLite 持久化已启用）")
    log.info(f"   路由链（按优先级）: {' → '.join(p.config.name for p in providers)}")
    for p in providers:
        log.info(f"   {p.config.name}: {p.config.base_url} ({p.config.model})")
    if embedding_client:
        log.info(f"   Embedding: {embed_cfg.provider}/{embed_cfg.model}")
    log.info(f"   监听: http://{settings.host}:{settings.port}")
    log.info(f"   Docs: http://{settings.host}:{settings.port}/docs")
    log.info("=" * 60)
    yield
    log.info("👋 Stage 2 关闭")


app = FastAPI(
    title="VerticalAgent Stage 2",
    description="会话持久化 + 历史回放 + 多 Provider 路由 + SSE + 熔断器",
    version="0.3.0",
    lifespan=lifespan,
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
if os.path.isdir(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


# ============== API ==============

@app.get("/")
def index():
    p = os.path.join(STATIC_DIR, "index.html")
    if os.path.isfile(p):
        return FileResponse(p)
    return {"hint": "static/index.html not found", "docs": "/docs"}


@app.get("/favicon.ico")
def favicon():
    """静默 favicon 请求（浏览器自动请求，避免 404 噪音）"""
    # 1x1 透明 PNG（最小响应体）
    import base64
    png = base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII="
    )
    from fastapi.responses import Response
    return Response(content=png, media_type="image/png")


@app.get("/health")
def health(db: DbSession = Depends(get_db)):
    """健康检查 + 数据库统计"""
    repo = SessionRepository(db)
    return {
        "status": "ok",
        "version": "0.3.0",
        "providers": [{"name": p.config.name, "state": p.breaker.get_stats()["state"]} for p in providers],
        "db": {
            "sessions": repo.count_sessions(),
            "messages": repo.count_messages(),
        },
    }


@app.get("/v1/models")
def list_models():
    """列出所有 Provider 及其健康状态"""
    return {"providers": router.list_health()}


@app.post("/chat", response_model=ChatResponse)
def chat(req: ChatRequest, db: DbSession = Depends(get_db)):
    """非流式对话：自动 fallback 到下一个 provider"""
    repo = SessionRepository(db)
    sid, sess = repo.get_or_create(req.session_id, primary_provider=req.provider)
    history = repo.get_history_openai(sid, max_messages=settings.max_history)

    messages = []
    if req.system_prompt:
        messages.append({"role": "system", "content": req.system_prompt})
    messages.extend(history)
    messages.append({"role": "user", "content": req.message})

    result = router.chat_with_fallback(
        messages,
        provider_name=req.provider,
        model_name=req.model,
        temperature=req.temperature,
        max_tokens=settings.max_tokens,
    )

    if not result["ok"]:
        db.rollback()
        raise HTTPException(status_code=503, detail=result)

    # 写库：user + assistant 两条消息
    repo.append_message(
        sid=sid, role="user", content=req.message,
        provider=req.provider, model=req.model,
    )
    repo.append_message(
        sid=sid, role="assistant", content=result["reply"],
        provider=result["provider"], model=result["model"],
        prompt_tokens=result["usage"]["prompt_tokens"],
        completion_tokens=result["usage"]["completion_tokens"],
        total_tokens=result["usage"]["total_tokens"],
        is_fallback=result["fallback_used"],
        latency_ms=result["latency_ms"],
    )
    db.commit()

    log.info(
        f"📊 session={sid[:8]}... provider={result['provider']} model={result['model']} "
        f"total={result['usage']['total_tokens']} attempts={len(result['attempts'])} "
        f"fallback={'Y' if result['fallback_used'] else 'N'}"
    )

    return ChatResponse(
        session_id=sid,
        reply=result["reply"],
        provider=result["provider"],
        model=result["model"],
        usage=result["usage"],
        latency_ms=result["latency_ms"],
        attempts=result["attempts"],
        fallback_used=result["fallback_used"],
    )


@app.post("/chat/stream")
def chat_stream(req: ChatRequest, db: DbSession = Depends(get_db)):
    """SSE 流式对话：自动 fallback + 完成后写库"""
    repo = SessionRepository(db)
    sid, sess = repo.get_or_create(req.session_id, primary_provider=req.provider)
    # 关键：立刻 commit session 创建。否则 FastAPI 关闭 db 时会 rollback，
    # 导致 generator 内部的 db2 通过 sid 找不到 session，messages 因 FK 报错被吞。
    db.commit()
    history = repo.get_history_openai(sid, max_messages=settings.max_history)

    messages = []
    if req.system_prompt:
        messages.append({"role": "system", "content": req.system_prompt})
    messages.extend(history)
    messages.append({"role": "user", "content": req.message})

    def event_generator():
        # 整段用 try/except 包裹，任何异常都 yield error 事件给前端，
        # 避免 generator 崩溃导致 SSE 连接中断、前端"什么也收不到"
        try:
            # 第一帧：发送 session_id + provider 选择
            primary = router.pick(req.provider, req.model)
            if primary is None:
                # provider 名不存在（前端传了未配置的 provider）
                avail = [p.config.name for p in providers]
                yield f"event: error\ndata: {json.dumps({'error_type': 'unknown_provider', 'error_msg': f'provider={req.provider!r} 未配置；可用: {avail}'}, ensure_ascii=False)}\n\n"
                return
            yield f"event: start\ndata: {json.dumps({'session_id': sid, 'primary_provider': primary.config.name}, ensure_ascii=False)}\n\n"

            full_reply = ""
            full_reasoning = ""
            provider_used = None
            model_used = None
            fallback_used = False
            usage = {}
            for chunk in router.chat_stream_with_fallback(
                messages,
                provider_name=req.provider,
                model_name=req.model,
                temperature=req.temperature,
                max_tokens=settings.max_tokens,
            ):
                if not chunk.get("ok"):
                    yield f"event: error\ndata: {json.dumps(chunk, ensure_ascii=False)}\n\n"
                    return
                if chunk.get("is_final"):
                    full_reply = chunk.get("reply", "")
                    full_reasoning = chunk.get("reasoning", "")
                    provider_used = chunk.get("provider")
                    model_used = chunk.get("model")
                    usage = chunk.get("usage", {})
                    # 完整事件：含 reasoning（用于历史会话展示）
                    yield (
                        f"event: done\n"
                        f"data: {json.dumps({'usage': usage, 'provider': provider_used, 'reasoning': full_reasoning}, ensure_ascii=False)}\n\n"
                    )
                else:
                    # 推理过程 / 正常内容 各自独立 SSE event
                    event_type = chunk.get("event", "delta")  # reasoning_delta / content_delta
                    if chunk.get("fallback_used"):
                        fallback_used = True
                    yield (
                        f"event: {event_type}\n"
                        f"data: {json.dumps({'delta': chunk.get('delta', ''), 'provider': chunk.get('provider')}, ensure_ascii=False)}\n\n"
                    )

            # 完成后写库
            if full_reply:
                try:
                    db2 = SessionLocal()
                    try:
                        repo2 = SessionRepository(db2)
                        if not repo2.get(sid):
                            log.error(f"❌ session={sid[:8]}... 不存在，写库失败（endpoint commit 漏了？）")
                            return
                        repo2.append_message(
                            sid=sid, role="user", content=req.message,
                            provider=req.provider, model=req.model,
                        )
                        repo2.append_message(
                            sid=sid, role="assistant", content=full_reply,
                            reasoning=full_reasoning or None,
                            provider=provider_used, model=model_used,
                            prompt_tokens=usage.get("prompt_tokens", 0),
                            completion_tokens=usage.get("completion_tokens", 0),
                            total_tokens=usage.get("total_tokens", 0),
                            is_fallback=fallback_used,
                        )
                        db2.commit()
                        log.info(f"💾 session={sid[:8]}... 已持久化（{len(full_reply)} 字 + {len(full_reasoning)} 字思考）")
                    finally:
                        db2.close()
                except Exception as e:
                    log.error(f"❌ 持久化失败: {e}")
            else:
                log.warning(f"⚠️ 流式未产出 reply，跳过写库 session={sid[:8]}...")
        except Exception as e:
            # 任何 yield 抛异常都被这里捕获，保证 SSE 连接优雅关闭 + 报错给前端
            log.exception(f"❌ event_generator 异常: {e}")
            try:
                yield f"event: error\ndata: {json.dumps({'error_type': 'internal_error', 'error_msg': str(e)}, ensure_ascii=False)}\n\n"
            except Exception:
                pass

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.get("/sessions/{sid}")
def get_session(sid: str, db: DbSession = Depends(get_db)):
    """获取会话的精简信息（不含消息）"""
    repo = SessionRepository(db)
    s = repo.get(sid)
    if not s:
        raise HTTPException(404, "session not found")
    return s.to_meta_dict()


@app.get("/sessions/{sid}/messages")
def get_session_messages(
    sid: str,
    db: DbSession = Depends(get_db),
):
    """获取会话的完整消息列表（含 reasoning + tokens + provider）—— 侧栏点会话时调用"""
    repo = SessionRepository(db)
    s = repo.get(sid)
    if not s:
        raise HTTPException(404, "session not found")
    messages = repo.get_messages(sid)
    return {
        "session_id": sid,
        "title": s.title,
        "primary_provider": s.primary_provider,
        "message_count": len(messages),
        "messages": [m.to_dict() for m in messages],
    }


@app.delete("/sessions/{sid}")
def clear_session(sid: str, db: DbSession = Depends(get_db)):
    """删除会话（CASCADE 删 messages）"""
    repo = SessionRepository(db)
    if not repo.delete(sid):
        raise HTTPException(404, "session not found")
    db.commit()
    return {"ok": True}


@app.get("/sessions")
def list_sessions(
    page: int = Query(1, ge=1, description="页码，从 1 开始"),
    page_size: int = Query(20, ge=1, le=100, description="每页条数，1-100"),
    q: Optional[str] = Query(None, description="搜索关键词：命中 title 或任意消息内容"),
    db: DbSession = Depends(get_db),
):
    """列出所有会话，支持翻页 + 关键词搜索（侧栏用）"""
    repo = SessionRepository(db)
    items, total = repo.list_sessions(page=page, page_size=page_size, q=q)
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "count": len(items),
        "sessions": [s.to_meta_dict() for s in items],
    }


@app.patch("/sessions/{sid}")
def update_session(
    sid: str,
    payload: dict,
    db: DbSession = Depends(get_db),
):
    """修改会话属性（目前仅支持改 title）"""
    repo = SessionRepository(db)
    if "title" in payload:
        if not repo.update_title(sid, payload["title"]):
            raise HTTPException(404, "session not found")
    db.commit()
    return {"ok": True}


# ============== Stage 2C: RAG / 知识库 ==============

class DocumentCreate(BaseModel):
    """创建文档请求（直接传文本，不支持文件上传，简化起步）"""
    title: str = Field(..., min_length=1, max_length=255, description="文档标题")
    content: str = Field(..., min_length=1, description="文档正文")
    source: Optional[str] = Field(None, description="来源标识（文件名/URL/'manual'）")
    doc_type: str = Field("text", description="text / file / url")
    chunk_size: int = Field(500, ge=100, le=2000)
    overlap: int = Field(50, ge=0, le=200)


class DocumentOut(BaseModel):
    id: int
    title: str
    source: Optional[str]
    doc_type: str
    chunk_count: int
    embedding_provider: Optional[str]
    embedding_model: Optional[str]
    embedding_dim: Optional[int]
    created_at: float


class SearchRequest(BaseModel):
    query: str = Field(..., min_length=1, description="查询文本")
    top_k: int = Field(5, ge=1, le=20, description="返回条数")
    min_score: float = Field(0.0, ge=-1.0, le=1.0, description="最低相似度阈值")


class SearchHit(BaseModel):
    chunk_id: int
    document_id: int
    document_title: str
    chunk_index: int
    content: str
    score: float


@app.post("/documents", response_model=DocumentOut)
def create_document(req: DocumentCreate, db: DbSession = Depends(get_db)):
    """创建文档：切片 → embedding → 存 chunks"""
    if not embedding_client:
        raise HTTPException(503, "RAG 不可用：未配置 EMBEDDING_PROVIDER")

    # 1. 切片
    chunks = chunk_text(req.content, chunk_size=req.chunk_size, overlap=req.overlap)
    if not chunks:
        raise HTTPException(400, "文档切片为空")
    log.info(f"📄 文档切片: {len(chunks)} 段 (size={req.chunk_size}, overlap={req.overlap})")

    # 2. embedding
    vectors = embedding_client.embed(chunks)
    if not vectors or len(vectors) != len(chunks):
        raise HTTPException(502, f"Embedding API 失败或返回数量不匹配 ({len(chunks)} 期望, {len(vectors) if vectors else 0} 实际)")

    # 3. 写入 DB
    doc_repo = DocumentRepository(db)
    chunk_repo = ChunkRepository(db)
    doc = doc_repo.create(
        title=req.title,
        source=req.source,
        doc_type=req.doc_type,
        embedding_provider=embedding_client.config.provider,
        embedding_model=embedding_client.config.model,
        embedding_dim=embedding_client.dimension,
    )
    chunk_repo.add_chunks(
        document_id=doc.id,
        chunks=[(i, c, v) for i, (c, v) in enumerate(zip(chunks, vectors))],
    )
    doc_repo.update_chunk_count(doc.id, len(chunks))
    db.commit()

    log.info(f"💾 文档入库: id={doc.id} title={doc.title!r} chunks={len(chunks)}")
    return doc.to_dict()


@app.get("/documents")
def list_documents(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    q: Optional[str] = Query(None, description="搜索关键词"),
    db: DbSession = Depends(get_db),
):
    """列出文档库"""
    repo = DocumentRepository(db)
    items, total = repo.list_documents(page=page, page_size=page_size, q=q)
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "count": len(items),
        "documents": [d.to_dict() for d in items],
    }


@app.delete("/documents/{doc_id}")
def delete_document(doc_id: int, db: DbSession = Depends(get_db)):
    """删除文档（CASCADE 删 chunks）"""
    repo = DocumentRepository(db)
    if not repo.delete(doc_id):
        raise HTTPException(404, "document not found")
    db.commit()
    return {"ok": True}


# ============== 文件上传 + JSON chunks 导入 ==============

MAX_FILE_BYTES = 20 * 1024 * 1024  # 20MB


@app.post("/documents/upload")
async def upload_document(
    file: UploadFile = File(..., description="支持 .txt / .md / .docx / .pdf（≤20MB）"),
    title: str = Form("", description="文档标题（留空则用文件名）"),
    chunk_size: int = Form(500, ge=100, le=2000),
    overlap: int = Form(50, ge=0, le=200),
    db: DbSession = Depends(get_db),
):
    """
    上传文件到知识库。

    自动识别扩展名分发解析：
        .txt  → UTF-8 / GBK 解码
        .md   → UTF-8 + 解析 YAML frontmatter（title/tags/source）
        .docx → python-docx 提段落
        .pdf  → pypdf 提所有页文本

    流程：解析 → 切片 → embedding → 落库（与 POST /documents 一致）。
    """
    if not embedding_client:
        raise HTTPException(503, "RAG 不可用：未配置 EMBEDDING_PROVIDER")

    # 1. 文件名校验 + 大小限制
    filename = file.filename or "unnamed"
    if not is_supported_file(filename):
        exts = ", ".join(supported_extensions())
        raise HTTPException(400, f"不支持的文件类型。文件名: {filename!r}，支持: {exts}")

    content = await file.read()
    if len(content) > MAX_FILE_BYTES:
        raise HTTPException(413, f"文件过大：{len(content)} bytes（限制 {MAX_FILE_BYTES} bytes / 20MB）")
    if len(content) == 0:
        raise HTTPException(400, "文件为空")

    # 2. 解析文件
    try:
        text, metadata = parse_document(filename, content)
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception as e:
        log.exception(f"❌ 解析失败: {filename}")
        raise HTTPException(422, f"文件解析失败: {type(e).__name__}: {e}")

    if not text or not text.strip():
        raise HTTPException(422, "文件解析后内容为空（可能是扫描版 PDF 或图片型 DOCX）")

    # 3. 标题优先级：用户指定 > frontmatter title > 文件名
    if not title or not title.strip():
        title = metadata.get("title") or metadata.get("doc_title") or metadata.get("pdf_title") or filename

    # 4. source 优先级：frontmatter source > 文件名
    source = metadata.get("source") or filename

    log.info(f"📄 解析 {filename}: {len(content)} bytes → {len(text)} 字符 (metadata: {list(metadata.keys())})")

    # 5. 切片 + embedding + 落库（复用 DocumentRepository 逻辑）
    chunks = chunk_text(text, chunk_size=chunk_size, overlap=overlap)
    if not chunks:
        raise HTTPException(400, "文档切片为空")

    vectors = embedding_client.embed(chunks)
    if not vectors or len(vectors) != len(chunks):
        raise HTTPException(502, f"Embedding API 失败 ({len(chunks)} 期望, {len(vectors) if vectors else 0} 实际)")

    doc_repo = DocumentRepository(db)
    chunk_repo = ChunkRepository(db)
    doc = doc_repo.create(
        title=title.strip()[:255],
        source=source,
        doc_type=metadata.get("extension", "file").lstrip("."),
        embedding_provider=embedding_client.config.provider,
        embedding_model=embedding_client.config.model,
        embedding_dim=embedding_client.dimension,
        # 把解析元数据 + frontmatter 存到 metadata_json
        metadata={
            **metadata,
            "upload_filename": filename,
        },
    )
    chunk_repo.add_chunks(
        document_id=doc.id,
        chunks=[(i, c, v) for i, (c, v) in enumerate(zip(chunks, vectors))],
    )
    doc_repo.update_chunk_count(doc.id, len(chunks))
    db.commit()

    log.info(f"💾 文件入库: id={doc.id} title={doc.title!r} chunks={len(chunks)} dim={embedding_client.dimension}")
    return doc.to_dict()


class ChunkImport(BaseModel):
    """JSON chunks 导入的单条 chunk"""
    chunk_index: Optional[int] = None
    content: str = Field(..., min_length=1, description="chunk 文本内容")
    embedding: Optional[List[float]] = Field(None, description="可选的预计算 embedding（不传则批量调 API 补）")


class DocumentImport(BaseModel):
    """JSON chunks 批量导入请求"""
    title: str = Field(..., min_length=1, max_length=255)
    source: Optional[str] = Field(None, description="来源标识")
    doc_type: str = Field("imported", description="默认 'imported'")
    chunks: List[ChunkImport] = Field(..., min_length=1, description="已切片的 chunks 列表")


@app.post("/documents/import")
def import_chunks(req: DocumentImport, db: DbSession = Depends(get_db)):
    """
    导入已切片的 chunks（支持带预计算 embedding）。

    智能处理：
        - chunks 带 embedding → 校验维度（与当前 EMBEDDING_DIMENSION 一致），不一致则**忽略 embedding 并自动重算**
        - chunks 不带 embedding → 批量调 embedding API 计算
        - 所有 chunk 落地后立即可被 RAG 检索

    典型场景：从 LangChain / LlamaIndex / Cursor 等工具导出的 JSON 知识库。
    """
    if not embedding_client:
        raise HTTPException(503, "RAG 不可用：未配置 EMBEDDING_PROVIDER")

    expected_dim = embedding_client.dimension
    n = len(req.chunks)

    # 1. 分离：哪些 chunk 带有效 embedding，哪些需要补
    vectors: List[Optional[List[float]]] = [None] * n
    need_embed_indices: List[int] = []
    rejected_dim = 0

    for i, chunk in enumerate(req.chunks):
        if chunk.embedding is not None and len(chunk.embedding) > 0:
            if expected_dim and len(chunk.embedding) != expected_dim:
                log.warning(
                    f"⚠️ chunk[{i}] embedding 维度 {len(chunk.embedding)} ≠ 配置 {expected_dim}，"
                    f"将忽略并重新计算"
                )
                rejected_dim += 1
                need_embed_indices.append(i)
            else:
                vectors[i] = chunk.embedding
        else:
            need_embed_indices.append(i)

    # 2. 批量补 embedding
    if need_embed_indices:
        log.info(f"📊 导入 {n} chunks：{n - len(need_embed_indices)} 带有效 embedding，{len(need_embed_indices)} 需要计算")
        texts_to_embed = [req.chunks[i].content for i in need_embed_indices]
        new_vectors = embedding_client.embed(texts_to_embed)
        if not new_vectors or len(new_vectors) != len(need_embed_indices):
            raise HTTPException(502, f"Embedding API 失败（{len(need_embed_indices)} 期望, {len(new_vectors) if new_vectors else 0} 实际）")
        for idx, vec in zip(need_embed_indices, new_vectors):
            vectors[idx] = vec

    # 3. 写库
    doc_repo = DocumentRepository(db)
    chunk_repo = ChunkRepository(db)
    doc = doc_repo.create(
        title=req.title.strip(),
        source=req.source or "json_import",
        doc_type=req.doc_type,
        embedding_provider=embedding_client.config.provider,
        embedding_model=embedding_client.config.model,
        embedding_dim=embedding_client.dimension,
        metadata={
            "import_format": "json",
            "total_chunks": n,
            "rejected_dims": rejected_dim,
        },
    )
    chunks_data: List[tuple] = []
    for i, chunk in enumerate(req.chunks):
        idx = chunk.chunk_index if chunk.chunk_index is not None else i
        chunks_data.append((idx, chunk.content, vectors[i]))
    chunk_repo.add_chunks(document_id=doc.id, chunks=chunks_data)
    doc_repo.update_chunk_count(doc.id, n)
    db.commit()

    log.info(f"💾 JSON 导入入库: id={doc.id} title={doc.title!r} chunks={n} rejected_dim={rejected_dim}")
    return doc.to_dict()


@app.post("/search", response_model=List[SearchHit])
def search_chunks(req: SearchRequest, db: DbSession = Depends(get_db)):
    """纯检索接口（不调用 LLM，直接返回 top-k chunks）"""
    if not embedding_client:
        raise HTTPException(503, "RAG 不可用：未配置 EMBEDDING_PROVIDER")

    # 1. query embedding
    query_vec = embedding_client.embed_one(req.query)
    if not query_vec:
        raise HTTPException(502, "query embedding 失败")

    # 2. 加载所有 chunks（起步阶段全表扫描，10 万级以下 OK）
    # 用 raw SQL 批量加载 + 只取 embedding 字段，避免 ORM 实例化开销
    from sqlalchemy import text
    rows = db.execute(
        text("SELECT c.id, c.document_id, c.chunk_index, c.content, c.embedding, d.title "
             "FROM chunks c JOIN documents d ON c.document_id = d.id")
    ).fetchall()

    if not rows:
        return []

    # 3. cosine 检索
    vectors = [parse_embedding(r.embedding) for r in rows]
    hits = cosine_search(query_vec, vectors, top_k=req.top_k, min_score=req.min_score)

    # 4. 构造结果
    results = []
    for idx, score in hits:
        r = rows[idx]
        results.append(SearchHit(
            chunk_id=r.id,
            document_id=r.document_id,
            document_title=r.title,
            chunk_index=r.chunk_index,
            content=r.content,
            score=round(float(score), 4),
        ))
    return results


class RagChatRequest(BaseModel):
    """RAG 对话：检索 + 流式 LLM 回答"""
    message: str = Field(..., min_length=1)
    session_id: Optional[str] = None
    top_k: int = Field(5, ge=1, le=10)
    min_score: float = Field(0.0, ge=-1.0, le=1.0)
    provider: Optional[str] = None
    model: Optional[str] = None
    temperature: float = Field(0.7, ge=0, le=2)


@app.post("/chat/rag")
def chat_rag(req: RagChatRequest, db: DbSession = Depends(get_db)):
    """RAG 对话：检索 top-k → 注入 system prompt → LLM 流式回答"""
    if not embedding_client:
        raise HTTPException(503, "RAG 不可用：未配置 EMBEDDING_PROVIDER")

    # 1. 创建/获取 session
    sess_repo = SessionRepository(db)
    sid, sess = sess_repo.get_or_create(req.session_id, primary_provider=req.provider)
    db.commit()  # 提前 commit，避免 SSE 流期间被 rollback

    # 2. query embedding + 检索
    query_vec = embedding_client.embed_one(req.message)
    chunks_with_meta: list = []
    if query_vec:
        from sqlalchemy import text
        rows = db.execute(
            text("SELECT c.id, c.document_id, c.chunk_index, c.content, c.embedding, d.title "
                 "FROM chunks c JOIN documents d ON c.document_id = d.id")
        ).fetchall()
        vectors = [parse_embedding(r.embedding) for r in rows]
        hits = cosine_search(query_vec, vectors, top_k=req.top_k, min_score=req.min_score)
        for idx, score in hits:
            r = rows[idx]
            chunks_with_meta.append({
                "chunk_id": r.id,
                "document_id": r.document_id,
                "document_title": r.title,
                "chunk_index": r.chunk_index,
                "content": r.content,
                "score": float(score),
            })

    # 3. 构造 system prompt（含 RAG context）
    rag_system = build_rag_prompt(req.message, chunks_with_meta)

    # 4. 历史消息
    history = sess_repo.get_history_openai(sid, max_messages=settings.max_history)
    messages = [{"role": "system", "content": rag_system}]
    messages.extend(history)
    messages.append({"role": "user", "content": req.message})

    def event_generator():
        primary = router.pick(req.provider, req.model)
        yield f"event: start\ndata: {json.dumps({'session_id': sid, 'primary_provider': primary.config.name if primary else None, 'rag_chunks': len(chunks_with_meta), 'rag_sources': [{'doc': c['document_title'], 'score': round(c['score'], 3)} for c in chunks_with_meta]}, ensure_ascii=False)}\n\n"

        full_reply = ""
        full_reasoning = ""
        provider_used = None
        model_used = None
        fallback_used = False
        usage = {}
        for chunk in router.chat_stream_with_fallback(
            messages,
            provider_name=req.provider,
            model_name=req.model,
            temperature=req.temperature,
            max_tokens=settings.max_tokens,
        ):
            if not chunk.get("ok"):
                yield f"event: error\ndata: {json.dumps(chunk, ensure_ascii=False)}\n\n"
                return
            if chunk.get("is_final"):
                full_reply = chunk.get("reply", "")
                full_reasoning = chunk.get("reasoning", "")
                provider_used = chunk.get("provider")
                model_used = chunk.get("model")
                usage = chunk.get("usage", {})
                yield (
                    f"event: done\n"
                    f"data: {json.dumps({'usage': usage, 'provider': provider_used, 'reasoning': full_reasoning, 'rag_chunks': chunks_with_meta}, ensure_ascii=False)}\n\n"
                )
            else:
                event_type = chunk.get("event", "delta")
                if chunk.get("fallback_used"):
                    fallback_used = True
                yield (
                    f"event: {event_type}\n"
                    f"data: {json.dumps({'delta': chunk.get('delta', ''), 'provider': chunk.get('provider')}, ensure_ascii=False)}\n\n"
                )

        # 写库
        if full_reply:
            try:
                db2 = SessionLocal()
                try:
                    repo2 = SessionRepository(db2)
                    repo2.append_message(sid=sid, role="user", content=req.message)
                    repo2.append_message(
                        sid=sid, role="assistant", content=full_reply,
                        reasoning=full_reasoning or None,
                        provider=provider_used, model=model_used,
                        prompt_tokens=usage.get("prompt_tokens", 0),
                        completion_tokens=usage.get("completion_tokens", 0),
                        total_tokens=usage.get("total_tokens", 0),
                        is_fallback=fallback_used,
                    )
                    db2.commit()
                finally:
                    db2.close()
            except Exception as e:
                log.error(f"❌ RAG 持久化失败: {e}")

    return StreamingResponse(event_generator(), media_type="text/event-stream")


# ============== 启动 ==============

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host=settings.host, port=settings.port, reload=False)
