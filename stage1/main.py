"""
VerticalAgent Stage 1 —— 多 Provider 路由 + SSE 流式响应
========================================================

实现目标（来自 Implementation Plan 阶段 1）:
    ✅ 1. 多 LLM Provider 配置（DeepSeek + Qwen + Moonshot + ...）
    ✅ 2. 路由策略：按 model / provider 参数选择
    ✅ 3. 自动 fallback：主 provider 失败 → 链上下一家
    ✅ 4. SSE 流式响应：OpenAI SDK stream=True
    ✅ 5. Token 计量：每次请求 stdout 打日志
    ✅ 6. 熔断器：连续失败 N 次 → 跳过该 provider
    ✅ 7. 健康检查：GET /v1/models 列出 provider 状态
    ✅ 8. 错误兜底：4 类常见异常 + 友好提示

不在阶段 1 范围（明确不做）:
    ❌ 数据库（仍是内存 Map）
    ❌ 多租户隔离
    ❌ RAG / 知识库（阶段 2）
    ❌ Skills / MCP（阶段 3）
    ❌ 鉴权 / 登录
    ❌ vLLM 自建（用户决定用 API）
    ❌ 可观测性 / 监控（阶段 4）
    ❌ 计费 / 商业化（阶段 5）

技术栈:
    - Python 3.10+
    - FastAPI
    - OpenAI Python SDK（兼容 DeepSeek / Qwen / Moonshot / GPT）
    - Pydantic v2
    - uvicorn

与 stage0 的差异:
    - 多 Provider 路由（stage0 只有一个）
    - 熔断器（stage0 没有）
    - SSE 流式（stage0 是整段返回）
    - 错误分类更细（fallback 链 vs 单点失败）
"""

import os
import json
import time
import uuid
import logging
from enum import Enum
from collections import deque
from threading import Lock
from contextlib import asynccontextmanager
from typing import Optional, List, Dict, Any, Generator
from dataclasses import dataclass, field

from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from dotenv import load_dotenv
from openai import OpenAI, APIError, APIConnectionError, RateLimitError, APITimeoutError, AuthenticationError, BadRequestError

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


# ============== Session Manager ==============

class SessionManager:
    def __init__(self, max_history: int = 10):
        self.max_history = max_history
        self._sessions: Dict[str, deque] = {}
        self._meta: Dict[str, dict] = {}
        self._lock = Lock()

    def get_or_create(self, sid: Optional[str]):
        with self._lock:
            if sid is None or sid not in self._sessions:
                sid = str(uuid.uuid4())
                self._sessions[sid] = deque(maxlen=self.max_history)
                self._meta[sid] = {
                    "created_at": time.time(),
                    "request_count": 0,
                    "total_tokens": 0,
                }
            return sid, self._sessions[sid]

    def append(self, sid: str, message: dict):
        with self._lock:
            if sid in self._sessions:
                self._sessions[sid].append(message)
                self._meta[sid]["request_count"] += 1

    def get_history(self, sid: str) -> list:
        with self._lock:
            return list(self._sessions.get(sid, []))

    def update_tokens(self, sid: str, tokens: int):
        with self._lock:
            if sid in self._meta:
                self._meta[sid]["total_tokens"] += tokens

    def clear(self, sid: str):
        with self._lock:
            self._sessions.pop(sid, None)
            self._meta.pop(sid, None)

    def list_sessions(self):
        with self._lock:
            return [{"session_id": s, **m} for s, m in self._meta.items()]


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
sessions = SessionManager(max_history=settings.max_history)


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


# ============== FastAPI App ==============

@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("=" * 60)
    log.info(f"🚀 VerticalAgent Stage 1 启动")
    log.info(f"   路由链（按优先级）: {' → '.join(p.config.name for p in providers)}")
    for p in providers:
        log.info(f"   {p.config.name}: {p.config.base_url} ({p.config.model})")
    log.info(f"   监听: http://{settings.host}:{settings.port}")
    log.info(f"   Docs: http://{settings.host}:{settings.port}/docs")
    log.info("=" * 60)
    yield
    log.info("👋 Stage 1 关闭")


app = FastAPI(
    title="VerticalAgent Stage 1",
    description="多 Provider 路由 + SSE 流式响应 + 熔断器",
    version="0.2.0",
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


@app.get("/health")
def health():
    return {
        "status": "ok",
        "version": "0.2.0",
        "providers": [{"name": p.config.name, "state": p.breaker.get_stats()["state"]} for p in providers],
    }


@app.get("/v1/models")
def list_models():
    """列出所有 Provider 及其健康状态"""
    return {"providers": router.list_health()}


@app.post("/chat", response_model=ChatResponse)
def chat(req: ChatRequest):
    """非流式对话：自动 fallback 到下一个 provider"""
    sid, history = sessions.get_or_create(req.session_id)
    messages = []
    if req.system_prompt:
        messages.append({"role": "system", "content": req.system_prompt})
    messages.extend(list(history))
    messages.append({"role": "user", "content": req.message})

    result = router.chat_with_fallback(
        messages,
        provider_name=req.provider,
        model_name=req.model,
        temperature=req.temperature,
        max_tokens=settings.max_tokens,
    )

    if not result["ok"]:
        raise HTTPException(status_code=503, detail=result)

    sessions.append(sid, {"role": "user", "content": req.message})
    sessions.append(sid, {"role": "assistant", "content": result["reply"]})
    sessions.update_tokens(sid, result["usage"]["total_tokens"])

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
def chat_stream(req: ChatRequest):
    """SSE 流式对话：自动 fallback"""
    sid, history = sessions.get_or_create(req.session_id)
    messages = []
    if req.system_prompt:
        messages.append({"role": "system", "content": req.system_prompt})
    messages.extend(list(history))
    messages.append({"role": "user", "content": req.message})

    def event_generator():
        # 第一帧：发送 session_id + provider 选择
        primary = router.pick(req.provider, req.model)
        yield f"event: start\ndata: {json.dumps({'session_id': sid, 'primary_provider': primary.config.name if primary else None}, ensure_ascii=False)}\n\n"

        full_reply = ""
        provider_used = None
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
                provider_used = chunk.get("provider")
                # 完整事件：含 reasoning（用于历史会话展示）
                yield (
                    f"event: done\n"
                    f"data: {json.dumps({'usage': chunk.get('usage', {}), 'provider': provider_used, 'reasoning': chunk.get('reasoning', '')}, ensure_ascii=False)}\n\n"
                )
            else:
                # 推理过程 / 正常内容 各自独立 SSE event
                event_type = chunk.get("event", "delta")  # reasoning_delta / content_delta
                yield (
                    f"event: {event_type}\n"
                    f"data: {json.dumps({'delta': chunk.get('delta', ''), 'provider': chunk.get('provider')}, ensure_ascii=False)}\n\n"
                )

        if full_reply:
            sessions.append(sid, {"role": "user", "content": req.message})
            sessions.append(sid, {"role": "assistant", "content": full_reply})

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.get("/sessions/{sid}")
def get_session(sid: str):
    history = sessions.get_history(sid)
    if not history:
        raise HTTPException(404, "session not found")
    return {"session_id": sid, "messages": history, "count": len(history)}


@app.delete("/sessions/{sid}")
def clear_session(sid: str):
    sessions.clear(sid)
    return {"ok": True}


@app.get("/sessions")
def list_sessions():
    """列出所有 session，附带首条 user 消息作为标题预览"""
    result = []
    for meta in sessions.list_sessions():
        sid = meta["session_id"]
        history = sessions.get_history(sid)
        # 取首条 user 消息作为标题
        title = "新会话"
        for msg in history:
            if msg.get("role") == "user":
                content = msg.get("content", "").strip()
                if content:
                    title = content[:30] + ("…" if len(content) > 30 else "")
                break
        result.append({
            **meta,
            "title": title,
            "message_count": len(history),
        })
    # 按创建时间倒序
    result.sort(key=lambda s: s.get("created_at", 0), reverse=True)
    return {"count": len(result), "sessions": result}


# ============== 启动 ==============

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host=settings.host, port=settings.port, reload=False)
