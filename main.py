"""
阶段 0 完整可跑代码 —— VerticalAgent MVP Backend
====================================================

实现目标（来自 Implementation Plan 阶段 0）:
    ✅ 1. Web 界面可对话（30 分钟搭出 Demo）
    ✅ 2. 多轮对话（5 轮上下文不丢）
    ✅ 3. LLM API 调用（DeepSeek / Qwen / OpenAI 兼容）
    ✅ 4. 首字延迟 < 2s
    ✅ 5. Token 用量日志（每次请求）
    ✅ 6. 错误兜底（4 类常见错误 + 友好提示 + 重试）
    ✅ 7. 单租户/单用户
    ✅ 8. 内存 session 存储
    ✅ 9. 一问一答能答对（4/5 分）

不在阶段 0 范围（明确不做）:
    ❌ 数据库
    ❌ 多租户
    ❌ RAG / 知识库
    ❌ 多模型路由
    ❌ 鉴权 / 登录
    ❌ 流式响应（SSE）—— 阶段 1 再加
    ❌ 前端框架（原生 HTML+JS 即可）

技术栈:
    - Python 3.10+
    - FastAPI
    - OpenAI Python SDK（兼容 DeepSeek / Qwen / Moonshot / GPT）
    - Pydantic v2
    - uvicorn

运行方法:
    1. pip install fastapi uvicorn openai pydantic python-dotenv
    2. 创建 .env 文件，填入 API Key（参考下方）
    3. python stage0_main.py
    4. 浏览器打开 http://localhost:8000

.env 模板:
    LLM_PROVIDER=deepseek
    LLM_API_KEY=sk-你的真实key
    LLM_BASE_URL=https://api.deepseek.com/v1
    LLM_MODEL=deepseek-chat
    LLM_MAX_TOKENS=2048
    LLM_TIMEOUT=30
"""

import os
import time
import uuid
import logging
from datetime import datetime
from typing import List, Dict, Optional
from collections import deque
from threading import Lock

from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from openai import OpenAI, APIError, APIConnectionError, RateLimitError, APITimeoutError, AuthenticationError, BadRequestError

# ============== 配置 ==============

class Settings:
    """从环境变量加载配置。阶段 0 单租户，无需加密。"""

    def __init__(self):
        self.provider: str = os.getenv("LLM_PROVIDER", "deepseek")
        self.api_key: str = os.getenv("LLM_API_KEY", "")
        self.base_url: str = os.getenv("LLM_BASE_URL", "https://api.deepseek.com/v1")
        self.model: str = os.getenv("LLM_MODEL", "deepseek-chat")
        self.max_tokens: int = int(os.getenv("LLM_MAX_TOKENS", "2048"))
        self.timeout: int = int(os.getenv("LLM_TIMEOUT", "30"))
        self.max_history: int = int(os.getenv("MAX_HISTORY", "10"))  # 保留最近 10 条
        self.max_retries: int = int(os.getenv("MAX_RETRIES", "2"))
        self.host: str = os.getenv("HOST", "0.0.0.0")
        self.port: int = int(os.getenv("PORT", "8000"))

        # 校验
        if not self.api_key:
            raise ValueError(
                "❌ LLM_API_KEY 未设置！请在 .env 中填入真实 API Key\n"
                "DeepSeek: https://platform.deepseek.com\n"
                "Qwen: https://dashscope.aliyun.com\n"
                "OpenAI: https://platform.openai.com"
            )

settings = Settings()

# ============== 日志 ==============

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger("stage0")

# ============== FastAPI ==============

app = FastAPI(
    title="VerticalAgent Stage 0",
    description="多轮对话 + Token 日志 + 错误兜底 —— 30 分钟可跑通",
    version="0.1.0",
)

# 跨域（前端跑在同源，但允许 5173 等开发端口）
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# 静态文件（前端）
STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
if os.path.isdir(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# ============== LLM Client ==============

client = OpenAI(
    api_key=settings.api_key,
    base_url=settings.base_url,
    timeout=settings.timeout,
)

# ============== Session 管理（内存）==============
# 阶段 0 简单：内存 dict，进程重启丢数据 —— 阶段 2 换 PostgreSQL

class SessionManager:
    """内存会话管理。线程安全。"""

    def __init__(self, max_history: int = 10):
        self.max_history = max_history
        self._sessions: Dict[str, deque] = {}
        self._meta: Dict[str, dict] = {}
        self._lock = Lock()

    def get_or_create(self, session_id: Optional[str]) -> tuple[str, deque]:
        with self._lock:
            if session_id is None or session_id not in self._sessions:
                session_id = str(uuid.uuid4())
                self._sessions[session_id] = deque(maxlen=self.max_history)
                self._meta[session_id] = {
                    "created_at": datetime.now().isoformat(),
                    "request_count": 0,
                    "total_tokens": 0,
                }
            return session_id, self._sessions[session_id]

    def append(self, session_id: str, message: dict):
        with self._lock:
            if session_id in self._sessions:
                self._sessions[session_id].append(message)
                self._meta[session_id]["request_count"] += 1

    def get_history(self, session_id: str) -> list:
        with self._lock:
            return list(self._sessions.get(session_id, []))

    def update_tokens(self, session_id: str, tokens: int):
        with self._lock:
            if session_id in self._meta:
                self._meta[session_id]["total_tokens"] += tokens

    def delete(self, session_id: str) -> bool:
        with self._lock:
            if session_id in self._sessions:
                del self._sessions[session_id]
                del self._meta[session_id]
                return True
            return False

    def list_sessions(self) -> List[dict]:
        with self._lock:
            return [
                {"session_id": sid, **meta}
                for sid, meta in self._meta.items()
            ]


sessions = SessionManager(max_history=settings.max_history)

# ============== Pydantic Models ==============

class ChatRequest(BaseModel):
    message: str = Field(..., min_length=1, max_length=8000, description="用户消息")
    session_id: Optional[str] = Field(None, description="会话 ID（首次可不传，自动生成）")
    system_prompt: Optional[str] = Field(None, description="可选 system prompt（阶段 0 简单用）")
    temperature: float = Field(0.7, ge=0, le=2, description="采样温度")


class ChatResponse(BaseModel):
    session_id: str
    reply: str
    usage: dict
    latency_ms: int
    model: str


class SessionInfo(BaseModel):
    session_id: str
    message_count: int
    total_tokens: int
    created_at: str
    last_message_preview: Optional[str] = None


# ============== 错误兜底 ==============

def call_llm_with_retry(messages: list, **kwargs) -> dict:
    """
    调用 LLM，带重试 + 错误分类。
    阶段 0 简单实现：指数退避，最多重试 2 次。
    """
    last_error = None
    for attempt in range(settings.max_retries + 1):
        try:
            start = time.time()
            resp = client.chat.completions.create(
                model=settings.model,
                messages=messages,
                max_tokens=settings.max_tokens,
                temperature=kwargs.get("temperature", 0.7),
                timeout=settings.timeout,
            )
            latency = time.time() - start
            return {
                "ok": True,
                "resp": resp,
                "latency": latency,
                "attempt": attempt + 1,
            }
        except RateLimitError as e:
            # 429 限流 —— 等 2s 重试
            last_error = ("rate_limit", f"调用频率过高（429），请稍后再试: {e}")
            log.warning(f"Rate limited, retry {attempt + 1}/{settings.max_retries}")
            time.sleep(2 ** attempt)
        except APITimeoutError as e:
            # 超时
            last_error = ("timeout", f"请求超时（>{settings.timeout}s），请检查网络: {e}")
            log.warning(f"Timeout, retry {attempt + 1}/{settings.max_retries}")
            time.sleep(1)
        except APIConnectionError as e:
            # 网络问题
            last_error = ("network", f"网络连接失败: {e}")
            log.warning(f"Network error, retry {attempt + 1}/{settings.max_retries}")
            time.sleep(1)
        except AuthenticationError as e:
            # API Key 无效 —— 不重试
            last_error = ("auth", f"API Key 无效（401），请检查 .env 配置: {e}")
            log.error(f"Auth failed: {e}")
            break
        except BadRequestError as e:
            # 参数错误（内容审核拦截、context 过长等）—— 不重试
            last_error = ("bad_request", f"请求被拒绝（400），可能是内容审核或上下文过长: {e}")
            log.error(f"Bad request: {e}")
            break
        except APIError as e:
            # 其他 API 错误
            last_error = ("api_error", f"LLM 服务错误: {e}")
            log.error(f"API error: {e}")
            time.sleep(1)
        except Exception as e:
            # 未知错误
            last_error = ("unknown", f"未知错误: {type(e).__name__}: {e}")
            log.exception(f"Unexpected error: {e}")
            break

    return {"ok": False, "error_type": last_error[0], "error_msg": last_error[1]}


# ============== API 路由 ==============

@app.get("/")
def index():
    """返回前端 HTML"""
    index_path = os.path.join(STATIC_DIR, "index.html")
    if os.path.isfile(index_path):
        return FileResponse(index_path)
    return {"hint": "请将前端 index.html 放到 static/ 目录", "docs": "/docs"}


@app.get("/health")
def health():
    """健康检查"""
    return {
        "status": "ok",
        "provider": settings.provider,
        "model": settings.model,
        "sessions": len(sessions.list_sessions()),
    }


@app.post("/chat", response_model=ChatResponse)
def chat(req: ChatRequest):
    """核心对话接口"""

    # 1. 获取/创建 session
    session_id, history = sessions.get_or_create(req.session_id)

    # 2. 构造 messages（OpenAI 格式）
    messages = []
    if req.system_prompt:
        messages.append({"role": "system", "content": req.system_prompt})
    messages.extend(list(history))
    messages.append({"role": "user", "content": req.message})

    # 3. 调用 LLM
    result = call_llm_with_retry(messages, temperature=req.temperature)

    if not result["ok"]:
        # 错误兜底：返回友好提示
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"error_type": result["error_type"], "message": result["error_msg"]},
        )

    resp = result["resp"]
    latency = result["latency"]

    # 4. 关键：Token 用量日志
    usage = resp.usage
    log.info(
        f"📊 session={session_id[:8]}... "
        f"model={settings.model} "
        f"prompt={usage.prompt_tokens} "
        f"completion={usage.completion_tokens} "
        f"total={usage.total_tokens} "
        f"latency={latency:.2f}s "
        f"attempt={result['attempt']}"
    )

    # 5. 更新 session 状态
    reply = resp.choices[0].message.content
    sessions.append(session_id, {"role": "user", "content": req.message})
    sessions.append(session_id, {"role": "assistant", "content": reply})
    sessions.update_tokens(session_id, usage.total_tokens)

    # 6. 返回
    return ChatResponse(
        session_id=session_id,
        reply=reply,
        usage={
            "prompt_tokens": usage.prompt_tokens,
            "completion_tokens": usage.completion_tokens,
            "total_tokens": usage.total_tokens,
        },
        latency_ms=int(latency * 1000),
        model=settings.model,
    )


@app.get("/sessions/{session_id}")
def get_session(session_id: str):
    """获取会话历史"""
    history = sessions.get_history(session_id)
    if not history:
        raise HTTPException(404, "session not found")
    return {
        "session_id": session_id,
        "message_count": len(history),
        "messages": history,
    }


@app.delete("/sessions/{session_id}")
def clear_session(session_id: str):
    """清空会话"""
    if sessions.delete(session_id):
        return {"ok": True}
    raise HTTPException(404, "session not found")


@app.get("/sessions")
def list_sessions():
    """列出所有 session（调试用）"""
    return {
        "count": len(sessions.list_sessions()),
        "sessions": sessions.list_sessions(),
    }


# ============== 启动 ==============

if __name__ == "__main__":
    import uvicorn

    log.info("=" * 60)
    log.info(f"🚀 VerticalAgent Stage 0 启动")
    log.info(f"   Provider: {settings.provider}")
    log.info(f"   Model:    {settings.model}")
    log.info(f"   Endpoint: http://{settings.host}:{settings.port}")
    log.info(f"   Docs:     http://{settings.host}:{settings.port}/docs")
    log.info("=" * 60)

    uvicorn.run(
        "main:app",
        host=settings.host,
        port=settings.port,
        reload=False,  # 阶段 0 不开 reload（重载会让内存 session 丢）
    )
