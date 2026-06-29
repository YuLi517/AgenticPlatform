"""
embedding.py —— Embedding API 客户端（OpenAI 兼容协议）
=========================================================

设计目标：
    1. 复用已有 provider 配置（DeepSeek / Qwen / MiniMax 都支持 OpenAI 兼容协议）
    2. 但 embedding 端点跟 chat 端点可能不同（/v1/embeddings vs /v1/chat/completions）
    3. 单独配置 EMBEDDING_*：provider / base_url / api_key / model / dimension

支持的 embedding 服务（OpenAI 兼容 /embeddings 端点）：
    - OpenAI: text-embedding-3-small (1536d), text-embedding-3-large (3072d)
    - 智谱: embedding-3 (2048d)
    - Qwen (DashScope 兼容模式): text-embedding-v3 (1024d)
    - DeepSeek: 暂不提供 embedding API（要用请配 OpenAI）

实现要点：
    - 单条 / 批量都支持（openai SDK 接受 list[str]）
    - 失败时返回 None + log，不抛异常（业务层决定降级）
    - 返回维度在首批调用时自动探测（看实际返回的 embedding 长度）
"""

import logging
import time
from dataclasses import dataclass
from typing import List, Optional

from openai import OpenAI, APIError, APIConnectionError, RateLimitError, APITimeoutError

log = logging.getLogger("stage2.embedding")


@dataclass
class EmbeddingConfig:
    provider: str              # 'openai' / 'zhipu' / 'qwen' / ...
    api_key: str
    base_url: str
    model: str
    timeout: int = 30
    # 期望维度（None 表示首批自动探测）
    expected_dim: Optional[int] = None


class EmbeddingClient:
    """单个 Embedding 服务客户端"""

    def __init__(self, config: EmbeddingConfig):
        self.config = config
        self.client = OpenAI(
            api_key=config.api_key,
            base_url=config.base_url,
            timeout=config.timeout,
        )
        self._detected_dim: Optional[int] = None

    def embed(self, texts: List[str]) -> Optional[List[List[float]]]:
        """批量 embedding，返回向量列表（每条对应输入顺序）。失败返 None。"""
        if not texts:
            return []
        # 空字符串过滤（部分 API 不接受空串）
        clean_texts = [t if t.strip() else " " for t in texts]
        try:
            start = time.time()
            resp = self.client.embeddings.create(
                model=self.config.model,
                input=clean_texts,
            )
            latency = time.time() - start
            vectors = [d.embedding for d in resp.data]
            # 自动探测维度
            if vectors and self._detected_dim is None:
                self._detected_dim = len(vectors[0])
                if self.config.expected_dim and self.config.expected_dim != self._detected_dim:
                    log.warning(
                        f"⚠️ embedding 维度不符: 配置 {self.config.expected_dim} 实际 {self._detected_dim}"
                    )
            log.info(
                f"✅ embedding [{self.config.provider}/{self.config.model}] "
                f"count={len(texts)} dim={self._detected_dim} latency={latency:.2f}s"
            )
            return vectors
        except (RateLimitError, APITimeoutError, APIConnectionError, APIError) as e:
            log.error(f"❌ embedding [{self.config.provider}] {type(e).__name__}: {e}")
            return None
        except Exception as e:
            log.exception(f"❌ embedding [{self.config.provider}] unknown error: {e}")
            return None

    def embed_one(self, text: str) -> Optional[List[float]]:
        """单条便捷接口"""
        result = self.embed([text])
        return result[0] if result else None

    @property
    def dimension(self) -> Optional[int]:
        return self._detected_dim


def load_embedding_config_from_env() -> Optional[EmbeddingConfig]:
    """从环境变量读取 embedding 配置（默认 None 表示未启用 RAG）"""
    import os
    provider = os.getenv("EMBEDDING_PROVIDER", "").strip()
    if not provider:
        return None
    api_key = os.getenv(f"{provider.upper()}_API_KEY", "") or os.getenv("EMBEDDING_API_KEY", "")
    base_url = os.getenv(
        f"{provider.upper()}_BASE_URL",
        os.getenv("EMBEDDING_BASE_URL", ""),
    )
    model = os.getenv(
        f"{provider.upper()}_EMBEDDING_MODEL",
        os.getenv("EMBEDDING_MODEL", ""),
    )
    if not (api_key and base_url and model):
        log.warning(f"⚠️ EMBEDDING_PROVIDER={provider} 但配置不完整，RAG 不可用")
        return None
    expected_dim = os.getenv("EMBEDDING_DIMENSION")
    return EmbeddingConfig(
        provider=provider,
        api_key=api_key,
        base_url=base_url,
        model=model,
        timeout=int(os.getenv("EMBEDDING_TIMEOUT", "30")),
        expected_dim=int(expected_dim) if expected_dim else None,
    )