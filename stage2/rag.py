"""
rag.py —— RAG 核心逻辑：切片 + 向量检索 + prompt 注入
======================================================

设计要点：
    1. 切片策略：按字符简单切（chunk_size=500, overlap=50）。
       不做语义切片（避免引入复杂依赖）。中英文都 OK。
    2. 向量检索：cosine similarity，优先用 numpy 批量算（快 50x+），降级纯 Python。
       起步阶段 1 万级以下没问题；10 万级以上换 Milvus（Stage 2E）。
    3. Prompt 注入：把 top-k 片段拼成 system prompt，让 LLM 基于片段回答并引用 [1][2] 编号。

API：
    chunk_text(text, chunk_size, overlap) -> List[str]
    cosine_search(query_vec, vectors, top_k) -> List[(idx, score)]
    build_rag_prompt(query, chunks, system_prompt=None) -> str
    build_rag_context(chunks_with_meta) -> str
"""

import math
from typing import List, Tuple, Optional


# ============== 切片 ==============

def chunk_text(text: str, chunk_size: int = 500, overlap: int = 50) -> List[str]:
    """
    按字符切片，每 chunk_size 字符一段，相邻段重叠 overlap 字符。

    Args:
        text: 输入文本
        chunk_size: 每段最大字符数（默认 500）
        overlap: 相邻段重叠字符数（默认 50）

    Returns:
        切片列表（保留原始顺序）
    """
    text = (text or "").strip()
    if not text:
        return []
    if len(text) <= chunk_size:
        return [text]

    chunks: List[str] = []
    start = 0
    n = len(text)
    while start < n:
        end = min(start + chunk_size, n)
        # 在 end 附近找一个自然断点（句号/换行/空格），让切片更可读
        if end < n:
            natural_break = max(
                text.rfind("\n\n", start, end),
                text.rfind("。", start, end),
                text.rfind(".\n", start, end),
                text.rfind("\n", start, end),
                text.rfind(" ", start, end),
            )
            if natural_break > start + chunk_size // 2:
                end = natural_break + 1
        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        if end >= n:
            break
        start = max(end - overlap, start + 1)
    return chunks


# ============== 向量检索 ==============

def _cosine_search_numpy(query_vec, vectors, top_k, min_score):
    """numpy 加速版（默认路径）"""
    try:
        import numpy as np
    except ImportError:
        return None  # 触发降级

    if not query_vec or not vectors:
        return []
    q = np.asarray(query_vec, dtype=np.float32)
    # 过滤维度不一致的（混合 embedding 模型时的容错）
    valid_mask = [len(v) == len(q) for v in vectors]
    valid_indices = [i for i, ok in enumerate(valid_mask) if ok]
    if not valid_indices:
        return []
    M = np.asarray([vectors[i] for i in valid_indices], dtype=np.float32)
    # 归一化（避免除零）
    q_norm = np.linalg.norm(q)
    M_norm = np.linalg.norm(M, axis=1)
    q_norm = q_norm if q_norm > 0 else 1.0
    safe_M_norm = np.where(M_norm > 0, M_norm, 1.0)
    # cosine = (M @ q) / (|M| * |q|)
    sims = (M @ q) / (safe_M_norm * q_norm)
    # 阈值过滤
    sims = np.where(sims >= min_score, sims, -np.inf)
    # 取 top-k
    k = min(top_k, len(valid_indices))
    if k <= 0:
        return []
    # argpartition 比 argsort 快（不需要全排序）
    top_idx_local = np.argpartition(-sims, k - 1)[:k]
    # 对 top-k 再排序
    top_idx_local = top_idx_local[np.argsort(-sims[top_idx_local])]
    return [(valid_indices[int(i)], float(sims[int(i)])) for i in top_idx_local]


def _cosine_search_python(query_vec, vectors, top_k, min_score):
    """纯 Python 降级版（numpy 不可用时）"""
    if not query_vec or not vectors:
        return []
    scores = []
    q_norm = math.sqrt(sum(x * x for x in query_vec))
    if q_norm == 0:
        q_norm = 1.0
    for i, v in enumerate(vectors):
        if len(v) != len(query_vec):
            continue
        dot = sum(x * y for x, y in zip(query_vec, v))
        v_norm = math.sqrt(sum(y * y for y in v))
        if v_norm == 0:
            v_norm = 1.0
        s = dot / (q_norm * v_norm)
        if s >= min_score:
            scores.append((i, s))
    scores.sort(key=lambda x: x[1], reverse=True)
    return scores[:top_k]


def cosine_search(
    query_vec: List[float],
    vectors: List[List[float]],
    top_k: int = 5,
    min_score: float = 0.0,
) -> List[Tuple[int, float]]:
    """
    在向量库里检索 top-k 最相似的。

    Args:
        query_vec: 查询向量（已被 embedding API 算好）
        vectors: 候选向量列表（与 chunks 一一对应）
        top_k: 返回数量
        min_score: 最低相似度阈值（默认 0）

    Returns:
        [(原始索引, 分数), ...] 按分数降序
    """
    result = _cosine_search_numpy(query_vec, vectors, top_k, min_score)
    if result is not None:
        return result
    return _cosine_search_python(query_vec, vectors, top_k, min_score)


# ============== Prompt 注入 ==============

RAG_SYSTEM_PROMPT_TEMPLATE = """你是 VerticalAgent，一个基于检索增强生成（RAG）的助手。

请严格根据下面提供的「参考资料」回答用户问题。如果参考资料不足以回答问题，请明确说"参考资料中没有足够信息"，不要编造。

回答要求：
1. 用中文回答
2. 引用参考资料时用方括号标注来源编号，例如「根据 [1] 的描述……」
3. 如果多个片段都相关，可以引用多个，例如 [1][3]
4. 保持简洁，必要时给出代码或公式

参考资料：
{context}
"""


def build_rag_context(
    chunks: List[dict],
    max_chars_per_chunk: int = 800,
) -> str:
    """
    把检索到的 chunks 拼成 LLM prompt 用的 context 字符串。

    Args:
        chunks: 检索结果列表，每个元素是 dict，至少含 'content'，可含 'document_id' / 'title' / 'score'
        max_chars_per_chunk: 单个 chunk 显示的最大字符数（截断避免超长）

    Returns:
        格式化的 context 字符串
    """
    if not chunks:
        return "（暂无参考资料）"

    parts = []
    for idx, c in enumerate(chunks, 1):
        content = (c.get("content") or "").strip()
        if len(content) > max_chars_per_chunk:
            content = content[:max_chars_per_chunk] + "..."
        title = c.get("title") or f"doc#{c.get('document_id', '?')}"
        score = c.get("score")
        score_str = f" 相似度={score:.2f}" if isinstance(score, (int, float)) else ""
        parts.append(f"[{idx}] (来自 {title}){score_str}\n{content}")
    return "\n\n".join(parts)


def build_rag_prompt(
    query: str,
    chunks: List[dict],
    base_system_prompt: Optional[str] = None,
) -> str:
    """
    构造带 RAG 上下文的 system prompt。
    """
    context = build_rag_context(chunks)
    rag_prompt = RAG_SYSTEM_PROMPT_TEMPLATE.format(context=context)
    if base_system_prompt:
        return f"{base_system_prompt}\n\n{rag_prompt}"
    return rag_prompt


# ============== 便捷封装 ==============

def parse_embedding(embedding_json: str) -> List[float]:
    """从 DB 读出的 JSON 字符串解析回 float 列表"""
    import json
    if not embedding_json:
        return []
    try:
        return json.loads(embedding_json)
    except (json.JSONDecodeError, TypeError):
        return []


def serialize_embedding(vec: List[float]) -> str:
    """把 float 列表序列化成 JSON 字符串存 DB"""
    import json
    return json.dumps(vec, ensure_ascii=False)