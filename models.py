"""
models.py —— SQLAlchemy 2.x ORM 模型
=====================================

四张表：

  sessions
    - id (TEXT, UUID)
    - title (TEXT, 默认 "新会话"，首条 user 消息自动生成)
    - created_at / updated_at (REAL, unix timestamp)
    - request_count / total_tokens (INTEGER)
    - primary_provider (TEXT, 可选，默认 provider)
    - metadata_json (TEXT, 备用 JSON)

  messages
    - id (INTEGER, autoincrement)
    - session_id (FK → sessions.id, ON DELETE CASCADE)
    - role (TEXT: 'user' | 'assistant' | 'system')
    - content (TEXT, 消息正文)
    - reasoning (TEXT, 思考过程，可选，DeepSeek-R1 / o1 风格)
    - provider / model (TEXT, 实际调用的 provider 和 model)
    - prompt_tokens / completion_tokens / total_tokens (INTEGER)
    - is_fallback (INTEGER 0/1, 是否 fallback 后生成)
    - latency_ms (INTEGER, 该次调用延迟)
    - created_at (REAL)

  documents  (Stage 2C 新增：知识库文档)
    - id (INTEGER, PK)
    - title (TEXT)
    - source (TEXT, 文件名/URL/手动输入标记)
    - doc_type (TEXT, 'text' / 'file' / 'url')
    - chunk_count (INTEGER)
    - embedding_provider / embedding_model / embedding_dim (TEXT/INT, 用于追踪向量维度)
    - created_at (REAL)

  chunks  (Stage 2C 新增：文档切片 + embedding)
    - id (INTEGER, PK)
    - document_id (FK → documents.id, ON DELETE CASCADE)
    - chunk_index (INTEGER, 文档内顺序)
    - content (TEXT, 切片文本)
    - embedding (TEXT, JSON 序列化的 float 数组)
    - token_count (INTEGER, 估算的 token 数)
    - created_at (REAL)

设计要点：
    1. 时间用 REAL 存 unix timestamp（秒），不用 datetime —— SQLite 原生支持，
       后期切 PG 也只需改 Column 类型，代码层不动。
    2. messages.session_id 加 INDEX，便于按会话查全部消息 + 翻页。
    3. sessions 加 INDEX on updated_at DESC，侧栏按"最近活动"排序。
    4. CASCADE：删 session 时自动删 messages；删 document 时自动删 chunks。
    5. messages / chunks 的 content 用 TEXT 而不是 VARCHAR —— 不限长度。
    6. relationship 加 lazy='selectin'，避免 N+1 查询。
    7. SQLAlchemy 2.x 用 Mapped[] 注解 relationship（替代旧式 Column/relationship 分开声明）。
    8. embedding 用 TEXT 存 JSON 字符串（不是 BLOB）—— 起步阶段够用，10 万级以下无压力。
       Stage 2E 切 Milvus 时整体替换 chunks 表。
"""

from datetime import datetime
from typing import List, Optional

from sqlalchemy import (
    String, Integer, Text, Float, ForeignKey, Index,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship, declarative_base

Base = declarative_base()


# ============== sessions ==============

class Session(Base):
    __tablename__ = "sessions"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    title: Mapped[str] = mapped_column(String(255), nullable=False, default="新会话")
    created_at: Mapped[float] = mapped_column(Float, nullable=False, default=lambda: datetime.now().timestamp())
    updated_at: Mapped[float] = mapped_column(
        Float,
        nullable=False,
        default=lambda: datetime.now().timestamp(),
        index=True,  # 侧栏按 updated_at DESC 排序
    )
    request_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    total_tokens: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    primary_provider: Mapped[Optional[str]] = mapped_column(String(64))
    metadata_json: Mapped[Optional[str]] = mapped_column(Text)  # 备用 JSON 字段（Stage 3+ tags/tenant 用）

    messages: Mapped[List["Message"]] = relationship(
        "Message",
        back_populates="session",
        cascade="all, delete-orphan",
        order_by="Message.created_at",
        lazy="selectin",
    )

    def to_meta_dict(self) -> dict:
        """侧栏列表用的精简字段（不含 messages 完整内容）"""
        return {
            "session_id": self.id,
            "title": self.title,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "request_count": self.request_count,
            "total_tokens": self.total_tokens,
            "primary_provider": self.primary_provider,
            "message_count": len(self.messages) if self.messages else 0,
        }

    def __repr__(self):
        return f"<Session id={self.id[:8]}... title={self.title!r} msgs={len(self.messages or [])}>"


# ============== messages ==============

class Message(Base):
    __tablename__ = "messages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    session_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("sessions.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    role: Mapped[str] = mapped_column(String(32), nullable=False)  # 'user' / 'assistant' / 'system'
    content: Mapped[str] = mapped_column(Text, nullable=False)
    reasoning: Mapped[Optional[str]] = mapped_column(Text)
    provider: Mapped[Optional[str]] = mapped_column(String(64))
    model: Mapped[Optional[str]] = mapped_column(String(128))
    prompt_tokens: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    completion_tokens: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    total_tokens: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    is_fallback: Mapped[int] = mapped_column(Integer, nullable=False, default=0)  # 0 / 1
    latency_ms: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[float] = mapped_column(Float, nullable=False, default=lambda: datetime.now().timestamp())

    session: Mapped["Session"] = relationship("Session", back_populates="messages")

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "session_id": self.session_id,
            "role": self.role,
            "content": self.content,
            "reasoning": self.reasoning,
            "provider": self.provider,
            "model": self.model,
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "total_tokens": self.total_tokens,
            "is_fallback": bool(self.is_fallback),
            "latency_ms": self.latency_ms,
            "created_at": self.created_at,
        }

    def __repr__(self):
        snippet = (self.content or "")[:30].replace("\n", " ")
        return f"<Message id={self.id} role={self.role} {snippet!r}>"


# ============== 复合索引 ==============

# messages 按 session + created_at 查（侧栏加载历史用）
Index("idx_messages_session_created", Message.session_id, Message.created_at)


# ============== documents (Stage 2C) ==============

class Document(Base):
    __tablename__ = "documents"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    source: Mapped[Optional[str]] = mapped_column(String(512))         # 文件名 / URL / 'manual'
    doc_type: Mapped[str] = mapped_column(String(32), nullable=False, default="text")
    chunk_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    # 记录 embedding 来源，方便未来混用不同模型时区分
    embedding_provider: Mapped[Optional[str]] = mapped_column(String(64))
    embedding_model: Mapped[Optional[str]] = mapped_column(String(128))
    embedding_dim: Mapped[Optional[int]] = mapped_column(Integer)
    created_at: Mapped[float] = mapped_column(
        Float,
        nullable=False,
        default=lambda: datetime.now().timestamp(),
        index=True,  # 文档列表按时间倒序
    )
    metadata_json: Mapped[Optional[str]] = mapped_column(Text)

    chunks: Mapped[List["Chunk"]] = relationship(
        "Chunk",
        back_populates="document",
        cascade="all, delete-orphan",
        order_by="Chunk.chunk_index",
        lazy="selectin",
    )

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "title": self.title,
            "source": self.source,
            "doc_type": self.doc_type,
            "chunk_count": self.chunk_count,
            "embedding_provider": self.embedding_provider,
            "embedding_model": self.embedding_model,
            "embedding_dim": self.embedding_dim,
            "created_at": self.created_at,
        }

    def __repr__(self):
        return f"<Document id={self.id} title={self.title!r} chunks={self.chunk_count}>"


# ============== chunks (Stage 2C) ==============

class Chunk(Base):
    __tablename__ = "chunks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    document_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("documents.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    chunk_index: Mapped[int] = mapped_column(Integer, nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    embedding: Mapped[str] = mapped_column(Text, nullable=False)  # JSON 序列化的 float[]
    token_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[float] = mapped_column(
        Float,
        nullable=False,
        default=lambda: datetime.now().timestamp(),
    )

    document: Mapped["Document"] = relationship("Document", back_populates="chunks")

    def to_dict(self, include_embedding: bool = False) -> dict:
        data = {
            "id": self.id,
            "document_id": self.document_id,
            "chunk_index": self.chunk_index,
            "content": self.content,
            "token_count": self.token_count,
            "created_at": self.created_at,
        }
        if include_embedding:
            data["embedding"] = self.embedding
        return data

    def __repr__(self):
        snippet = (self.content or "")[:30].replace("\n", " ")
        return f"<Chunk id={self.id} doc={self.document_id} idx={self.chunk_index} {snippet!r}>"


# chunks 按 document + chunk_index 查（检索后回查文档）
Index("idx_chunks_doc_index", Chunk.document_id, Chunk.chunk_index)