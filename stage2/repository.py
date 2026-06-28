"""
repository.py —— Session / Message 数据库操作封装
=================================================

设计目标：
    1. 业务层（main.py）只调 repository，不直接写 SQL
    2. 单一职责：每个方法一个明确动作（get / create / append / list / search / delete）
    3. 兼容 stage1 的 SessionManager 接口语义（get_or_create / append / get_history / clear / list_sessions）
       —— 这样 main.py 改造最小，只需把内存 dict 操作换成 repo 调用。

搜索策略（LIKE 即可，Stage 2 不上 FTS5）：
    - title 命中 OR 任一 message.content 命中
    - 用 LIKE '%kw%'（SQLite 默认 NOCASE 不开，区分大小写 —— 中文场景无所谓）
    - 10 万级数据走 LIKE 没问题；上百万再换 FTS5 或外部 ES

翻页策略：
    - offset / limit（简单，对侧栏"加载更多"够用）
    - 总数单独查一次（select count） —— 单会话表几千行以下没问题
"""

import time
import uuid
from typing import List, Optional, Tuple

from sqlalchemy import select, or_, func, delete
from sqlalchemy.orm import Session as DbSession

from models import Session as SessionModel, Message as MessageModel


# ============== 工具 ==============

def generate_session_id() -> str:
    return str(uuid.uuid4())


def _now() -> float:
    return time.time()


def _truncate_title(content: str, n: int = 30) -> str:
    """生成会话标题：首条 user 消息前 n 字"""
    content = (content or "").strip().replace("\n", " ")
    if len(content) <= n:
        return content
    return content[:n] + "…"


# ============== SessionRepository ==============

class SessionRepository:
    """所有 Session / Message 相关 DB 操作。"""

    def __init__(self, db: DbSession):
        self.db = db

    # ----- commit / rollback -----
    def commit(self):
        self.db.commit()

    def rollback(self):
        self.db.rollback()

    # ----- Session 基础 -----

    def get(self, sid: str) -> Optional[SessionModel]:
        return self.db.get(SessionModel, sid)

    def get_or_create(
        self,
        sid: Optional[str],
        primary_provider: Optional[str] = None,
    ) -> Tuple[str, SessionModel]:
        """兼容 stage1 接口语义：传 sid 就用，不存在就创建。"""
        if sid:
            s = self.db.get(SessionModel, sid)
            if s:
                return sid, s
        new_sid = generate_session_id()
        now = _now()
        s = SessionModel(
            id=new_sid,
            title="新会话",
            created_at=now,
            updated_at=now,
            primary_provider=primary_provider,
        )
        self.db.add(s)
        self.db.flush()  # 立即可见
        return new_sid, s

    # ----- Session 列表 / 搜索 / 翻页 -----

    def list_sessions(
        self,
        page: int = 1,
        page_size: int = 20,
        q: Optional[str] = None,
    ) -> Tuple[List[SessionModel], int]:
        """返回 (本页列表, 命中总数)。按 updated_at DESC。"""
        page = max(1, page)
        page_size = max(1, min(page_size, 100))

        stmt = select(SessionModel)

        if q:
            like = f"%{q}%"
            # 命中条件：title 含 OR 任一 message.content 含
            hit_sids_subq = (
                select(MessageModel.session_id)
                .where(MessageModel.content.like(like))
                .distinct()
            )
            stmt = stmt.where(
                or_(
                    SessionModel.title.like(like),
                    SessionModel.id.in_(hit_sids_subq),
                )
            )

        # 总数
        total_stmt = select(func.count()).select_from(stmt.subquery())
        total = self.db.execute(total_stmt).scalar() or 0

        # 分页
        paged_stmt = (
            stmt.order_by(SessionModel.updated_at.desc())
            .limit(page_size)
            .offset((page - 1) * page_size)
        )
        items = list(self.db.execute(paged_stmt).scalars())
        return items, total

    def update_title(self, sid: str, title: str) -> bool:
        s = self.db.get(SessionModel, sid)
        if not s:
            return False
        title = (title or "").strip()[:255] or "新会话"
        s.title = title
        s.updated_at = _now()
        return True

    def touch(self, sid: str, **kwargs):
        """更新 updated_at 和任意字段（用于 fallback 时记录 primary_provider 等）"""
        s = self.db.get(SessionModel, sid)
        if not s:
            return
        s.updated_at = _now()
        for k, v in kwargs.items():
            if hasattr(s, k):
                setattr(s, k, v)

    def delete(self, sid: str) -> bool:
        s = self.db.get(SessionModel, sid)
        if not s:
            return False
        self.db.delete(s)  # CASCADE 自动删 messages
        return True

    # ----- Message -----

    def append_message(
        self,
        sid: str,
        role: str,
        content: str,
        reasoning: Optional[str] = None,
        provider: Optional[str] = None,
        model: Optional[str] = None,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
        total_tokens: int = 0,
        is_fallback: bool = False,
        latency_ms: int = 0,
    ) -> MessageModel:
        """追加一条消息（不 commit，由外层 commit）。"""
        m = MessageModel(
            session_id=sid,
            role=role,
            content=content,
            reasoning=reasoning,
            provider=provider,
            model=model,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=total_tokens,
            is_fallback=1 if is_fallback else 0,
            latency_ms=latency_ms,
            created_at=_now(),
        )
        self.db.add(m)
        # 同步更新 session 元数据
        s = self.db.get(SessionModel, sid)
        if s:
            s.updated_at = _now()
            if role == "user":
                # 首条 user 消息自动设标题
                if s.title == "新会话" or not s.title:
                    s.title = _truncate_title(content)
                s.request_count = (s.request_count or 0) + 1
            elif role == "assistant":
                s.total_tokens = (s.total_tokens or 0) + total_tokens
        return m

    def get_messages(self, sid: str) -> List[MessageModel]:
        stmt = (
            select(MessageModel)
            .where(MessageModel.session_id == sid)
            .order_by(MessageModel.created_at)
        )
        return list(self.db.execute(stmt).scalars())

    def get_history_openai(self, sid: str, max_messages: int = 10) -> list:
        """返回 OpenAI 格式（role+content），最近 N 条，用于 LLM 上下文拼接。"""
        msgs = self.get_messages(sid)
        recent = msgs[-max_messages:]
        return [{"role": m.role, "content": m.content} for m in recent]

    # ----- 统计 -----

    def count_sessions(self) -> int:
        return self.db.execute(select(func.count(SessionModel.id))).scalar() or 0

    def count_messages(self) -> int:
        return self.db.execute(select(func.count(MessageModel.id))).scalar() or 0