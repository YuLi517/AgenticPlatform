# VerticalAgent Stage 2 —— 会话持久化 + RAG 知识库

> 基于 [Implementation Plan](../Implementation_Plan.docx) **阶段 2** 的 **2A + 2B + 2C** 子任务：
> - **2A/2B**：数据持久化（SQLite）+ 侧栏会话搜索 + 翻页 + 加载历史会话
> - **2C**：Embedding + 简单向量检索（cosine + numpy）+ RAG 注入
>
> 继承 Stage 1 全部能力：多 Provider 路由 + SSE 流式 + 熔断器 + Token 计量。

[![Python](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110%2B-009688)](https://fastapi.tiangolo.com)
[![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.x-orange)](https://www.sqlalchemy.org)

## ⚡ 30 秒快速开始

```bash
# 1. 装依赖
pip install -r requirements.txt

# 2. 配 API Key
cp .env.example .env
# 编辑 .env 填入真实 Key（DeepSeek / Qwen / MiniMax 任选 ≥1 家）

# 3. 启动（首次启动会自动创建 data/stage2.db）
python main.py

# 4. 浏览器
http://localhost:8000
```

## 🆕 Stage 2 新增的能力

| # | 能力 | 入口 | 实现 |
|---|---|---|---|
| 1 | **重启不丢数据** | DB 文件 `data/stage2.db` | SQLAlchemy 2.x + SQLite |
| 2 | **侧栏会话搜索** | 左侧栏顶部搜索框 | `GET /sessions?q=keyword`（LIKE 命中 title 或任意消息） |
| 3 | **会话翻页** | 侧栏底部"加载更多" | `GET /sessions?page=1&page_size=20` |
| 4 | **加载历史会话** | 侧栏点击任意会话 | `GET /sessions/{sid}/messages`（含 reasoning + tokens） |
| 5 | **完整元数据** | 数据库持久化字段 | reasoning / provider / model / prompt_tokens / completion_tokens / latency / fallback 标记 |
| 6 | **DB 统计** | `GET /health` | sessions 总数 + messages 总数 |
| 7 | **🆕 知识库文档管理** | 侧栏「文档库」tab | `POST /documents`（上传 + 切片 + embedding） |
| 8 | **🆕 纯向量检索** | `POST /search` | cosine similarity（numpy 加速） |
| 9 | **🆕 RAG 对话** | `/chat/rag` | 检索 top-k + 注入 system prompt + LLM 流式回答带 `[1][2]` 引用 |
| 10 | **🆕 多种 Embedding 适配** | OpenAI / 智谱 / Qwen | `.env` 切 `EMBEDDING_PROVIDER`，OpenAI 兼容协议 |

## 📊 数据模型

### `sessions` 表

| 列 | 类型 | 说明 |
|---|---|---|
| `id` | TEXT (PK) | UUID，前端用作 session_id |
| `title` | TEXT | 首条 user 消息前 30 字（自动生成） |
| `created_at` | REAL | unix timestamp（秒） |
| `updated_at` | REAL | 最后活动时间（侧栏排序用，INDEX） |
| `request_count` | INTEGER | 用户消息计数 |
| `total_tokens` | INTEGER | 累计 token |
| `primary_provider` | TEXT | 默认 provider（deepseek/qwen/...） |
| `metadata_json` | TEXT | 备用 JSON（Stage 3+ tags/tenant 用） |

### `messages` 表

| 列 | 类型 | 说明 |
|---|---|---|
| `id` | INTEGER (PK) | autoincrement |
| `session_id` | TEXT (FK) | `ON DELETE CASCADE` |
| `role` | TEXT | `user` / `assistant` / `system` |
| `content` | TEXT | 消息正文 |
| `reasoning` | TEXT | 思考过程（DeepSeek-R1 / o1 风格，可选） |
| `provider` / `model` | TEXT | 实际调用的 provider 和 model |
| `prompt_tokens` / `completion_tokens` / `total_tokens` | INTEGER | token 拆分 |
| `is_fallback` | INTEGER (0/1) | 是否 fallback 后生成 |
| `latency_ms` | INTEGER | 该次调用延迟 |
| `created_at` | REAL | unix timestamp |

**索引**：
- `sessions.updated_at` —— 侧栏按"最近活动"排序
- `messages.session_id` —— 单条会话查询
- `messages.(session_id, created_at)` —— 加载历史会话（复合索引）

## 🔌 API 端点

### 会话列表 / 搜索 / 翻页

```
GET /sessions?page=1&page_size=20&q=RAG
```

**Query 参数**：
- `page` —— 页码（默认 1）
- `page_size` —— 每页条数（默认 20，max 100）
- `q` —— 搜索关键词（可选，命中 title 或任意消息 content）

**响应**：
```json
{
  "total": 42,
  "page": 1,
  "page_size": 20,
  "count": 20,
  "sessions": [
    {
      "session_id": "uuid-xxx",
      "title": "什么是 RAG？",
      "created_at": 1782650612.34,
      "updated_at": 1782650618.92,
      "request_count": 4,
      "total_tokens": 130,
      "primary_provider": "deepseek",
      "message_count": 5
    }
  ]
}
```

### 加载完整历史

```
GET /sessions/{sid}/messages
```

**响应**：
```json
{
  "session_id": "uuid-xxx",
  "title": "什么是 RAG？",
  "primary_provider": "deepseek",
  "message_count": 5,
  "messages": [
    {
      "id": 1,
      "role": "user",
      "content": "什么是 RAG？",
      "reasoning": null,
      "provider": null,
      "total_tokens": 0,
      "created_at": 1782650612.34
    },
    {
      "id": 2,
      "role": "assistant",
      "content": "RAG 是检索增强生成……",
      "reasoning": "用户问 RAG，我直接答。",
      "provider": "deepseek",
      "model": "deepseek-chat",
      "prompt_tokens": 12,
      "completion_tokens": 88,
      "total_tokens": 100,
      "is_fallback": false,
      "latency_ms": 1500,
      "created_at": 1782650618.92
    }
  ]
}
```

### 其它（沿用 Stage 1）

| 方法 | 路径 | 用途 |
|---|---|---|
| `POST` | `/chat` | 非流式对话 |
| `POST` | `/chat/stream` | **SSE 流式**（带 reasoning 折叠区） |
| `GET` | `/sessions/{sid}` | 会话精简元信息 |
| `DELETE` | `/sessions/{sid}` | 删除会话（CASCADE 删 messages） |
| `PATCH` | `/sessions/{sid}` | 修改 title |
| `GET` | `/v1/models` | Provider 健康状态 |
| `GET` | `/health` | 健康检查 + DB 统计 |

完整 Swagger：`http://localhost:8000/docs`

## ✅ 验收清单（来自 Implementation Plan 2A+2B）

| # | 验收项 | 标准 | 状态 |
|---|---|---|---|
| 1 | 重启不丢数据 | DB 文件落地 `data/stage2.db` | ✅ |
| 2 | 对话历史可查 | `GET /sessions/{sid}/messages` 返回完整字段 | ✅ |
| 3 | 对话历史翻页 | `GET /sessions?page=1&page_size=20` | ✅ |
| 4 | 对话历史搜索 | 命中 title 或任意消息 content | ✅ |
| 5 | 自动 title | 首条 user 消息前 30 字 | ✅ |
| 6 | CASCADE 删除 | 删 session → 自动删 messages | ✅ |
| 7 | reasoning 持久化 | DeepSeek-R1 / o1 思考过程存 DB | ✅ |
| 8 | fallback 标记 | is_fallback 字段 + 前端徽章 | ✅ |

## 🛠️ 实现要点

### SQLAlchemy 2.x ORM

- 用 `Mapped[]` 类型注解（现代写法，类型安全）
- `relationship(lazy='selectin')` 避免 N+1 查询
- `Base.metadata.create_all(engine)` 启动时建表（Stage 3+ 接 Alembic 迁移）
- 数据库事件 `PRAGMA foreign_keys=ON` 启用外键约束（SQLite 默认关闭）

### 数据层架构

```
main.py              ← FastAPI endpoint（Depends(get_db)）
  ↓
repository.py        ← SessionRepository（业务封装）
  ↓
models.py            ← SQLAlchemy ORM（Session / Message）
  ↓
database.py          ← Engine + SessionLocal（连接管理）
```

业务层不直接写 SQL，全部走 repository。这样后期切 PostgreSQL 只改 `DB_URL`。

### 搜索策略

```sql
-- 命中 title OR 任一 message.content
WHERE sessions.title LIKE '%kw%'
   OR sessions.id IN (
     SELECT DISTINCT session_id FROM messages WHERE content LIKE '%kw%'
   )
```

- SQLite `LIKE` 10 万级以下没问题
- 上百万级换 FTS5 虚拟表（SQLite 原生）或外部 ES

### 切 PostgreSQL

```bash
# 1. 装驱动
pip install psycopg2-binary

# 2. 改 .env
STAGE2_DB_URL=postgresql+psycopg2://user:pass@localhost:5432/verticalagent

# 3. 启动（自动建表）
python main.py
```

代码层不动。SQLAlchemy 自动处理方言差异。

## 📁 文件结构

```
stage2/
├── main.py              # FastAPI 入口（800 行）
├── database.py          # SQLAlchemy 引擎 + Session 工厂
├── models.py            # Session / Message / Document / Chunk ORM
├── repository.py        # 业务层 CRUD 封装（Session / Document / Chunk）
├── embedding.py         # 🆕 Stage 2C: Embedding API 客户端（OpenAI 兼容）
├── rag.py               # 🆕 Stage 2C: 切片 + cosine 检索 + prompt 注入
├── requirements.txt
├── .env.example
├── data/                # 运行时生成
│   └── stage2.db        # SQLite 数据库（首次启动自动创建）
└── static/
    └── index.html       # 前端（侧栏加搜索 + 加载更多 + 历史回填 + 🆕 文档库 tab）
```

## 🆕 Stage 2C 数据模型

### `documents` 表

| 列 | 类型 | 说明 |
|---|---|---|
| `id` | INTEGER (PK) | autoincrement |
| `title` | TEXT | 文档标题 |
| `source` | TEXT | 来源（文件名/URL/manual） |
| `doc_type` | TEXT | `text` / `file` / `url` |
| `chunk_count` | INTEGER | 切片数量 |
| `embedding_provider` / `embedding_model` / `embedding_dim` | TEXT/INT | 记录向量来源（未来混用不同模型时区分） |
| `created_at` | REAL | unix timestamp，INDEX |
| `metadata_json` | TEXT | 备用 JSON |

### `chunks` 表

| 列 | 类型 | 说明 |
|---|---|---|
| `id` | INTEGER (PK) | autoincrement |
| `document_id` | INTEGER (FK) | `ON DELETE CASCADE` |
| `chunk_index` | INTEGER | 文档内顺序 |
| `content` | TEXT | 切片文本 |
| `embedding` | TEXT | **JSON 序列化的 float[]**（1536-2048 维，5-30KB/条） |
| `token_count` | INTEGER | 估算的 token 数 |
| `created_at` | REAL | unix timestamp |

**索引**：
- `documents.created_at` —— 文档列表按时间倒序
- `chunks.document_id` —— 按文档查所有切片
- `chunks.(document_id, chunk_index)` —— 复合索引，检索后回查

## 🆕 Stage 2C API

### 上传文档

```
POST /documents
{
  "title": "RAG 入门教程",
  "content": "完整文档正文...",
  "source": "manual",
  "doc_type": "text",
  "chunk_size": 500,
  "overlap": 50
}
```

**响应**：
```json
{
  "id": 1,
  "title": "RAG 入门教程",
  "source": "manual",
  "doc_type": "text",
  "chunk_count": 3,
  "embedding_provider": "openai",
  "embedding_model": "text-embedding-3-small",
  "embedding_dim": 1536,
  "created_at": 1782650618.92
}
```

**流水线**：切片（chunk_text）→ 批量 embedding → 写入 documents + chunks。

### 列出 / 删除文档

```
GET /documents?page=1&page_size=20&q=keyword
DELETE /documents/{id}
```

### 纯向量检索

```
POST /search
{
  "query": "什么是 RAG",
  "top_k": 5,
  "min_score": 0.0
}
```

**响应**：`[SearchHit]`，每条含 chunk_id / document_title / content / score。
起步阶段全表扫描所有 chunks（numpy 算 cosine）。10 万级以下 < 200ms。

### RAG 对话

```
POST /chat/rag
{
  "message": "什么是 RAG？",
  "session_id": "可选",
  "top_k": 5,
  "min_score": 0.0,
  "provider": "deepseek",
  "temperature": 0.7
}
```

**SSE 事件流**：
1. `event: start` —— `{"session_id", "primary_provider", "rag_chunks": 5, "rag_sources": [{"doc": "...", "score": 0.87}]}`
2. `event: reasoning_delta` —— 思考过程（如有）
3. `event: content_delta` —— 增量输出
4. `event: done` —— 完成，`{"usage", "reasoning", "rag_chunks"}`

**System prompt 注入模板**：

```
你是 VerticalAgent，一个基于检索增强生成（RAG）的助手。

请严格根据下面提供的「参考资料」回答用户问题。

参考资料：
[1] (来自 RAG 入门教程) 相似度=0.87
RAG 是检索增强生成……

[2] (来自 向量库选型) 相似度=0.76
……
```

## 🚧 不在 Stage 2 范围

- ❌ 多租户隔离（Stage 2D）
- ❌ Milvus / 向量库（10 万级再做；Stage 2E）
- ❌ Skills / MCP（阶段 3）
- ❌ 鉴权 / 登录

## 下一步

**Stage 2D**：多租户隔离（tenant_id + X-API-Key + 隔离自动化测试）。
**Stage 2E**：Milvus 接入（1 Collection + 1 Partition 模式 + 压测到 10 万级 P99 < 500ms）。

## 许可

仅作个人学习与求职展示用。