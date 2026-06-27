# VerticalAgent Stage 1 —— 多 Provider 路由 + SSE 流式响应

> 基于 [《VerticalAgent 平台工业级设计文档 v1.4.5》](../../../VerticalAgent平台工业级设计文档_v1.4.5.docx)
> [《Implementation Plan》](../../../Implementation_Plan.docx) **阶段 1** 完整可跑代码。

## 与 stage0 的差异

| 维度 | stage0 | **stage1** |
|---|---|---|
| Provider 数量 | 1 个 | **多 Provider + 自动 fallback** |
| 响应方式 | 整段返回 | **SSE 流式（一个字一个字显示）** |
| 错误处理 | 简单重试 | **熔断器（CLOSED/OPEN/HALF_OPEN）+ fallback 链** |
| 健康检查 | 无 | **`/v1/models` 列出 Provider 状态** |
| API | `/chat` | `/chat` + `/chat/stream` |

## 这是什么

一个**多 LLM Provider 路由**的对话 Demo：

- **多 Provider 配置**：DeepSeek + Qwen + Moonshot + ... 任选
- **自动 fallback**：主 Provider 失败 → 自动尝试下一个
- **SSE 流式响应**：用 Server-Sent Events 推送到前端
- **熔断器**：Provider 连续失败 N 次 → 暂时跳过 → 60s 后探测
- **路由策略**：按 `model` / `provider` 参数选，或按配置优先级

## 快速启动

### 1. 装依赖

```bash
pip install -r requirements.txt
```

### 2. 配 API Key

```bash
cp .env.example .env
# 编辑 .env，至少填一个 Provider 的 API Key
# 至少 DEEPSEEK_API_KEY=sk-xxx
# 推荐同时配置 DEEPSEEK + QEN，做 fallback 演示
```

### 3. 启动

```bash
python main.py
```

### 4. 访问

打开 http://localhost:8000

## API 列表

| 方法 | 路径 | 用途 | 响应 |
|---|---|---|---|
| GET | `/` | 前端页面 | HTML |
| GET | `/health` | 健康检查 | JSON |
| GET | `/v1/models` | **Provider 健康状态** | JSON |
| POST | `/chat` | **非流式对话** | JSON |
| POST | `/chat/stream` | **SSE 流式对话** | text/event-stream |
| GET | `/sessions/{id}` | 查询会话 | JSON |
| DELETE | `/sessions/{id}` | 清空会话 | JSON |
| GET | `/sessions` | 列出所有 session | JSON |

## POST /chat（非流式）

### 请求

```json
{
  "message": "你好",
  "session_id": "可选",
  "provider": "可选，如 deepseek",
  "model": "可选，如 deepseek-chat",
  "temperature": 0.7,
  "system_prompt": "可选"
}
```

### 响应

```json
{
  "session_id": "abc-123",
  "reply": "你好！我是基于 LLM 的对话助手...",
  "provider": "deepseek",
  "model": "deepseek-chat",
  "usage": {"prompt_tokens": 12, "completion_tokens": 156, "total_tokens": 168},
  "latency_ms": 1820,
  "attempts": [
    {"provider": "deepseek", "ok": true, "error_type": null},
    {"provider": "qwen", "ok": false, "error_type": "circuit_open"}
  ],
  "fallback_used": false
}
```

`fallback_used: true` 表示主 Provider 失败，fallback 到了下一个。

## POST /chat/stream（SSE 流式）

SSE 事件流：

```
event: start
data: {"session_id": "abc-123", "primary_provider": "deepseek"}

event: delta
data: {"delta": "你", "provider": "deepseek"}

event: delta
data: {"delta": "好", "provider": "deepseek"}

...

event: delta
data: {"delta": "！", "provider": "qwen"}    ← 如果 deepseek 挂了

event: done
data: {"usage": {"completion_tokens": 168}, "provider": "qwen"}
```

## GET /v1/models

```json
{
  "providers": [
    {"provider": "deepseek", "model": "deepseek-chat", "state": "closed", "error_rate": 0, "samples": 5},
    {"provider": "qwen", "model": "qwen-plus", "state": "closed", "error_rate": 0.2, "samples": 10}
  ]
}
```

`state` 含义：
- `closed` ✅ 正常
- `open` 🔴 熔断中（拒绝请求）
- `half_open` 🟡 探测中（放一个请求试）

## 熔断器机制

```
        连续失败 ≥ 5 次
CLOSED ─────────────────→ OPEN
   ↑                        │
   │ 探测成功               │ 60s 后
   │                        ▼
   └────── HALF_OPEN ──────┘
              │ 探测失败
              └─→ 重新 OPEN
```

每个 Provider 独立熔断器，互不影响。

## Fallback 链

按 `LLM_PROVIDERS` 配置顺序：

```
LLM_PROVIDERS=deepseek,qwen
```

请求流程：
```
1. 用户调 /chat
2. 选 deepseek (primary)
3. deepseek 失败（429/timeout/network）→ 熔断器记一次
4. 如果熔断器未 OPEN → 重试 deepseek（最多 2 次）
5. 仍失败 → 切到 qwen (fallback)
6. qwen 成功 → 返回，attempts=[deepseek❌, qwen✅]
```

**注意**：auth_error（401）和 bad_request（400）不触发 fallback —— 这些是配置/输入问题，重试无意义。

## 验收清单

| # | 验收项 | 状态 |
|---|---|---|
| 1 | 多 Provider 路由 | ✅ |
| 2 | 自动 fallback 链 | ✅ |
| 3 | SSE 流式响应 | ✅ |
| 4 | 熔断器状态机 | ✅ |
| 5 | 熔断器自动恢复 | ✅（60s 后） |
| 6 | Token 计量 + 日志 | ✅ |
| 7 | 4 类错误兜底 | ✅ |
| 8 | 多轮对话 | ✅（继承 stage0） |
| 9 | 健康检查 `/v1/models` | ✅ |

## 演示场景

### 场景 1：正常对话

```bash
curl -X POST http://localhost:8000/chat \
  -H "Content-Type: application/json" \
  -d '{"message": "你好"}'
```

### 场景 2：SSE 流式

```bash
curl -N -X POST http://localhost:8000/chat/stream \
  -H "Content-Type: application/json" \
  -d '{"message": "写一个短故事"}'
```

### 场景 3：指定 Provider

```bash
curl -X POST http://localhost:8000/chat \
  -H "Content-Type: application/json" \
  -d '{"message": "你好", "provider": "qwen"}'
```

### 场景 4：测试 fallback

让 deepseek 触发 429（用 fake key 或手动 kill），请求会 fallback 到 qwen：

```bash
# 在 .env 中把 DEEPSEEK_API_KEY 改成无效的
DEEPSEEK_API_KEY=sk-fake-invalid-key

# 启动后会看到
# [2026-06-27 10:30:15] 🔑 [deepseek] auth failed
# [2026-06-27 10:30:15] ✅ [qwen/qwen-plus] prompt=12 completion=156 latency=1.20s
# attempts=[deepseek❌, qwen✅], fallback_used=true
```

## 阶段 1 明确不做的

- ❌ 数据库（仍是内存 Map）
- ❌ 多租户隔离
- ❌ RAG / 知识库（阶段 2）
- ❌ Skills / MCP（阶段 3）
- ❌ 鉴权 / 登录
- ❌ vLLM 自建（用户决定用 API）
- ❌ 可观测性 / 监控（阶段 4）
- ❌ 计费 / 商业化（阶段 5）

## 常见问题

### Q1: 启动报错 "Provider 'xxx' 配置不完整"
答：`.env` 里没填该 Provider 的 3 个环境变量（API_KEY / BASE_URL / MODEL）。
最少要有一个 Provider 完整配置。

### Q2: 浏览器 SSE 显示不出流式
答：用最新 Chrome / Firefox / Edge 浏览器。SSE 兼容性：Chrome 6+ / Firefox 6+ / Edge 79+。

### Q3: 切换流式/整段
答：点击右上角"📡 流式 ON"按钮可切换。两种模式都用同一个 `/chat` 或 `/chat/stream` 端点。

### Q4: 熔断器一直 OPEN
答：等 60s（默认 `RECOVERY_TIME`）后会自动进入 HALF_OPEN 探测。如果一直失败，可以调大 `FAILURE_THRESHOLD` 或减小 `RECOVERY_TIME`。

## 关联文档

> 仓库内引用了 `D:\Projects\Resume\` 下的设计文档（218KB）和 Implementation Plan（47KB），
> 这些文档不在 Git 仓库内（太大且为个人作品集），需要本地访问。
> 想看完整设计可访问项目主页 https://github.com/YuLi517/AgenticPlatform

- 父级设计：[《VerticalAgent 平台工业级设计文档 v1.4.5》](../README.md#📐-架构设计)
- 实施路线：见仓库根 [README.md](../README.md)
- stage0：[README.md](../README.md#stage-0--对话-demo)
- 仓库入口：[../README.md](../README.md)

## 许可

MIT License - Copyright (c) 2026 Justin Li (李宇)
