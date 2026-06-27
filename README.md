# VerticalAgent Stage 0 —— 对话 Demo

> 基于 [《VerticalAgent 平台工业级设计文档 v1.4.5》](../VerticalAgent平台工业级设计文档_v1.4.5.docx) 的
> [《Implementation Plan》](../Implementation_Plan.docx) **阶段 0** 完整可跑代码。

[![GitHub stars](https://img.shields.io/github/stars/YuLi517/AgenticPlatform?style=flat-square)](https://github.com/YuLi517/AgenticPlatform/stargazers)
[![GitHub forks](https://img.shields.io/github/forks/YuLi517/AgenticPlatform?style=flat-square)](https://github.com/YuLi517/AgenticPlatform/network)
[![License](https://img.shields.io/github/license/YuLi517/AgenticPlatform?style=flat-square)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue?style=flat-square)](https://www.python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110%2B-009688?style=flat-square)](https://fastapi.tiangolo.com)
[![CI](https://img.shields.io/github/actions/workflow/status/YuLi517/AgenticPlatform/ci.yml?style=flat-square)](https://github.com/YuLi517/AgenticPlatform/actions)

## ⚡ 30 秒快速开始

```bash
# 1. 克隆
git clone https://github.com/YuLi517/AgenticPlatform.git
cd AgenticPlatform

# 2. 装依赖（需要 Python 3.10+）
pip install -r requirements.txt

# 3. 配 API Key
cp .env.example .env
# 编辑 .env 填入 LLM_API_KEY（推荐 DeepSeek）

# 4. 启动
python main.py

# 5. 浏览器打开 http://localhost:8000
```

> 详细文档看下方「快速启动」一节；遇到问题看「常见问题」。

---
## 这是什么

一个**单租户**的 Web 对话 Demo，**30 分钟内**能跑起来：
- Web 界面可对话
- 多轮对话（保留 5 轮上下文）
- 每次请求打印 Token 用量 + 延迟
- 4 类异常有友好兜底（限流 / 超时 / 网络 / 鉴权）
- 单 LLM Provider（DeepSeek 兼容 OpenAI 协议）

## 目录结构

```
stage0/
├── main.py              # 后端（FastAPI + OpenAI SDK）
├── static/
│   └── index.html       # 前端（单文件 HTML + 原生 JS）
├── .env.example         # 环境变量模板
└── README.md            # 本文件
```

## 快速启动

### 1. 装依赖

```bash
pip install fastapi uvicorn openai pydantic python-dotenv
```

### 2. 配环境变量

```bash
# 复制模板
cp .env.example .env

# 编辑 .env，填入真实 API Key
# 推荐 DeepSeek（中文强 + 性价比高）
LLM_API_KEY=sk-你的真实key
```

### 3. 启动

```bash
python main.py
```

### 4. 访问

打开浏览器 → http://localhost:8000

## API 接口

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/` | 返回前端页面 |
| GET | `/health` | 健康检查 |
| POST | `/chat` | 核心对话（带 session_id 保持上下文） |
| GET | `/sessions/{id}` | 查询会话历史 |
| DELETE | `/sessions/{id}` | 清空会话 |
| GET | `/sessions` | 列出所有 session（调试） |
| GET | `/docs` | Swagger API 文档 |

### POST /chat 示例

**请求**（query string 或 JSON body）：

```
POST /chat?message=你好&session_id=可选
```

或 JSON：

```json
{
  "message": "你好",
  "session_id": "可选",
  "temperature": 0.7
}
```

**响应**：

```json
{
  "session_id": "abc12345-...",
  "reply": "你好！我是基于 LLM 的对话助手...",
  "usage": {
    "prompt_tokens": 523,
    "completion_tokens": 187,
    "total_tokens": 710
  },
  "latency_ms": 1820,
  "model": "deepseek-chat"
}
```

## 验收清单

| # | 验收项 | 标准 | 状态 |
|---|---|---|---|
| 1 | Web 界面可对话 | 浏览器打开 / 能发能收 | ✅ |
| 2 | 多轮对话 | 5 轮上下文不丢 | ✅ |
| 3 | LLM API 调用 | 任一 OpenAI 兼容 API | ✅ |
| 4 | 首字延迟 | < 2s（取决于 Provider） | ⚠️ 阶段 0 整段返回 |
| 5 | Token 用量日志 | 每次请求 stdout 打日志 | ✅ |
| 6 | 错误兜底 | 4 类异常有友好提示 | ✅ |
| 7 | 单租户 | 不需要登录 | ✅ |
| 8 | Session 存储 | 内存 Map（重启丢） | ✅ |
| 9 | 问答质量 | 主观 4/5 分 | ⚠️ 取决于模型 |

## Token 用量日志示例

每次请求会在终端打印：

```
[2026-06-27 10:30:15] INFO 📊 session=abc12345... 
                       model=deepseek-chat 
                       prompt=12 completion=156 total=168 
                       latency=1.20s attempt=1
```

字段说明：
- `session`：会话 ID 前 8 位
- `model`：实际调用的模型
- `prompt / completion / total`：Token 拆分
- `latency`：端到端响应延迟
- `attempt`：重试次数（1 = 一次成功）

## 错误兜底矩阵

| 异常类型 | 触发场景 | 行为 | 用户感知 |
|---|---|---|---|
| `RateLimitError` (429) | 调用频率过高 | 指数退避重试 2 次 | 短暂等待后重试 |
| `APITimeoutError` | 网络超时 > 30s | 重试 2 次 | "请求超时，请检查网络" |
| `APIConnectionError` | DNS / 网络断开 | 重试 2 次 | "网络连接失败" |
| `AuthenticationError` (401) | API Key 无效 | **不重试** | "API Key 无效，请检查 .env" |
| `BadRequestError` (400) | 内容审核 / 上下文过长 | **不重试** | "请求被拒绝" |
| 其他 | 未知 | 不重试 | "未知错误: xxx" |

## 阶段 0 明确不做的

这些是设计文档里有、但**阶段 0 不做**的：

- ❌ 数据库（内存 Map 存 session）
- ❌ 多租户隔离
- ❌ RAG / 知识库
- ❌ 多模型路由（阶段 1 加）
- ❌ 用户鉴权 / 登录
- ❌ 流式响应 SSE（阶段 1 加）
- ❌ Skills / MCP（阶段 3 加）
- ❌ 可观测性 / 监控（阶段 4 加）
- ❌ 计费 / 商业化（阶段 5 加）

## 阶段 1 升级预告

| 模块 | 阶段 0 → 阶段 1 |
|---|---|
| `Settings` | 加 `models: List[dict]` 支持多模型 |
| `SessionManager` | 内存 dict → PostgreSQL + Redis |
| `call_llm_with_retry` | 单 Provider → 多 Provider 路由 + 熔断 |
| `/chat` | 加 `model` 参数 + 租户识别 |
| 新增 | SSE 流式响应 |
| 新增 | `/v1/models` 列出可用模型 |
| 新增 | `/usage` 租户用量统计 |

## 常见问题

### Q1: 启动报错 "LLM_API_KEY 未设置"
答：没复制 `.env.example` 为 `.env`，或者 `.env` 里没填真实 key。

### Q2: 浏览器打开 127.0.0.1:8000 显示 "请将前端 index.html 放到 static/ 目录"
答：说明你执行 `python` 的**当前目录**不是 `stage0/`，或者 `static/index.html` 不在。
解决：`cd stage0 && python main.py`

### Q3: 中文显示乱码
答：终端不是 UTF-8 编码。在 Windows PowerShell 里跑 `chcp 65001` 切到 UTF-8。

### Q4: 想换其他 LLM（Qwen / Moonshot / GPT-4o）
答：改 `.env` 三行：
```bash
LLM_PROVIDER=qwen
LLM_API_KEY=sk-你的qwen-key
LLM_BASE_URL=https://dashscope.aliyuncs.com/compatible-mode/v1
LLM_MODEL=qwen-plus
```

### Q5: 想要流式响应（一个字一个字显示）
答：阶段 1 加。先用当前整段返回版本体验一下业务流程。

## 关联文档

- 父级设计：[《VerticalAgent 平台工业级设计文档 v1.4.5》](../VerticalAgent平台工业级设计文档_v1.4.5.docx) (218KB)
- 实施路线：[《Implementation Plan》](../Implementation_Plan.docx) (47KB)
- 仓库入口：[README.md](../README.md)

## 许可

仅作个人学习与求职展示用。
