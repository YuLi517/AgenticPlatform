"""
database.py —— SQLAlchemy 2.x 引擎 + Session 工厂
=================================================

Stage 2 数据层入口：
    - SQLite 文件存到 data/stage2.db（自动创建 data/ 目录）
    - engine 进程级单例
    - SessionLocal() 创建会话（FastAPI Depends 用法）

为什么 SQLAlchemy 2.x ORM（不是裸 SQL，也不是 Django ORM）：
    - 工业级标准，DJango 之外的另一极
    - 后期切 PostgreSQL 只需改一行 URL（mysql+pymysql / postgresql+psycopg2）
    - ORM 类型提示友好（IDE 自动补全）
    - Stage 3+ 加 RAG/多租户时，迁移工具齐全（Alembic）
"""

import os
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

# ============== 数据库路径 ==============

DB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(DB_DIR, exist_ok=True)

DB_PATH = os.path.join(DB_DIR, "stage2.db")
DB_URL = f"sqlite:///{DB_PATH}"

# 便于测试 / 演示覆盖：环境变量 STAGE2_DB_URL 优先
_env_url = os.getenv("STAGE2_DB_URL")
if _env_url:
    DB_URL = _env_url
    DB_PATH = None  # 外部数据库，data/ 目录没意义

# ============== 引擎 ==============

_engine_kwargs = {
    "echo": False,           # True 时打印 SQL（调试用）
    "pool_pre_ping": True,   # 每次连接前 ping，避免 SQLite stale
}

# SQLite 专属：允许多线程访问（FastAPI 异步 + 同步 SQLAlchemy 需要）
if DB_URL.startswith("sqlite"):
    _engine_kwargs["connect_args"] = {"check_same_thread": False}

    # 启用 SQLite 外键约束（默认关闭！sessions.id → messages.session_id 的 CASCADE 需要它）
    @event.listens_for(__import__("sqlalchemy").engine.Engine, "connect")
    def _enable_sqlite_fk(dbapi_connection, connection_record):
        if DB_URL.startswith("sqlite"):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

engine = create_engine(DB_URL, **_engine_kwargs)

# ============== Session 工厂 ==============

SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,  # commit 后属性不失效，方便返回数据
)


def get_db():
    """FastAPI Depends 用法：
        def my_endpoint(db: Session = Depends(get_db)):
            ...
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    """创建所有表（首次启动时调用）"""
    from models import Base  # 避免循环导入
    Base.metadata.create_all(bind=engine)


# ============== 启动自检 ==============

if __name__ == "__main__":
    # 直接 python database.py 时：打印连接信息 + 建表
    init_db()
    print(f"✅ DB initialized: {DB_PATH or DB_URL}")