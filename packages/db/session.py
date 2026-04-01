from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import StaticPool
from packages.core.config import settings

# Use the standard synchronous SQLite driver which is 100% stable on Windows
sync_url = settings.database.url.replace("+aiosqlite", "")

_engine = create_engine(
    sync_url,
    poolclass=StaticPool,
    connect_args={"check_same_thread": False}
)

# Enable WAL mode for safety
with _engine.connect() as conn:
    conn.exec_driver_sql("PRAGMA journal_mode=WAL")

_session_factory = sessionmaker(bind=_engine, expire_on_commit=False, autoflush=False)

import asyncio

class DummyAsyncSession:
    """
    A wrapper around a synchronous SQLAlchemy Session that exposes an async interface.
    Uses asyncio.to_thread to offload blocking SQLite calls to a background thread,
    ensuring the FastAPI event loop remains responsive during heavy sync cycles.
    """
    def __init__(self, sync_session: Session):
        self._sync_session = sync_session

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await asyncio.to_thread(self._sync_session.close)

    async def execute(self, *args, **kwargs):
        return await asyncio.to_thread(self._sync_session.execute, *args, **kwargs)

    async def merge(self, *args, **kwargs):
        return await asyncio.to_thread(self._sync_session.merge, *args, **kwargs)

    async def commit(self):
        await asyncio.to_thread(self._sync_session.commit)

    async def rollback(self):
        await asyncio.to_thread(self._sync_session.rollback)
        
    async def refresh(self, *args, **kwargs):
        await asyncio.to_thread(self._sync_session.refresh, *args, **kwargs)

    def add(self, *args, **kwargs):
        # add() is a metadata-only operation in SQLAlchemy and doesn't hit the DB
        # until flush/commit, so it can remain synchronous.
        self._sync_session.add(*args, **kwargs)
        
    def close(self):
        self._sync_session.close()

def AsyncSessionLocal():
    """Returns the dummy async session."""
    return DummyAsyncSession(_session_factory())



def _ensure_columns(engine):
    """
    Add any ORM-defined columns that are absent from an existing DB.
    Handles tables created by an older Alembic migration before new columns
    were added to the models (e.g. gamma_score on trader_profiles).
    SQLite supports ADD COLUMN but not DROP/MODIFY, so this is safe to run
    on every startup — it's a no-op if the column already exists.
    """
    _migrations = [
        # (table, column, sqlite_type, default_value)
        ("trader_profiles", "gamma_score", "REAL", "0.0"),
        ("trader_profiles", "median_clv",  "REAL", "0.0"),
    ]
    with engine.connect() as conn:
        for table, col, col_type, default in _migrations:
            try:
                conn.exec_driver_sql(
                    f"ALTER TABLE {table} ADD COLUMN {col} {col_type} NOT NULL DEFAULT {default}"
                )
                conn.exec_driver_sql("SELECT 1")  # flush
            except Exception:
                pass  # column already exists — ignore


def init_db():
    """Create all tables defined in models if they don't exist."""
    import packages.db.models  # noqa - ensures all ORM models are registered with Base.metadata
    from packages.db.base import Base
    Base.metadata.create_all(_engine)

    # Safety: add columns that exist in the ORM model but may be absent from a DB
    # that was previously created by an older Alembic migration.
    _ensure_columns(_engine)
