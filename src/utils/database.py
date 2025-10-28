"""
Database management utilities for the Australian company pipeline.
Async PostgreSQL implementation compatible with the pipeline's awaited calls.
"""

import asyncio
import logging
from typing import List, Dict, Any, Optional, Iterable
from contextlib import asynccontextmanager
import json

import asyncpg

logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    Async PostgreSQL database manager using asyncpg.
    Provides simple helpers for executing queries and bulk operations.
    """

    def __init__(self, database_url: str):
        self.database_url = database_url
        self.pool: Optional[asyncpg.Pool] = None

    async def initialize(self):
        """Initialize connection pool and test connectivity."""
        if self.pool is None:
            self.pool = await asyncpg.create_pool(dsn=self.database_url, min_size=1, max_size=10)
        async with self.pool.acquire() as conn:
            await conn.execute("SELECT 1")
        logger.info("PostgreSQL connection pool initialized")

    async def close(self):
        """Close the pool."""
        if self.pool is not None:
            await self.pool.close()
            self.pool = None

    @asynccontextmanager
    async def connection(self):
        if self.pool is None:
            await self.initialize()
        assert self.pool is not None
        conn = await self.pool.acquire()
        try:
            yield conn
        finally:
            await self.pool.release(conn)

    async def execute(self, query: str, params: Optional[Dict[str, Any]] = None) -> str:
        """Execute a statement returning status string."""
        async with self.connection() as conn:
            statement, args = self._prepare_query(query, params)
            return await conn.execute(statement, *args)

    async def fetch_all(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Fetch all rows as list of dicts."""
        async with self.connection() as conn:
            statement, args = self._prepare_query(query, params)
            rows = await conn.fetch(statement, *args)
            return [dict(r) for r in rows]

    async def fetch_one(self, query: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """Fetch a single row as dict or None."""
        async with self.connection() as conn:
            statement, args = self._prepare_query(query, params)
            row = await conn.fetchrow(statement, *args)
            return dict(row) if row else None

    async def bulk_insert(self, table: str, records: List[Dict[str, Any]], batch_size: int = 1000) -> int:
        """Bulk insert using executemany for small-to-medium batches."""
        if not records:
            return 0
        columns = list(records[0].keys())
        placeholders = ", ".join([f"${i+1}" for i in range(len(columns))])
        col_list = ", ".join(columns)
        sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"

        async with self.connection() as conn:
            total = 0
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                args = [tuple(self._adapt_value(rec.get(c)) for c in columns) for rec in batch]
                await conn.executemany(sql, args)
                total += len(batch)
        logger.info(f"Bulk insert complete: {len(records)} records into {table}")
        return total

    async def bulk_upsert(
        self,
        table: str,
        records: List[Dict[str, Any]],
        conflict_columns: List[str],
        update_columns: Optional[List[str]] = None,
        batch_size: int = 1000,
    ) -> int:
        """Bulk upsert using ON CONFLICT DO UPDATE."""
        if not records:
            return 0
        columns = list(records[0].keys())
        if update_columns is None:
            update_columns = [c for c in columns if c not in conflict_columns]
        insert_cols = ", ".join(columns)
        placeholders = ", ".join([f"${i+1}" for i in range(len(columns))])
        conflict_cols = ", ".join(conflict_columns)
        update_set = ", ".join([f"{c}=EXCLUDED.{c}" for c in update_columns])
        sql = (
            f"INSERT INTO {table} ({insert_cols}) VALUES ({placeholders}) "
            f"ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_set}"
        )

        async with self.connection() as conn:
            total = 0
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                args = [tuple(self._adapt_value(rec.get(c)) for c in columns) for rec in batch]
                await conn.executemany(sql, args)
                total += len(batch)
        logger.info(f"Bulk upsert complete: {len(records)} records in {table}")
        return total

    async def execute_sql_file(self, file_path: str):
        """Execute semicolon-delimited SQL from a file."""
        with open(file_path, 'r') as f:
            sql_content = f.read()
        statements = [s.strip() for s in sql_content.split(';') if s.strip()]
        async with self.connection() as conn:
            for stmt in statements:
                await conn.execute(stmt)
        logger.info(f"Executed SQL file: {file_path}")

    def _prepare_query(self, query: str, params: Optional[Dict[str, Any]]):
        """
        Support both positional ($1) and named-style placeholders in codebase.
        If params is provided as dict with keys, map to $1..$n in order of appearance of :key or $key.
        """
        if not params:
            return query, []

        # Support simple mapping for queries using $1 style already
        if any(f"${i}" in query for i in range(1, 10)) and not any(":" in query for _ in [0]):
            # Caller passed dict; preserve order by common keys if present
            args = list(params.values())
            return query, args

        # Map :name occurrences to $1..$n deterministically by first appearance
        ordered_keys: List[str] = []
        out = []
        i = 1
        j = 0
        while j < len(query):
            if query[j] == ':' and j + 1 < len(query) and (query[j+1].isalpha() or query[j+1] == '_'):
                k = j + 2
                while k < len(query) and (query[k].isalnum() or query[k] == '_'):
                    k += 1
                key = query[j+1:k]
                if key not in ordered_keys:
                    ordered_keys.append(key)
                out.append(f"${ordered_keys.index(key)+1}")
                j = k
            else:
                out.append(query[j])
                j += 1
        args = [params.get(k) for k in ordered_keys]
        return "".join(out), args

    def _adapt_value(self, value: Any) -> Any:
        """Adapt Python values for insertion (e.g., dicts/lists to JSON)."""
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        return value


# Simple self-test
if __name__ == "__main__":
    async def main():
        import os
        dsn = os.getenv("DATABASE_URL", "postgresql://postgres:postgres_password@localhost:5432/australian_companies")
        db = DatabaseManager(dsn)
        await db.initialize()
        await db.execute("CREATE SCHEMA IF NOT EXISTS logs")
        row = await db.fetch_one("SELECT 1 as ok")
        print(row)
        await db.close()

    asyncio.run(main())