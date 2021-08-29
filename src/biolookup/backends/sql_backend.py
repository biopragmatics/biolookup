# -*- coding: utf-8 -*-

"""An BLS backend that communicates with a SQL database."""

import logging
import time
from collections import Counter
from functools import lru_cache
from typing import Optional, Union

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from .backend import Backend
from ..constants import ALTS_TABLE_NAME, DEFS_TABLE_NAME, REFS_TABLE_NAME, get_sqlalchemy_uri

__all__ = [
    "RawSQLBackend",
]

logger = logging.getLogger(__name__)


def _ensure_engine(engine: Union[None, str, Engine]) -> Engine:
    if engine is None:
        return create_engine(get_sqlalchemy_uri())
    elif isinstance(engine, str):
        return create_engine(engine)
    else:
        return engine


class RawSQLBackend(Backend):
    """A backend that communicates with low-level SQL statements."""

    #: The engine
    engine: Engine

    def __init__(
        self,
        *,
        refs_table: Optional[str] = None,
        alts_table: Optional[str] = None,
        defs_table: Optional[str] = None,
        engine: Union[None, str, Engine] = None,
    ):
        """Initialize the raw SQL backend.

        :param refs_table: A name for the references (prefix-id-name) table. Defaults to 'obo_reference'
        :param alts_table: A name for the alts (prefix-id-alt) table. Defaults to 'obo_alt'
        :param defs_table: A name for the defs (prefix-id-def) table. Defaults to 'obo_def'
        :param engine: An engine, connection string, or None if you want the default.
        """
        self.engine = _ensure_engine(engine)

        self.refs_table = refs_table or REFS_TABLE_NAME
        self.alts_table = alts_table or ALTS_TABLE_NAME
        self.defs_table = defs_table or DEFS_TABLE_NAME

    @lru_cache(maxsize=1)
    def count_names(self) -> int:
        """Get the number of names."""
        return self._get_one(
            f"SELECT SUM(identifier_count) FROM {self.refs_table}_summary;"  # noqa:S608
        )

    @lru_cache(maxsize=1)
    def count_prefixes(self) -> int:
        """Count prefixes using a SQL query to the references summary table."""
        logger.info("counting prefixes")
        start = time.time()
        rv = self._get_one(
            f"SELECT COUNT(DISTINCT prefix) FROM {self.refs_table}_summary;"  # noqa:S608
        )
        logger.info("done counting prefixes after %.2fs", time.time() - start)
        return rv

    @lru_cache(maxsize=1)
    def count_definitions(self) -> int:
        """Count definitions using a SQL query to the definitions summary table."""
        return self._get_one(
            f"SELECT SUM(identifier_count) FROM {self.defs_table}_summary;"  # noqa:S608
        )

    @lru_cache(maxsize=1)
    def count_alts(self) -> Optional[int]:
        """Count alts using a SQL query to the alts summary table."""
        logger.info("counting alts")
        return self._get_one(f"SELECT COUNT(*) FROM {self.alts_table};")  # noqa:S608

    def _get_one(self, sql: str):
        with self.engine.connect() as connection:
            result = connection.execute(sql).fetchone()
            return result[0]

    def summarize_names(self) -> Counter:
        """Return the results of a SQL query that dumps the name summary table."""
        return self._get_summary(self.refs_table)

    def summarize_alts(self) -> Counter:
        """Return the results of a SQL query that dumps the alts summary table."""
        return self._get_summary(self.alts_table)

    def summarize_definitions(self) -> Counter:
        """Return the results of a SQL query that dumps the definitions summary table."""
        return self._get_summary(self.defs_table)

    @lru_cache()
    def _get_summary(self, table) -> Counter:
        sql = f"SELECT prefix, identifier_count FROM {table}_summary;"  # noqa:S608
        with self.engine.connect() as connection:
            return Counter(dict(connection.execute(sql).fetchall()))

    @lru_cache()
    def has_prefix(self, prefix: str) -> bool:
        """Check for the prefix with a SQL query."""
        sql = text(
            f"SELECT EXISTS(SELECT 1 from {self.refs_table} WHERE prefix = :prefix);"  # noqa:S608
        )
        with self.engine.connect() as connection:
            result = connection.execute(sql, prefix=prefix).fetchone()
            return bool(result)

    @lru_cache(maxsize=100_000)
    def get_primary_id(self, prefix: str, identifier: str) -> str:
        """Get the canonical identifier with a SQL query to the alts table."""
        sql = text(
            f"""
            SELECT identifier
            FROM {self.alts_table}
            WHERE prefix = :prefix and alt = :alt;
        """  # noqa:S608
        )
        with self.engine.connect() as connection:
            result = connection.execute(sql, prefix=prefix, alt=identifier).fetchone()
            return result[0] if result else identifier

    def get_name(self, prefix: str, identifier: str) -> Optional[str]:
        """Get the name with a SQL query to the names table."""
        return self._help_one(self.refs_table, "name", prefix, identifier)

    def get_definition(self, prefix: str, identifier: str) -> Optional[str]:
        """Get the definition with a SQL query to the definitions table."""
        return self._help_one(self.defs_table, "definition", prefix, identifier)

    @lru_cache(maxsize=100_000)
    def _help_one(self, table: str, column: str, prefix: str, identifier: str) -> Optional[str]:
        sql = text(
            f"""
            SELECT {column}
            FROM {table}
            WHERE prefix = :prefix and identifier = :identifier;
        """
        )  # noqa:S608
        with self.engine.connect() as connection:
            result = connection.execute(sql, prefix=prefix, identifier=identifier).fetchone()
            if result:
                return result[0]
        return None