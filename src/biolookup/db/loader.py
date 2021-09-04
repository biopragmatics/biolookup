# -*- coding: utf-8 -*-

"""Upload the Ooh Na Na nomenclature database to PostgreSQL.

After installing with pip, run with: ``biolookup load``.
This will take care of downloading the latest data from Zenodo (you
might need to set up an API key) and loading it into a SQL database.
Use ``--help`` for options on configuration.
"""

import gzip
import io
import logging
import time
from contextlib import closing
from pathlib import Path
from textwrap import dedent
from typing import Optional, Union

import click
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from pyobo.resource_utils import (
    ensure_alts,
    ensure_definitions,
    ensure_ooh_na_na,
    ensure_species,
    ensure_synonyms,
)
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from tabulate import tabulate

from ..constants import (
    ALTS_TABLE_NAME,
    DEFS_TABLE_NAME,
    DERIVED_NAME,
    REFS_TABLE_NAME,
    SPECIES_TABLE_NAME,
    SYNONYMS_NAME,
    get_sqlalchemy_uri,
)

__all__ = [
    "load",
]

logger = logging.getLogger(__name__)


def echo(s, **kwargs) -> None:
    """Wrap echo with time logging."""
    click.echo(f'[{time.strftime("%Y-%m-%d %H:%M:%S")}] ', nl="")
    click.secho(s, **kwargs)


#: Number of test rows if --test is used
TEST_N = 100_000


class Loader:
    """Database loader class."""

    def __init__(
        self,
        uri: Union[None, str, Engine] = None,
        *,
        refs_table: Optional[str] = None,
        alts_table: Optional[str] = None,
        defs_table: Optional[str] = None,
        species_table: Optional[str] = None,
        derived_table: Optional[str] = None,
        synonyms_table: Optional[str] = None,
    ):
        """Load the database.

        :param uri: The URI of the database to connect to.
        :param refs_table: Name of the references table
        :param alts_table: Name of the alts table
        :param defs_table: Name of the definitions table
        :param species_table: Name of the species table
        :param derived_table: Name of the prefix-id-... derived table.
        :param synonyms_table: Name of the synonyms table
        """
        self.engine = _ensure_engine(uri)
        self.alts_table = alts_table or ALTS_TABLE_NAME
        self.refs_table = refs_table or REFS_TABLE_NAME
        self.defs_table = defs_table or DEFS_TABLE_NAME
        self.species_table = species_table or SPECIES_TABLE_NAME
        self.derived_table = derived_table or DERIVED_NAME
        self.synonyms_table = synonyms_table or SYNONYMS_NAME

    def load_alts(
        self,
        *,
        path: Union[None, str, Path] = None,
        test: bool = False,
    ):
        """Load the alternative identifiers table."""
        self._load_table(
            table=self.alts_table,
            path=path if path is not None else ensure_alts(),
            test=test,
            target_col="alt",
            target_col_size=64,
            add_unique_constraints=False,
            add_reverse_index=True,
        )

    def load_definition(
        self,
        *,
        path: Union[None, str, Path] = None,
        test: bool = False,
    ):
        """Load the definitions table."""
        self._load_table(
            table=self.defs_table,
            path=path if path else ensure_definitions(),
            test=test,
            target_col="definition",
            use_varchar=False,
        )

    def load_species(
        self,
        *,
        path: Union[None, str, Path] = None,
        test: bool = False,
    ):
        """Load the species table."""
        self._load_table(
            table=self.species_table,
            path=path if path else ensure_species(),
            test=test,
            target_col="species",
            target_col_size=16,
        )

    def load_name(
        self,
        *,
        path: Union[None, str, Path] = None,
        test: bool = False,
    ):
        """Load the names table."""
        self._load_table(
            table=self.refs_table,
            path=path if path else ensure_ooh_na_na(),
            test=test,
            target_col="name",
            target_col_size=4096,
        )

    def load_synonyms(
        self,
        *,
        path: Union[None, str, Path] = None,
        test: bool = False,
    ):
        """Load the synonyms table."""
        self._load_table(
            table=self.synonyms_table,
            path=path if path else ensure_synonyms(),
            test=test,
            target_col="synonym",
            target_col_size=4096,
            add_unique_constraints=False,
        )

    def _load_table(
        self,
        table: str,
        path: Union[str, Path],
        target_col: str,
        *,
        test: bool = False,
        target_col_size: Optional[int] = None,
        add_unique_constraints: bool = True,
        add_reverse_index: bool = False,
        use_varchar: bool = True,
    ) -> None:
        drop_statement = f"DROP TABLE IF EXISTS {table} CASCADE;"

        if use_varchar:
            if target_col_size is None:
                raise ValueError("target_col_size should not be none when use_varchar=True")
            target_col_type = f"VARCHAR({target_col_size})"
        else:
            target_col_type = "TEXT"

        # tidbit: the largest name's length is 2936 characters
        create_statement = dedent(
            f"""
        CREATE TABLE {table} (
            id           SERIAL,  /* automatically the primary key */
            prefix       VARCHAR(32) NOT NULL,
            identifier   VARCHAR(64) NOT NULL,
            {target_col} {target_col_type} NOT NULL
        ) WITH (
            autovacuum_enabled = false,
            toast.autovacuum_enabled = false
        );
        """
        ).rstrip()

        drop_summary_statement = f"DROP TABLE IF EXISTS {table}_summary CASCADE;"
        create_summary_statement = dedent(
            f"""
        CREATE TABLE {table}_summary AS
          SELECT prefix, COUNT(identifier) as identifier_count
          FROM {table}
          GROUP BY prefix;

        CREATE UNIQUE INDEX {table}_summary_prefix
            ON {table}_summary (prefix);
        """  # noqa:S608
        ).rstrip()

        copy_statement = dedent(
            f"""
        COPY {table} (prefix, identifier, {target_col})
        FROM STDIN
        WITH CSV HEADER DELIMITER E'\\t' QUOTE E'\\b';
        """
        ).rstrip()

        cleanup_statement = dedent(
            f"""
        ALTER TABLE {table} SET (
            autovacuum_enabled = true,
            toast.autovacuum_enabled = true
        );
        """
        ).rstrip()

        index_curie_statement = f"CREATE INDEX ON {table} (prefix, identifier);"

        unique_curie_stmt = dedent(
            f"""
        ALTER TABLE {table}
            ADD CONSTRAINT {table}_prefix_identifier_unique UNIQUE (prefix, identifier);
        """
        ).rstrip()

        with closing(self.engine.raw_connection()) as connection:
            with closing(connection.cursor()) as cursor:
                echo("Preparing blank slate")
                echo(drop_statement, fg="yellow")
                cursor.execute(drop_statement)
                echo(drop_summary_statement, fg="yellow")
                cursor.execute(drop_summary_statement)

                echo("Creating table")
                echo(create_statement, fg="yellow")
                cursor.execute(create_statement)

                echo("Start COPY")
                echo(copy_statement, fg="yellow")
                try:
                    with gzip.open(path, "rt") as file:
                        if test:
                            echo(f"Loading testing data (rows={TEST_N}) from {path}")
                            sio = io.StringIO(
                                "".join(line for line, _ in zip(file, range(TEST_N + 1)))
                            )
                            sio.seek(0)
                            cursor.copy_expert(copy_statement, sio)
                        else:
                            echo(f"Loading data from {path}")
                            cursor.copy_expert(copy_statement, file)
                except Exception:
                    echo("Copy failed")
                    raise
                else:
                    echo("Copy ended")

                try:
                    connection.commit()
                except Exception:
                    echo("Commit failed")
                    raise
                else:
                    echo("Commit ended")

                echo("Start re-enable autovacuum")
                echo(cleanup_statement, fg="yellow")
                cursor.execute(cleanup_statement)
                echo("End re-enable autovacuum")

                echo("Start index on prefix/identifier")
                echo(index_curie_statement, fg="yellow")
                cursor.execute(index_curie_statement)
                echo("End indexing")

                if add_unique_constraints:
                    echo("Start unique on prefix/identifier")
                    echo(unique_curie_stmt, fg="yellow")
                    cursor.execute(unique_curie_stmt)
                    echo("End unique")

                if add_reverse_index:
                    index_reverse_statement = f"CREATE INDEX ON {table} (prefix, {target_col});"
                    echo("Start reverse indexing")
                    echo(index_reverse_statement, fg="yellow")
                    cursor.execute(index_reverse_statement)
                    echo("End reverse indexing")

        with closing(self.engine.raw_connection()) as connection:
            connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with connection.cursor() as cursor:
                echo("Creating summary table")
                echo(create_summary_statement, fg="yellow")
                cursor.execute(create_summary_statement)
                echo("Done creating summary table")

        with closing(self.engine.raw_connection()) as connection:
            connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with connection.cursor() as cursor:
                for x in (table, f"{table}_summary"):
                    sql = f"VACUUM ANALYSE {x};"
                    echo(sql, fg="yellow")
                    cursor.execute(sql)

        with self.engine.connect() as connection:
            select_statement = f"SELECT * FROM {table} LIMIT 10;"  # noqa:S608
            click.secho("Example query:", fg="green", bold=True)
            click.secho(select_statement, fg="green")
            result = connection.execute(select_statement)
            headers = ["id", "prefix", "identifier", target_col]
            click.echo(tabulate(map(tuple, result), headers=headers))

            # Summary table
            select_statement = f"SELECT * FROM {table}_summary ORDER BY identifier_count DESC LIMIT 10 ;"  # noqa:S608
            click.secho("Top entries in summary view:", fg="green", bold=True)
            click.secho(select_statement, fg="green")
            result = connection.execute(select_statement)
            click.echo(tabulate(map(tuple, result), headers=["prefix", "count"]))

    def derive_table(self):
        """Collapse the name, definition, and species tables."""
        drop_derived = f"DROP TABLE IF EXISTS {self.derived_table} CASCADE;"
        create_derived = dedent(
            f"""\
            CREATE TABLE {self.derived_table} AS (
            SELECT r.prefix, r.identifier, r.name, d.definition, s.species
            FROM {self.refs_table} r
            LEFT JOIN {self.defs_table} d on r.prefix = d.prefix
                and r.identifier = d.identifier
            LEFT JOIN {self.species_table} s on r.prefix = s.prefix
                and r.identifier = s.identifier
            )
        """  # noqa:S608
        ).rstrip()
        pkey_statement = dedent(
            f"""
            ALTER TABLE {self.derived_table}
                ADD CONSTRAINT pk_{self.derived_table} PRIMARY KEY (prefix, identifier);
        """
        ).rstrip()

        with closing(self.engine.raw_connection()) as connection:
            with closing(connection.cursor()) as cursor:
                echo("Cleanup table")
                echo(drop_derived, fg="yellow")
                cursor.execute(drop_derived)

                echo("Creating derived table")
                echo(create_derived, fg="yellow")
                cursor.execute(create_derived)
                echo("Done creating derived table")

                echo("Indexing PKEY on derived table")
                echo(pkey_statement, fg="yellow")
                cursor.execute(pkey_statement)
                echo("Done indexing PKEY on derived table")

                echo("Dropping unused tables")
                drop_refs = f"DROP TABLE {self.refs_table} CASCADE;"
                echo(drop_refs)
                cursor.execute(drop_refs)
                drop_defs = f"DROP TABLE {self.defs_table} CASCADE;"
                echo(drop_defs)
                cursor.execute(drop_defs)
                drop_species = f"DROP TABLE {self.species_table} CASCADE;"
                echo(drop_species)
                cursor.execute(drop_species)


def load(
    *,
    refs_path: Union[None, str, Path] = None,
    alts_path: Union[None, str, Path] = None,
    defs_path: Union[None, str, Path] = None,
    species_path: Union[None, str, Path] = None,
    synonyms_path: Union[None, str, Path] = None,
    refs_table: Optional[str] = None,
    alts_table: Optional[str] = None,
    defs_table: Optional[str] = None,
    species_table: Optional[str] = None,
    derived_table: Optional[str] = None,
    synonyms_table: Optional[str] = None,
    test: bool = False,
    uri: Optional[str] = None,
) -> None:
    """Load the database.

    :param refs_table: Name of the references table
    :param refs_path: Path to the references table data
    :param alts_table: Name of the alts table
    :param alts_path: Path to the alts table data
    :param defs_table: Name of the definitions table
    :param defs_path: Path to the definitions table data
    :param species_table: Name of the species table
    :param species_path: Path to the species table data
    :param derived_table: Name of the prefix-id-... derived table.
    :param synonyms_table: Name of the synonyms table
    :param synonyms_path: Path to the syononyms table data
    :param test: Should only a test set of rows be uploaded? Defaults to false.
    :param uri: The URI of the database to connect to.
    """
    loader = Loader(
        refs_table=refs_table,
        alts_table=alts_table,
        defs_table=defs_table,
        species_table=species_table,
        derived_table=derived_table,
        synonyms_table=synonyms_table,
        uri=uri,
    )
    loader.load_synonyms(path=synonyms_path, test=test)
    loader.load_alts(path=alts_path, test=test)
    loader.load_definition(path=defs_path, test=test)
    loader.load_name(path=refs_path, test=test)
    loader.load_species(path=species_path, test=test)
    loader.derive_table()
    echo("Done")


def _ensure_engine(engine: Union[None, str, Engine] = None) -> Engine:
    if engine is None:
        engine = get_sqlalchemy_uri()
    if isinstance(engine, str):
        logger.debug("connecting to database %s", engine)
        engine = create_engine(engine)
    return engine
