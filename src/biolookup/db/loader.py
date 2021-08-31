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
from pyobo.resource_utils import ensure_alts, ensure_definitions, ensure_ooh_na_na, ensure_species
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from tabulate import tabulate

from ..constants import (
    ALTS_TABLE_NAME,
    DEFS_TABLE_NAME,
    DERIVED_NAME,
    REFS_TABLE_NAME,
    SPECIES_TABLE_NAME,
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


def load(
    *,
    refs_path: Union[None, str, Path] = None,
    alts_path: Union[None, str, Path] = None,
    defs_path: Union[None, str, Path] = None,
    species_path: Union[None, str, Path] = None,
    refs_table: Optional[str] = None,
    alts_table: Optional[str] = None,
    defs_table: Optional[str] = None,
    species_table: Optional[str] = None,
    derived_table: Optional[str] = None,
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
    :param test: Should only a test set of rows be uploaded? Defaults to false.
    :param uri: The URI of the database to connect to.
    """
    engine = _ensure_engine(uri)
    if alts_table is None:
        alts_table = ALTS_TABLE_NAME
    if refs_table is None:
        refs_table = REFS_TABLE_NAME
    if defs_table is None:
        defs_table = DEFS_TABLE_NAME
    if species_table is None:
        species_table = SPECIES_TABLE_NAME
    if derived_table is None:
        derived_table = DERIVED_NAME
    _load_alts(engine=engine, table=alts_table, path=alts_path, test=test)
    _load_definition(engine=engine, table=defs_table, path=defs_path, test=test)
    _load_name(engine=engine, table=refs_table, path=refs_path, test=test)
    _load_species(engine=engine, table=species_table, path=species_path, test=test)

    # Use
    drop_derived = f"DROP TABLE IF EXISTS {derived_table} CASCADE;"
    create_derived = dedent(
        f"""\
        CREATE TABLE {derived_table} AS (
        SELECT r.prefix, r.identifier, r.name, d.definition, s.species
        FROM {refs_table} r
        LEFT JOIN {defs_table} d on r.prefix = d.prefix
            and r.identifier = d.identifier
        LEFT JOIN {species_table} s on r.prefix = s.prefix
            and r.identifier = s.identifier
        )
    """  # noqa:S608
    ).rstrip()
    pkey_statement = dedent(
        f"""
        ALTER TABLE {derived_table}
            ADD CONSTRAINT pk_{derived_table} PRIMARY KEY (prefix, identifier);
    """
    ).rstrip()

    with closing(engine.raw_connection()) as connection:
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
            drop_refs = f"DROP TABLE {refs_table} CASCADE;"
            echo(drop_refs)
            cursor.execute(drop_refs)
            drop_defs = f"DROP TABLE {defs_table} CASCADE;"
            echo(drop_defs)
            cursor.execute(drop_defs)
            drop_species = f"DROP TABLE {species_table} CASCADE;"
            echo(drop_species)
            cursor.execute(drop_species)

    echo("Done")


def _load_alts(
    *,
    engine: Union[None, str, Engine] = None,
    table: Optional[str] = None,
    path: Union[None, str, Path] = None,
    test: bool = False,
):
    engine = _ensure_engine(engine)
    _load_table(
        engine=engine,
        table=table or ALTS_TABLE_NAME,
        path=path if path is not None else ensure_alts(),
        test=test,
        target_col="alt",
        target_col_size=64,
        add_unique_constraints=False,
        add_reverse_index=True,
    )


def _load_definition(
    *,
    engine: Union[None, str, Engine] = None,
    table: Optional[str] = None,
    path: Union[None, str, Path] = None,
    test: bool = False,
):
    engine = _ensure_engine(engine)
    _load_table(
        engine=engine,
        table=table or DEFS_TABLE_NAME,
        path=path if path else ensure_definitions(),
        test=test,
        target_col="definition",
        use_varchar=False,
    )


def _load_species(
    *,
    engine: Union[None, str, Engine] = None,
    table: Optional[str] = None,
    path: Union[None, str, Path] = None,
    test: bool = False,
):
    engine = _ensure_engine(engine)
    _load_table(
        engine=engine,
        table=table or SPECIES_TABLE_NAME,
        path=path if path else ensure_species(),
        test=test,
        target_col="species",
        target_col_size=16,
    )


def _load_name(
    *,
    engine: Union[None, str, Engine] = None,
    table: Optional[str] = None,
    path: Union[None, str, Path] = None,
    test: bool = False,
):
    engine = _ensure_engine(engine)
    _load_table(
        engine=engine,
        table=table or REFS_TABLE_NAME,
        path=path if path else ensure_ooh_na_na(),
        test=test,
        target_col="name",
        target_col_size=4096,
    )


def _ensure_engine(engine: Union[None, str, Engine] = None) -> Engine:
    if engine is None:
        engine = get_sqlalchemy_uri()
    if isinstance(engine, str):
        logger.debug("connecting to database %s", engine)
        engine = create_engine(engine)
    return engine


def _load_table(
    table: str,
    path: Union[str, Path],
    target_col: str,
    *,
    test: bool = False,
    target_col_size: Optional[int] = None,
    engine: Union[None, str, Engine] = None,
    add_unique_constraints: bool = True,
    add_reverse_index: bool = False,
    use_varchar: bool = True,
) -> None:
    engine = _ensure_engine(engine)

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

    with closing(engine.raw_connection()) as connection:
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
                        sio = io.StringIO("".join(line for line, _ in zip(file, range(TEST_N + 1))))
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

    with closing(engine.raw_connection()) as connection:
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        with connection.cursor() as cursor:
            echo("Creating summary table")
            echo(create_summary_statement, fg="yellow")
            cursor.execute(create_summary_statement)
            echo("Done creating summary table")

    with closing(engine.raw_connection()) as connection:
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        with connection.cursor() as cursor:
            for x in (table, f"{table}_summary"):
                sql = f"VACUUM ANALYSE {x};"
                echo(sql, fg="yellow")
                cursor.execute(sql)

    with engine.connect() as connection:
        select_statement = f"SELECT * FROM {table} LIMIT 10;"  # noqa:S608
        click.secho("Example query:", fg="green", bold=True)
        click.secho(select_statement, fg="green")
        result = connection.execute(select_statement)
        headers = ["id", "prefix", "identifier", target_col]
        click.echo(tabulate(map(tuple, result), headers=headers))

        # Summary table
        select_statement = (
            f"SELECT * FROM {table}_summary ORDER BY identifier_count DESC LIMIT 10 ;"  # noqa:S608
        )
        click.secho("Top entries in summary view:", fg="green", bold=True)
        click.secho(select_statement, fg="green")
        result = connection.execute(select_statement)
        click.echo(tabulate(map(tuple, result), headers=["prefix", "count"]))
