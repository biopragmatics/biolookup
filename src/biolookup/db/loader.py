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
from typing import Iterable, Optional, Union

import click
import pystow
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from pyobo.resource_utils import (
    ensure_alts,
    ensure_definitions,
    ensure_inspector_javert,
    ensure_ooh_na_na,
    ensure_relations,
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
    RELS_NAME,
    SPECIES_TABLE_NAME,
    SYNONYMS_NAME,
    XREFS_NAME,
    get_sqlalchemy_uri,
)

__all__ = [
    "load",
    "load_date",
]

logger = logging.getLogger(__name__)


def echo(s, **kwargs) -> None:
    """Wrap echo with time logging."""
    click.echo(f'[{time.strftime("%Y-%m-%d %H:%M:%S")}] ', nl="")
    click.secho(s, **kwargs)


#: Number of test rows if --test is used
TEST_N = 100_000


def run(cursor, sql, desc):
    """Run a command with colorful logging."""
    echo(f"Start {desc}")
    echo(sql, fg="yellow")
    cursor.execute(sql)
    echo(f"End {desc}")


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
        xrefs_table: Optional[str] = None,
        rels_table: Optional[str] = None,
    ):
        """Load the database.

        :param uri: The URI of the database to connect to.
        :param refs_table: Name of the references table
        :param alts_table: Name of the alts table
        :param defs_table: Name of the definitions table
        :param species_table: Name of the species table
        :param derived_table: Name of the prefix-id-... derived table.
        :param synonyms_table: Name of the synonyms table
        :param xrefs_table: Name of the xrefs table
        :param rels_table: Name of the relations table
        """
        self.engine = _ensure_engine(uri)
        self.alts_table = alts_table or ALTS_TABLE_NAME
        self.refs_table = refs_table or REFS_TABLE_NAME
        self.defs_table = defs_table or DEFS_TABLE_NAME
        self.species_table = species_table or SPECIES_TABLE_NAME
        self.derived_table = derived_table or DERIVED_NAME
        self.synonyms_table = synonyms_table or SYNONYMS_NAME
        self.xrefs_table = xrefs_table or XREFS_NAME
        self.rels_table = rels_table or RELS_NAME

    def load_alts(
        self,
        *,
        path: Union[None, str, Path] = None,
        test: bool = False,
    ):
        """Load the alternative identifiers table."""
        self._load_three_col_table(
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
        self._load_three_col_table(
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
        self._load_three_col_table(
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
        self._load_three_col_table(
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
        self._load_three_col_table(
            table=self.synonyms_table,
            path=path if path else ensure_synonyms(),
            test=test,
            target_col="synonym",
            target_col_size=4096,
            add_unique_constraints=False,
        )

    def load_xrefs(
        self,
        *,
        path: Union[None, str, Path] = None,
        test: bool = False,
    ):
        """Load xrefs table."""
        self._load_table(
            table=self.xrefs_table,
            path=path if path else ensure_inspector_javert(),
            test=test,
            target_col=["xref_prefix", "xref_identifier", "provenance"],
            # TODO Change second column to 64 after fixing chebi's metacyc xrefs
            target_col_type=["VARCHAR(32)", "VARCHAR(1024)", "VARCHAR(512)"],
            add_unique_constraints=False,
        )

    def load_rels(
        self,
        *,
        path: Union[None, str, Path] = None,
        test: bool = False,
    ):
        """Load relations table."""
        self._load_table(
            table=self.rels_table,
            path=path if path else ensure_relations(),
            test=test,
            target_col=[
                "relation_prefix",
                "relation_identifier",
                "target_prefix",
                "target_identifier",
            ],
            target_col_type=["VARCHAR(32)", "VARCHAR(64)", "VARCHAR(32)", "VARCHAR(64)"],
            add_unique_constraints=False,
        )

    @staticmethod
    def _create_table_ddl(
        table: str,
        target_col: Union[str, Iterable[str]],
        target_col_type: Union[str, Iterable[str]],
    ) -> str:
        if isinstance(target_col, str) and isinstance(target_col_type, str):
            column_defs = f"{target_col} {target_col_type} NOT NULL"
        else:
            column_defs = ",\n".join(
                f"{x} {y} NOT NULL" for x, y in zip(target_col, target_col_type)
            )

        # tidbit: the largest name's length is 2936 characters
        return dedent(
            f"""
        CREATE TABLE {table} (
            id           SERIAL,  /* automatically the primary key */
            prefix       VARCHAR(32) NOT NULL,
            identifier   VARCHAR(64) NOT NULL,
            {column_defs}
        ) WITH (
            autovacuum_enabled = false,
            toast.autovacuum_enabled = false
        );
        """
        ).rstrip()

    @staticmethod
    def _create_summary_table_ddl(table: str) -> str:
        return dedent(
            f"""
        CREATE TABLE {table}_summary AS
          SELECT prefix, COUNT(identifier) as identifier_count
          FROM {table}
          GROUP BY prefix;

        CREATE UNIQUE INDEX {table}_summary_prefix
            ON {table}_summary (prefix);
        """  # noqa:S608
        ).rstrip()

    @staticmethod
    def _create_copy_ddl(table: str, target_col: Union[str, Iterable[str]]) -> str:
        if not isinstance(target_col, str):
            target_col = ", ".join(target_col)
        return dedent(
            f"""
        COPY {table} (prefix, identifier, {target_col})
        FROM STDIN
        WITH CSV HEADER DELIMITER E'\\t' QUOTE E'\\b';
        """
        ).rstrip()

    @staticmethod
    def _create_unique_ddl(table: str) -> str:
        return dedent(
            f"""
        ALTER TABLE {table}
            ADD CONSTRAINT {table}_prefix_identifier_unique UNIQUE (prefix, identifier);
        """
        ).rstrip()

    @staticmethod
    def _create_autovacuum_ddl(table: str) -> str:
        return dedent(
            f"""
        ALTER TABLE {table} SET (
            autovacuum_enabled = true,
            toast.autovacuum_enabled = true
        );
        """
        ).rstrip()

    @staticmethod
    def _copy(connection, cursor, sql, path, test):
        echo("Start COPY")
        echo(sql, fg="yellow")
        try:
            with gzip.open(path, "rt") as file:
                if test:
                    echo(f"Loading testing data (rows={TEST_N}) from {path}")
                    sio = io.StringIO("".join(line for line, _ in zip(file, range(TEST_N + 1))))
                    sio.seek(0)
                    cursor.copy_expert(sql, sio)
                else:
                    echo(f"Loading data from {path}")
                    cursor.copy_expert(sql, file)
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

    @staticmethod
    def _get_target_col_type(use_varchar: bool, target_col_size: Optional[int] = None) -> str:
        if use_varchar:
            if target_col_size is None:
                raise ValueError("target_col_size should not be none when use_varchar=True")
            return f"VARCHAR({target_col_size})"
        else:
            return "TEXT"

    def _load_three_col_table(
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
        target_col_type = self._get_target_col_type(
            use_varchar=use_varchar, target_col_size=target_col_size
        )
        self._load_table(
            table=table,
            path=path,
            target_col=target_col,
            target_col_type=target_col_type,
            test=test,
            add_unique_constraints=add_unique_constraints,
            add_reverse_index=add_reverse_index,
        )

    def _load_table(
        self,
        table: str,
        path: Union[str, Path],
        target_col: Union[str, Iterable[str]],
        target_col_type: Union[str, Iterable[str]],
        *,
        test: bool = False,
        add_unique_constraints: bool = True,
        add_reverse_index: bool = False,
    ) -> None:
        drop_statement = f"DROP TABLE IF EXISTS {table} CASCADE;"
        drop_summary_statement = f"DROP TABLE IF EXISTS {table}_summary CASCADE;"
        create_statement = self._create_table_ddl(
            table=table, target_col=target_col, target_col_type=target_col_type
        )
        create_summary_statement = self._create_summary_table_ddl(table=table)
        copy_statement = self._create_copy_ddl(table=table, target_col=target_col)
        cleanup_statement = self._create_autovacuum_ddl(table=table)
        index_curie_statement = f"CREATE INDEX ON {table} (prefix, identifier);"
        unique_curie_stmt = self._create_unique_ddl(table=table)
        index_reverse_statement = f"CREATE INDEX ON {table} (prefix, {target_col});"

        with closing(self.engine.raw_connection()) as connection:
            with closing(connection.cursor()) as cursor:
                run(cursor, drop_statement, "dropping table")
                run(cursor, drop_summary_statement, "dropping summary table")
                run(cursor, create_statement, "creating table")
                self._copy(
                    connection=connection, cursor=cursor, sql=copy_statement, path=path, test=test
                )
                run(cursor, cleanup_statement, "re-enable autovacuum")
                run(cursor, index_curie_statement, "index on prefix/identifier")
                if add_unique_constraints:
                    run(cursor, unique_curie_stmt, "constrain unique prefix/identifier")
                if add_reverse_index:
                    run(cursor, index_reverse_statement, "reversing index")

        with closing(self.engine.raw_connection()) as connection:
            connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with connection.cursor() as cursor:
                run(cursor, create_summary_statement, "creating summary table")

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
            if isinstance(target_col, str):
                headers = ["id", "prefix", "identifier", target_col]
            else:
                headers = ["id", "prefix", "identifier", *target_col]
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
                run(cursor, drop_derived, "cleanup derived table")
                run(cursor, create_derived, "create derived table")
                run(cursor, pkey_statement, "indexing derived table primary key")

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


def load_date(*, date: str, **kwargs):
    """Load the database from the given date."""
    directory = pystow.join("pyobo", "database", date)
    return load(
        refs_path=directory.joinpath("names.tsv.gz"),
        alts_path=directory.joinpath("alts.tsv.gz"),
        defs_path=directory.joinpath("definitions.tsv.gz"),
        species_path=directory.joinpath("species.tsv.gz"),
        synonyms_path=directory.joinpath("synonyms.tsv.gz"),
        xrefs_path=directory.joinpath("xrefs.tsv.gz"),
        rels_path=directory.joinpath("relations.tsv.gz"),
        **kwargs,
    )


def load(
    *,
    refs_path: Union[None, str, Path] = None,
    alts_path: Union[None, str, Path] = None,
    defs_path: Union[None, str, Path] = None,
    species_path: Union[None, str, Path] = None,
    synonyms_path: Union[None, str, Path] = None,
    xrefs_path: Union[None, str, Path] = None,
    rels_path: Union[None, str, Path] = None,
    refs_table: Optional[str] = None,
    alts_table: Optional[str] = None,
    defs_table: Optional[str] = None,
    species_table: Optional[str] = None,
    derived_table: Optional[str] = None,
    synonyms_table: Optional[str] = None,
    xrefs_table: Optional[str] = None,
    rels_table: Optional[str] = None,
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
    :param xrefs_table: Name of the xrefs table
    :param xrefs_path: Path to the xrefs table data
    :param rels_table: Name of the relations table
    :param rels_path: Path to the relations table data
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
        xrefs_table=xrefs_table,
        rels_table=rels_table,
        uri=uri,
    )
    loader.load_xrefs(path=xrefs_path, test=test)
    loader.load_rels(path=rels_path, test=test)
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
