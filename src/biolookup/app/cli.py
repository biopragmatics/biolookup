"""Biolookup Service CLI.

Run with ``biolookup web``.
"""

import logging
import sys

import click
from more_click import (
    host_option,
    port_option,
    verbose_option,
)

from ..constants import MODULE

__all__ = [
    "web",
]

LOG_PATH = MODULE.join(name="log.txt")


@click.command()
@port_option  # type:ignore[misc]
@host_option  # type:ignore[misc]
@click.option("--name-data", help="local 3-column gzipped TSV as database")
@click.option("--alts-data", help="local 3-column gzipped TSV as database")
@click.option("--defs-data", help="local 3-column gzipped TSV as database")
@click.option(
    "--sql",
    is_flag=True,
    help="Use a sql database backend. If not used, defaults to in memory dictionaries",
)
@click.option("--uri", help="SQL URI string when using --sql")
@click.option("--sql-refs-table", help="use preloaded SQL database as backend")
@click.option("--sql-alts-table", help="use preloaded SQL database as backend")
@click.option("--sql-defs-table", help="use preloaded SQL database as backend")
@click.option("--lazy", is_flag=True, help="do no load full cache into memory automatically")
@click.option("--test", is_flag=True, help="run in test mode with only a few datasets")
@click.option("--workers", type=int, help="number of workers to use in --gunicorn mode")
@verbose_option  # type:ignore[misc]
def web(
    port: str,
    host: str,
    sql: bool,
    uri: str | None,
    sql_refs_table: str | None,
    sql_alts_table: str | None,
    sql_defs_table: str | None,
    name_data: str | None,
    alts_data: str | None,
    defs_data: str | None,
    test: bool,
    lazy: bool,
    workers: int,
) -> None:
    """Run the Biolookup Service."""
    import uvicorn

    if test:
        if lazy:
            click.secho("Can not run in --test and --lazy mode at the same time", fg="red")
            sys.exit(0)
        if sql:
            click.secho("Can not run in --test and --sql mode at the same time", fg="red")
            sys.exit(0)

    from .wsgi import get_app

    if test:
        import pandas as pd
        from pyobo import get_alts_to_id, get_id_definition_mapping, get_id_name_mapping

        prefixes = ["hgnc", "chebi", "doid", "go", "uniprot"]
        name_data = pd.DataFrame(
            [
                (prefix, identifier, name)
                for prefix in prefixes
                for identifier, name in get_id_name_mapping(prefix).items()
            ],
            columns=["prefix", "identifier", "name"],
        )
        click.echo(f"prepared {len(name_data.index):,} test names from {prefixes}")  # type:ignore
        alts_data = pd.DataFrame(
            [
                (prefix, alt, identifier)
                for prefix in prefixes
                for alt, identifier in get_alts_to_id(prefix).items()
            ],
            columns=["prefix", "alt", "identifier"],
        )
        click.echo(f"prepared {len(alts_data.index):,} test alts from {prefixes}")  # type:ignore
        defs_data = pd.DataFrame(
            [
                (prefix, identifier, definition)
                for prefix in prefixes
                for identifier, definition in get_id_definition_mapping(prefix).items()
            ],
            columns=["prefix", "identifier", "definition"],
        )
        click.echo(f"prepared {len(defs_data.index):,} test defs from {prefixes}")  # type:ignore

    app = get_app(
        name_data=name_data,
        alts_data=alts_data,
        defs_data=defs_data,
        lazy=lazy,
        sql=sql,
        uri=uri,
        refs_table=sql_refs_table,
        alts_table=sql_alts_table,
        defs_table=sql_defs_table,
    )

    # see logging cookbook https://docs.python.org/3/howto/logging-cookbook.html
    fh = logging.FileHandler(LOG_PATH)
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    fh.setFormatter(formatter)

    from . import wsgi
    from ..backends import sql_backend

    logging.getLogger("werkzeug").addHandler(fh)
    sql_backend.logger.setLevel(logging.DEBUG)
    sql_backend.logger.addHandler(fh)
    wsgi.logger.setLevel(logging.DEBUG)
    wsgi.logger.addHandler(fh)

    uvicorn.run(app, host=host, port=int(port), workers=workers)


if __name__ == "__main__":
    web()
