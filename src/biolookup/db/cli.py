# -*- coding: utf-8 -*-

"""CLI for the Biolookup Service database loader.

Run with ``biolookup load``.
"""

from typing import Optional

import click
from more_click import verbose_option
from pyobo.resource_utils import (
    ensure_alts,
    ensure_definitions,
    ensure_inspector_javert,
    ensure_ooh_na_na,
)

from ..constants import (
    ALTS_TABLE_NAME,
    DEFS_TABLE_NAME,
    REFS_TABLE_NAME,
    XREFS_NAME,
    get_sqlalchemy_uri,
)

__all__ = [
    "load",
]

uri_option = click.option(
    "--uri", default=get_sqlalchemy_uri, help="The database URL.", show_default=True
)
refs_table_option = click.option("--refs-table", default=REFS_TABLE_NAME, show_default=True)
refs_path_option = click.option(
    "--refs-path", default=ensure_ooh_na_na, show_default=True, help="By default, load from Zenodo"
)
alts_table_option = click.option("--alts-table", default=ALTS_TABLE_NAME, show_default=True)
alts_path_option = click.option(
    "--alts-path", default=ensure_alts, show_default=True, help="By default, load from Zenodo"
)
defs_table_option = click.option("--defs-table", default=DEFS_TABLE_NAME, show_default=True)
defs_path_option = click.option(
    "--defs-path",
    default=ensure_definitions,
    show_default=True,
    help="By default, load from Zenodo",
)
xrefs_table_option = click.option("--xrefs-table", default=XREFS_NAME, show_default=True)
xrefs_path_option = click.option(
    "--xrefs-path",
    default=ensure_inspector_javert,
    show_default=True,
    help="By default, load from Zenodo",
)
test_option = click.option("--test", is_flag=True, help="Test run with a small test subset")


@click.command()
@uri_option
@refs_table_option
@refs_path_option
@alts_table_option
@alts_path_option
@defs_table_option
@defs_path_option
@xrefs_table_option
@xrefs_path_option
@test_option
@verbose_option
@click.option("--date", required=True)
def load(
    uri: str,
    refs_table: str,
    refs_path: str,
    alts_table: str,
    alts_path: str,
    defs_table: str,
    defs_path: str,
    xrefs_table: str,
    xrefs_path: str,
    test: bool,
    date: Optional[str],
):
    """Load the SQL database."""
    from .loader import load as _load
    from .loader import load_date

    if date:
        load_date(
            uri=uri,
            date=date,
            test=test,
        )
    else:
        _load(
            uri=uri,
            refs_table=refs_table,
            refs_path=refs_path,
            alts_table=alts_table,
            alts_path=alts_path,
            defs_table=defs_table,
            defs_path=defs_path,
            xrefs_table=xrefs_table,
            xrefs_path=xrefs_path,
            test=test,
        )


if __name__ == "__main__":
    load()
