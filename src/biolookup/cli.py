"""Command line interface for :mod:`biolookup`."""

import json
import logging

import click

from . import api
from .app.cli import web
from .db.cli import load

__all__ = ["main"]

logger = logging.getLogger(__name__)


@click.group()
@click.version_option()
def main():
    """CLI for the Biolookup Service."""


@main.command(name="lookup")
@click.argument("curie")
def lookup(curie: str):
    """Look up a CURIE."""
    click.echo(json.dumps(api.lookup(curie), indent=2, sort_keys=True))


main.add_command(web)
main.add_command(load)

if __name__ == "__main__":
    main()
