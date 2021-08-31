# -*- coding: utf-8 -*-

"""Command line interface for :mod:`biolookup`.

Why does this file exist, and why not put this in ``__main__``? You might be tempted to import things from ``__main__``
later, but that will cause problems--the code will get executed twice:

- When you run ``python3 -m biolookup`` python will execute``__main__.py`` as a script.
  That means there won't be any ``biolookup.__main__`` in ``sys.modules``.
- When you import __main__ it will get executed again (as a module) because
  there's no ``biolookup.__main__`` in ``sys.modules``.

.. seealso:: https://click.palletsprojects.com/en/7.x/setuptools/#setuptools-integration
"""

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
    print(json.dumps(api.lookup(curie), indent=2, sort_keys=True))


main.add_command(web)
main.add_command(load)

if __name__ == "__main__":
    main()
