# -*- coding: utf-8 -*-

"""Biolookup Service.

Run with ``python -m biolookup.app``
"""

import logging
from typing import Optional, Union

import pandas as pd
from flasgger import Swagger
from flask import Blueprint, Flask, render_template
from flask_bootstrap import Bootstrap

from .blueprints import biolookup_blueprint
from .proxies import backend
from ..backends import Backend, get_backend

logger = logging.getLogger(__name__)

ui = Blueprint("ui", __name__)


def _figure_number(n):
    if n > 1_000_000:
        lead = n / 1_000_000
        if lead < 10:
            return round(lead, 1), "M"
        else:
            return round(lead), "M"
    if n > 1_000:
        lead = n / 1_000
        if lead < 10:
            return round(lead, 1), "K"
        else:
            return round(lead), "K"


@ui.route("/")
def home():
    """Serve the home page."""
    name_count, name_suffix = _figure_number(backend.count_names())
    alts_count, alts_suffix = _figure_number(backend.count_alts())
    defs_count, defs_suffix = _figure_number(backend.count_definitions())
    species_count, species_suffix = _figure_number(backend.count_species())
    return render_template(
        "home.html",
        name_count=name_count,
        name_suffix=name_suffix,
        alts_count=alts_count,
        alts_suffix=alts_suffix,
        prefix_count=backend.count_prefixes(),
        definition_count=defs_count,
        definition_suffix=defs_suffix,
        species_count=species_count,
        species_suffix=species_suffix,
    )


@ui.route("/statistics")
def summary():
    """Serve the summary page."""
    return render_template(
        "statistics.html",
        summary_df=backend.summary_df(),
    )


@ui.route("/about")
def about():
    """Serve the about page."""
    return render_template("meta/about.html")


@ui.route("/downloads")
def downloads():
    """Serve the downloads page."""
    return render_template("meta/download.html")


@ui.route("/usage")
def usage():
    """Serve the usage page."""
    return render_template("meta/access.html")


@ui.route("/entity/<curie>")
def entity(curie: str):
    """Serve an entity page."""
    res = backend.lookup(curie)
    return render_template("entity.html", res=res)


def get_app(
    name_data: Union[None, str, pd.DataFrame] = None,
    alts_data: Union[None, str, pd.DataFrame] = None,
    defs_data: Union[None, str, pd.DataFrame] = None,
    species_data: Union[None, str, pd.DataFrame] = None,
    lazy: bool = False,
    sql: bool = False,
    uri: Optional[str] = None,
    refs_table: Optional[str] = None,
    alts_table: Optional[str] = None,
    defs_table: Optional[str] = None,
    species_table: Optional[str] = None,
) -> Flask:
    """Build a flask app.

    :param name_data: If none, uses the internal PyOBO loader. If a string, assumes is a gzip and reads a
         dataframe from there. If a dataframe, uses it directly. Assumes data frame has 3 columns - prefix,
         identifier, and name and is a TSV.
    :param alts_data: If none, uses the internal PyOBO loader. If a string, assumes is a gzip and reads a
         dataframe from there. If a dataframe, uses it directly. Assumes data frame has 3 columns - prefix,
         identifier, and alt identifier and is a TSV.
    :param defs_data: If none, uses the internal PyOBO loader. If a string, assumes is a gzip and reads a
         dataframe from there. If a dataframe, uses it directly. Assumes data frame has 3 columns - prefix,
         identifier, and definition and is a TSV.
    :param species_data: If none, uses the internal PyOBO loader. If a string, assumes is a gzip and reads a
         dataframe from there. If a dataframe, uses it directly. Assumes data frame has 3 columns - prefix,
         identifier, and species and is a TSV.
    :param lazy: don't load the full cache into memory to run
    :param sql: use a remote SQL database
    :param uri: If using a remote SQL database, specify a non-default connection string
    :param refs_table: Name of the reference table in the SQL database
    :param alts_table: Name of the alternative identifiers table in the SQL database
    :param defs_table: Name of the definitions table in the SQL database
    :param species_table: Name of the species table in the SQL database
    :return: A pre-built flask app.
    """
    backend = get_backend(
        name_data=name_data,
        alts_data=alts_data,
        defs_data=defs_data,
        species_data=species_data,
        lazy=lazy,
        sql=sql,
        uri=uri,
        refs_table=refs_table,
        alts_table=alts_table,
        defs_table=defs_table,
        species_table=species_table,
    )
    return get_app_from_backend(backend)


def get_app_from_backend(backend: Backend) -> Flask:
    """Build a flask app."""
    app = Flask(__name__)
    Swagger(
        app,
        merge=True,
        config={
            "title": "Biolookup API",
            "description": "Resolves CURIEs to their names, definitions, and other attributes.",
            "contact": {
                "responsibleDeveloper": "Charles Tapley Hoyt",
                "email": "cthoyt@gmail.com",
            },
        },
    )
    Bootstrap(app)

    app.config["resolver_backend"] = backend
    app.register_blueprint(ui)
    app.register_blueprint(biolookup_blueprint)

    @app.before_first_request
    def _before_first_request():
        logger.info("before_first_request")
        backend.count_all()

    return app
