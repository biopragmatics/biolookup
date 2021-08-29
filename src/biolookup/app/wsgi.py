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
from humanize import intcomma

from .backends import get_backend
from .blueprints import biolookup_blueprint
from .proxies import backend

logger = logging.getLogger(__name__)

ui = Blueprint("ui", __name__)


@ui.route("/")
def home():
    """Serve the home page."""
    return render_template(
        "home.html",
        name_count=intcomma(backend.count_names()),
        alts_count=intcomma(backend.count_alts()),
        prefix_count=intcomma(backend.count_prefixes()),
        definition_count=intcomma(backend.count_definitions()),
    )


@ui.route("/summary")
def summary():
    """Serve the summary page."""
    return render_template(
        "summary.html",
        summary_df=backend.summary_df(),
    )


def get_app(
    name_data: Union[None, str, pd.DataFrame] = None,
    alts_data: Union[None, str, pd.DataFrame] = None,
    defs_data: Union[None, str, pd.DataFrame] = None,
    lazy: bool = False,
    sql: bool = False,
    uri: Optional[str] = None,
    refs_table: Optional[str] = None,
    alts_table: Optional[str] = None,
    defs_table: Optional[str] = None,
) -> Flask:
    """Build a flask app.

    :param name_data: If none, uses the internal PyOBO loader. If a string, assumes is a gzip and reads a
         dataframe from there. If a dataframe, uses it directly. Assumes data frame has 3 columns - prefix,
         identifier, and name and is a TSV.
    :param alts_data: If none, uses the internal PyOBO loader. If a string, assumes is a gzip and reads a
         dataframe from there. If a dataframe, uses it directly. Assumes data frame has 3 columns - prefix,
         alt identifier, and identifier and is a TSV.
    :param defs_data: If none, uses the internal PyOBO loader. If a string, assumes is a gzip and reads a
         dataframe from there. If a dataframe, uses it directly. Assumes data frame has 3 columns - prefix,
         identifier identifier, and definition and is a TSV.
    :param lazy: don't load the full cache into memory to run
    :param sql: use a remote SQL database
    :param uri: If using a remote SQL database, specify a non-default connection string
    :param refs_table: Name of the reference table in the SQL database
    :param alts_table: Name of the alternative identifiers table in the SQL database
    :param defs_table: Name of the definitions table in the SQL database
    :return: A pre-built flask app.
    """
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

    app.config["resolver_backend"] = get_backend(
        name_data=name_data,
        alts_data=alts_data,
        defs_data=defs_data,
        lazy=lazy,
        sql=sql,
        uri=uri,
        refs_table=refs_table,
        alts_table=alts_table,
        defs_table=defs_table,
    )
    app.register_blueprint(ui)
    app.register_blueprint(biolookup_blueprint)

    @app.before_first_request
    def _before_first_request():
        logger.info("before_first_request")
        backend.count_all()

    return app
