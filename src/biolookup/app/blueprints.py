# -*- coding: utf-8 -*-

"""A reusable blueprint for the Biolookup Service."""

import logging
import os
import time

from flask import Blueprint, jsonify
from humanize.filesize import naturalsize

from .proxies import backend

__all__ = [
    "biolookup_blueprint",
]

logger = logging.getLogger(__name__)
biolookup_blueprint = Blueprint("biolookup", __name__)


@biolookup_blueprint.route("/lookup/<curie>")
def resolve(curie: str):
    """Resolve a CURIE.

    The goal of this endpoint is to resolve a CURIE.

    - ``doid:14330``, an exact match to the CURIE for Parkinson's disease in the Disease Ontology
    - ``DOID:14330``, a close match to the CURIE for Parkinson's disease in the Disease Ontology, only differing
      by capitalization
    - ``do:14330``, a match to doid via synonyms in the metaregistry. Still resolves to Parkinson's disease
      in the Disease Ontology.
    ---
    parameters:
      - name: curie
        in: path
        description: compact uniform resource identifier (CURIE) of the entity
        required: true
        type: string
        example: doid:14330
    """  # noqa:DAR101,DAR201
    logger.debug("resolving %s", curie)
    start = time.time()
    rv = backend.resolve(curie)
    logger.debug("resolved %s in %.2f seconds", curie, time.time() - start)
    return jsonify(rv)


@biolookup_blueprint.route("/summary.json")
def summary_json():
    """Summary of the content in the service."""
    return jsonify(backend.summarize_names())


@biolookup_blueprint.route("/size")
def size():
    """Return how much memory we're taking.

    Doesn't work if you're running with Gunicorn because it makes child processes.
    """  # noqa:DAR201
    try:
        import psutil
    except ImportError:
        return jsonify({})
    process = psutil.Process(os.getpid())
    n_bytes = process.memory_info().rss  # in bytes
    return jsonify(
        n_bytes=n_bytes,
        n_bytes_human=naturalsize(n_bytes),
    )
