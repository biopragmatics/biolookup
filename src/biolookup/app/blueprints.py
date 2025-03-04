"""A reusable blueprint for the Biolookup Service."""

import logging
import os
import time

from fastapi import APIRouter, Path, Request

from ..backends.backend import LookupResult
from ..constants import DEFAULT_URL

__all__ = [
    "biolookup_blueprint",
]

logger = logging.getLogger(__name__)
biolookup_blueprint = APIRouter(prefix="/api")


@biolookup_blueprint.get("/lookup/{curie}", response_model=LookupResult)
def lookup(
    request: Request,
    curie: str = Path(
        ...,
        description="A compact uniform resource identifier (CURIE) of an entity",
        examples=["doid:14330"],
    ),
) -> LookupResult:
    """Lookup metadata and ontological information for a biomedical entity.

    This endpoint uses the Bioregistry to recognize and standardize
    compact uniform resource identifiers (CURIEs) for biomedical entities.
    For example:

    - ``doid:14330``, an exact match to the CURIE for Parkinson's disease in the Disease Ontology
    - ``DOID:14330``, a close match to the CURIE for Parkinson's disease in the Disease Ontology,
      only differing by capitalization
    - ``do:14330``, a match to doid via synonyms in the Bioregistry. Still resolves to Parkinson's
      disease in the Disease Ontology.
    """
    backend = request.app.state.backend
    logger.debug("querying %s", curie)
    start = time.time()
    rv = backend.lookup(curie)
    logger.debug("queried %s in %.2f seconds", curie, time.time() - start)
    if rv.providers is None:
        rv.providers = {"biolookup": f"{DEFAULT_URL}/{curie}"}
    else:
        rv.providers["biolookup"] = f"{DEFAULT_URL}/{curie}"
    return rv


@biolookup_blueprint.route("/summary.json")
def summary_json(request: Request):
    """Summary of the content in the service."""
    backend = request.app.state.backend
    return backend.summarize_names()


@biolookup_blueprint.route("/size")
def size():
    """Return how much memory we're taking.

    Doesn't work if you're running with Gunicorn because it makes child processes.
    """
    try:
        import psutil
    except ImportError:
        return {}
    from humanize.filesize import naturalsize

    process = psutil.Process(os.getpid())
    n_bytes = process.memory_info().rss  # in bytes
    return {
        "n_bytes": n_bytes,
        "n_bytes_human": naturalsize(n_bytes),
    }
