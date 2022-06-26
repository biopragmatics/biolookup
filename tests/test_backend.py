# -*- coding: utf-8 -*-

"""Test for loading the database."""

import gzip
import tempfile
import unittest
from pathlib import Path
from typing import ClassVar, Type

import pyobo
import pystow
from flask import Flask

from biolookup.app.wsgi import get_app_from_backend
from biolookup.backends import Backend, MemoryBackend, RawSQLBackend, get_backend
from biolookup.constants import DEFAULT_ENDPOINT
from biolookup.db import loader

TEST_URI = pystow.get_config("biolookup", "test_uri")
REFS = [
    ("go", "0000073", "initial mitotic spindle pole body separation"),
    ("go", "0000075", "cell cycle checkpoint"),
    ("go", "0000076", "DNA replication checkpoint"),
    ("hgnc", "10020", "RIPK2"),
    ("hgnc", "10021", "RIPK3"),
    ("hgnc", "10023", "RIT1"),
]
ALTS = [
    ("go", "0000073", "0030475"),
]
DEF_1 = (
    "The release of duplicated mitotic spindle pole bodies (SPBs) that "
    "begins with the nucleation of microtubules from each SPB within the "
    "nucleus, leading to V-shaped spindle microtubules. Interpolar microtubules "
    "that elongate from each pole are interconnected, forming overlapping "
    "microtubules. Capturing and antiparallel sliding apart of microtubules "
    "promotes the initial separation of the SPB."
)
DEF_2 = "receptor interacting serine/threonine kinase 2"
DEFS = [
    ("go", "0000073", DEF_1),
    ("go", "0000075", "def_12"),
    ("go", "0000076", "def_13"),
    ("hgnc", "10020", DEF_2),
    ("hgnc", "10021", "def_22"),
    # ("hgnc", "id_23", "def_23"),
]
SPECIES = [
    ("hgnc", "10020", "9606"),
    ("hgnc", "10021", "9606"),
    ("hgnc", "10023", "9606"),
]
SYNONYMS = []
XREFS = []
RELS = []


def _write(path, data, *last):
    with gzip.open(path, "wt") as file:
        print("prefix", "identifier", *last, sep="\t", file=file)
        for line in data:
            print(*line, sep="\t", file=file)


class BackendTestCase(unittest.TestCase):
    """A mixin for checking the backend."""

    backend_cls: ClassVar[Type[Backend]]
    counts: ClassVar[bool]

    def help_check(self, backend: Backend, counts: bool = True):
        """Check backend."""
        self.assertTrue(issubclass(self.backend_cls, Backend))
        self.assertIsInstance(backend, self.backend_cls)

        if counts:
            self.assertEqual(6, backend.count_names())
            self.assertEqual(5, backend.count_definitions())
            self.assertEqual(1, backend.count_alts())
            self.assertEqual(3, backend.count_species(), msg="Got wrong number of species")

            self.assertEqual({"go": 3, "hgnc": 3}, dict(backend.summarize_names()))
            self.assertEqual({"go": 3, "hgnc": 2}, dict(backend.summarize_definitions()))
            self.assertEqual({"go": 1}, dict(backend.summarize_alts()))
            self.assertEqual(
                {"hgnc": 3}, dict(backend.summarize_species()), msg="Wrong species summary"
            )

        # Test name lookup
        self.assertEqual(
            "initial mitotic spindle pole body separation", backend.get_name("go", "0000073")
        )
        self.assertEqual("RIT1", backend.get_name("hgnc", "10023"))

        # Test resolution of definitions
        self.assertEqual(DEF_1, backend.get_definition("go", "0000073"))
        self.assertIsNone(backend.get_definition("hgnc", "1002310101010101"))

        # Test resolution of species
        self.assertEqual("9606", backend.get_species("hgnc", "10020"))
        self.assertIsNone(backend.get_species("go", "0030475"))

        # Test resolution of alt ids
        self.assertEqual("0000073", backend.get_primary_id("go", "0030475"))
        self.assertIsNone(backend.get_name("go", "0030475"))
        self.assertIsNone(backend.get_definition("go", "0030475"))

        r = backend.lookup("go:0000073")
        self.assert_go_example(r)

        r = backend.lookup("go:0030475")
        self.assertEqual("go", r["prefix"])
        self.assertEqual("0000073", r["identifier"])
        self.assertEqual("initial mitotic spindle pole body separation", r["name"])
        self.assertEqual(DEF_1, r["definition"])
        self.assertEqual("go:0030475", r["query"])
        self.assertNotIn("species", r)

        # Extra test to check species
        r = backend.lookup("hgnc:10020")
        self.assertEqual("hgnc", r["prefix"])
        self.assertEqual("10020", r["identifier"])
        self.assertEqual("RIPK2", r["name"])
        self.assertEqual(DEF_2, r["definition"])
        self.assertEqual("9606", r["species"])
        self.assertEqual("hgnc:10020", r["query"])

    def assert_go_example(self, r):
        """Run test of the canonical GO example."""
        self.assertIsNotNone(r)
        self.assertEqual("go", r["prefix"])
        self.assertEqual("0000073", r["identifier"])
        self.assertEqual("initial mitotic spindle pole body separation", r["name"])
        self.assertEqual(DEF_1, r["definition"])
        self.assertEqual("go:0000073", r["query"])
        self.assertNotIn("species", r)

    def assert_app_lookup(self, app: Flask):
        """Run the test on looking up the canonical GO example."""
        with app.test_client() as client:
            res = client.get(f"/{DEFAULT_ENDPOINT}/go:0000073")
            self.assertIsNotNone(res)
            self.assert_go_example(res.json)


@unittest.skipUnless(TEST_URI, reason="No biolookup/test_uri configuration found")
class TestRawSQLBackend(BackendTestCase):
    """Tests for the raw SQL backend."""

    backend_cls = RawSQLBackend
    counts = True

    def setUp(self) -> None:
        """Set up the test case."""
        self.refs_table = "refs"
        self.alts_table = "alts"
        self.defs_table = "defs"
        self.species_table = "species"
        self.synonyms_table = "synonyms"
        self.xrefs_table = "xrefs"
        self.rels_table = "rels"
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            refs_path = directory / "refs.tsv.gz"
            alts_path = directory / "alts.tsv.gz"
            defs_path = directory / "defs.tsv.gz"
            species_path = directory / "species.tsv.gz"
            synonyms_path = directory / "synonyms.tsv.gz"
            xrefs_path = directory / "xrefs.tsv.gz"
            rels_path = directory / "rels.tsv.gz"
            _write(refs_path, REFS, "name")
            _write(alts_path, ALTS, "alt")
            _write(defs_path, DEFS, "definition")
            _write(species_path, SPECIES, "species")
            _write(synonyms_path, SYNONYMS, "synonym")
            _write(xrefs_path, XREFS, "xref_prefix", "xref_identifier", "provenance")
            _write(
                rels_path,
                RELS,
                "relation_prefix",
                "relation_identifier",
                "object_prefix",
                "object_identifier",
            )
            loader.load(
                uri=TEST_URI,
                refs_path=refs_path,
                alts_path=alts_path,
                defs_path=defs_path,
                species_path=species_path,
                synonyms_path=synonyms_path,
                xrefs_path=xrefs_path,
                rels_path=rels_path,
                refs_table=self.refs_table,
                alts_table=self.alts_table,
                defs_table=self.defs_table,
                species_table=self.species_table,
                synonyms_table=self.synonyms_table,
                xrefs_table=self.xrefs_table,
                rels_table=self.rels_table,
            )
            self.backend = get_backend(
                sql=True,
                uri=TEST_URI,
                refs_table=self.refs_table,
                alts_table=self.alts_table,
                defs_table=self.defs_table,
                species_table=self.species_table,
                synonyms_table=self.synonyms_table,
                xrefs_table=self.xrefs_table,
                rels_table=self.rels_table,
            )

    def test_backend(self):
        """Test the raw SQL backend."""
        self.help_check(self.backend, counts=self.counts)

    def test_app(self):
        """Test the app works properly."""
        app = get_app_from_backend(self.backend)
        self.assert_app_lookup(app)


class TestMemoryBackend(BackendTestCase):
    """Tests for the in-memory backend."""

    backend_cls = MemoryBackend
    counts = False

    def setUp(self) -> None:
        """Prepare the in-memory backend."""
        # TODO rather than pre-loading GO, use mocks
        _ = pyobo.get_id_name_mapping("go", strict=False)
        self.backend = get_backend(lazy=True)

    def test_backend(self):
        """Test the in-memory backend."""
        self.help_check(self.backend, counts=self.counts)

    def test_app(self):
        """Test the app works properly."""
        app = get_app_from_backend(self.backend)
        self.assert_app_lookup(app)
