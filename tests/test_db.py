# -*- coding: utf-8 -*-

"""Test for loading the database."""

import gzip
import tempfile
import unittest
from pathlib import Path

import pystow

from biolookup.backends import get_backend, RawSQLBackend
from biolookup.db import loader

REFS = [
    ("p_1", "id_11", "name_11"),
    ("p_1", "id_12", "name_12"),
    ("p_1", "id_13", "name_13"),
    ("p_2", "id_21", "name_21"),
    ("p_2", "id_22", "name_22"),
    ("p_2", "id_23", "name_23"),
]
ALTS = [
    ("p_1", "altid_11", "id_11"),
]
DEFS = [
    ("p_1", "id_11", "def_11"),
    ("p_1", "id_12", "def_12"),
    ("p_1", "id_13", "def_13"),
    ("p_2", "id_21", "def_21"),
    ("p_2", "id_22", "def_22"),
    # ("p_2", "id_23", "def_23"),
]


def _write(path, data, last):
    with gzip.open(path, "wt") as file:
        print("prefix", "identifier", last, sep="\t", file=file)
        for line in data:
            print(*line, sep="\t", file=file)


TEST_URI = pystow.get_config("biolookup", "test_uri")


@unittest.skipUnless(TEST_URI, reason="No biolookup/test_uri configuration found")
class TestDatabase(unittest.TestCase):
    """Tests for the database."""

    def setUp(self) -> None:
        self.refs_table = "refs"
        self.alts_table = "alts"
        self.defs_table = "defs"

    def test_load(self):
        """Test loading the database."""
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            refs_path = directory / "refs.tsv.gz"
            alts_path = directory / "alts.tsv.gz"
            defs_path = directory / "defs.tsv.gz"
            _write(refs_path, REFS, "name")
            _write(alts_path, ALTS, "alt")
            _write(defs_path, DEFS, "definition")
            loader.load(
                uri=TEST_URI,
                refs_path=refs_path,
                refs_table=self.refs_table,
                alts_path=alts_path,
                alts_table=self.alts_table,
                defs_path=defs_path,
                defs_table=self.defs_table,
            )
            backend = get_backend(
                sql=True,
                uri=TEST_URI,
                refs_table=self.refs_table,
                alts_table=self.alts_table,
                defs_table=self.defs_table,
            )
            self.assertIsInstance(backend, RawSQLBackend)
            self.assertEqual("name_11", backend.get_name("p_1", "id_11"))
