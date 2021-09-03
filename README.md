<p align="center">
  <img alt="Biolookup Service logo" src="https://github.com/biopragmatics/biolookup/raw/main/src/biolookup/app/static/logo.svg" height="150">
</p>

<h1 align="center">
  Biolookup
</h1>

<p align="center">
    <a href="https://github.com/biopragmatics/biolookup/actions?query=workflow%3ATests">
        <img alt="Tests" src="https://github.com/biopragmatics/biolookup/workflows/Tests/badge.svg" />
    </a>
    <a href="https://github.com/cthoyt/cookiecutter-python-package">
        <img alt="Cookiecutter template from @cthoyt" src="https://img.shields.io/badge/Cookiecutter-python--package-yellow" /> 
    </a>
    <a href="https://pypi.org/project/biolookup">
        <img alt="PyPI" src="https://img.shields.io/pypi/v/biolookup" />
    </a>
    <a href="https://pypi.org/project/biolookup">
        <img alt="PyPI - Python Version" src="https://img.shields.io/pypi/pyversions/biolookup" />
    </a>
    <a href="https://github.com/biopragmatics/biolookup/blob/main/LICENSE">
        <img alt="PyPI - License" src="https://img.shields.io/pypi/l/biolookup" />
    </a>
    <a href='https://biolookup.readthedocs.io/en/latest/?badge=latest'>
        <img src='https://readthedocs.org/projects/biolookup/badge/?version=latest' alt='Documentation Status' />
    </a>
    <a href="https://zenodo.org/badge/latestdoi/400996921">
        <img src="https://zenodo.org/badge/400996921.svg" alt="DOI">
    </a>
    <a href='https://github.com/psf/black'>
        <img src='https://img.shields.io/badge/code%20style-black-000000.svg' alt='Code style: black' />
    </a>
</p>

Get metadata and ontological information about biomedical entities.

### 🔍 Querying the Biolookup Service

The Biolookup Service has an endpoint `/api/lookup/<curie>` for retrieving metadata and ontological
information about a biomedical entity via its compact identifier (CURIE).

```python
import requests

res = requests.get("http://localhost:5000/api/lookup/doid:14330").json()
assert res["name"] == "Parkinson's disease"
assert res["identifier"] == "14330"
assert res["prefix"] == "doid"
assert res["definition"] is not None  # not shown for brevity
```

The [INDRA Lab](https://indralab.github.io) hosts an instance of the Biolookup Service at
http://biolookup.io, so you can alternatively use `http://biolookup.io/api/lookup/doid:14330`.

The same can be accomplished using the `biolookup` package:

```python
import biolookup

res = biolookup.lookup("doid:14330")
assert res["name"] == "Parkinson's disease"
# ... same as before
```

If you've configured the `BIOLOOKUP_SQLALCHEMY_URI` environment variable (or any other valid way
with [`pystow`](https://github.com/cthoyt/pystow) to point directly at the database for an instance
of the Biolookup Service, it will make a direct connection to the database instead of using the
web-based API.

### 🕸️ Running the Lookup App

You can run the lookup app in local mode with:

```shell
$ biolookup web --lazy
```

This means that the in-memory data from `pyobo` are used. If you have a large external database, you
can run in remote mode with the `--sql` flag:

```shell
$ biolookup web --sql --uri postgresql+psycopg2://postgres:biolookup@localhost:5434/biolookup
```

If `--uri` is not given for the `web` subcommand, it
uses `pystow.get_config("biolookup", "sqlalchemy_uri)`to look up from `BIOLOOKUP_SQLALCHEMY_URI` or
in `~/.config/biolookup.ini`. If none is given, it defaults to a SQLite database
in `~/.data/biolookup/biolookup.db`.

### 🗂️ Load the Database

```shell
$ biolookup load --uri postgresql+psycopg2://postgres:biolookup@localhost:5434/biolookup
```

If `--uri` is not given for the `load` subcommand, it
uses `pystow.get_config("biolookup", "sqlalchemy_uri)`to look up from `BIOLOOKUP_SQLALCHEMY_URI` or
in `~/.config/biolookup.ini`. If none is given, it creates a defaults a SQLite database
at `~/.data/biolookup/biolookup.db`.

## 🚀 Installation

The most recent release can be installed from
[PyPI](https://pypi.org/project/biolookup/) with:

```bash
$ pip install biolookup
```

The most recent code and data can be installed directly from GitHub with:

```bash
$ pip install git+https://github.com/biopragmatics/biolookup.git
```

To install in development mode, use the following:

```bash
$ git clone git+https://github.com/biopragmatics/biolookup.git
$ cd biolookup
$ pip install -e .
```

## 👐 Contributing

Contributions, whether filing an issue, making a pull request, or forking, are appreciated. See
[CONTRIBUTING.rst](https://github.com/biopragmatics/biolookup/blob/master/CONTRIBUTING.rst) for more
information on getting involved.

## 👀 Attribution

### ⚖️ License

The code in this package is licensed under the MIT License.

<!--
### 📖 Citation

Citation goes here!
-->

### 🎁 Support

The Biolookup Service was developed by the [INDRA Lab](https://indralab.github.io), a part of the
[Laboratory of Systems Pharmacology](https://hits.harvard.edu/the-program/laboratory-of-systems-pharmacology/about/)
and the [Harvard Program in Therapeutic Science (HiTS)](https://hits.harvard.edu)
at [Harvard Medical School](https://hms.harvard.edu/).

### 💰 Funding

This project has been supported by the following grants:

| Funding Body                                             | Program                                                                                                                       | Grant           |
|----------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|-----------------|
| DARPA                                                    | [Automating Scientific Knowledge Extraction (ASKE)](https://www.darpa.mil/program/automating-scientific-knowledge-extraction) | HR00111990009   |

### 🍪 Cookiecutter

This package was created with [@audreyfeldroy](https://github.com/audreyfeldroy)'s
[cookiecutter](https://github.com/cookiecutter/cookiecutter) package
using [@cthoyt](https://github.com/cthoyt)'s
[cookiecutter-snekpack](https://github.com/cthoyt/cookiecutter-snekpack) template.
