# -*- coding: utf-8 -*-

"""Generate charts for the summary section of the biolookup service."""

import pathlib

import bioregistry
import matplotlib.pyplot as plt
from matplotlib_venn import venn2

from biolookup import backends

HERE = pathlib.Path(__file__).parent.resolve()
STATIC = HERE.joinpath("static")


def main():
    """Generate charts for the biolookup app."""
    # 1
    backend = backends.get_backend(sql=True)
    biolookup_prefixes = set(backend.summarize_names())
    bioregistry_prefixes = set(bioregistry.read_registry())
    fig, ax = plt.subplots()
    venn2([biolookup_prefixes, bioregistry_prefixes], ["Biolookup", "Bioregistry"], ax=ax)
    fig.savefig(STATIC.joinpath("coverage.svg"))
    plt.close(fig)


if __name__ == "__main__":
    main()
