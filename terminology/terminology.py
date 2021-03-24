#!/usr/bin/env python3

import gizmos.tree
import gizmos.search

CMI_PB_DB = "build/cmi-pb.db"

PREDICATE_IDS = [
    "rdfs:label",
    "CMI-PB:shortLabel",
    "IAO:0000118",
    "IAO:0000115",
    "IAO:0000119",
    "IAO:0000112",
    "rdf:type",
    "rdfs:subClassOf",
]


def search(text, db=None):
    """Search for a term in CMI-PB based on the text label.
    Return the results in JSON format for Typeahead search."""
    if not db:
        db = CMI_PB_DB
    return gizmos.search.search(db, text)


def term(db=None):
    """Return the HTML tree browser at the top-level Class."""
    return term(None, db=db)


def term(term_id, db=None):
    """Return the HTML tree browser at a given term ID.
    If term_id is None, return the top-level."""
    if not db:
        db = CMI_PB_DB
    return gizmos.tree.tree(
        db,
        term_id,
        title="CMI-PB Terminology",
        href="./{curie}",
        predicate_ids=PREDICATE_IDS,
        include_search=False,
        standalone=False,
    )
