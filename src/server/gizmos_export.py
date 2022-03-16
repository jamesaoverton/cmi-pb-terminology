import re

from collections import defaultdict
from gizmos_helpers import add_labels, get_iri
from gizmos.export import get_predicate_ids  # These still work with LDTab
from gizmos.helpers import get_ids           # These still work with LDTab
from sqlalchemy.engine import Connection
from sqlalchemy.sql.expression import bindparam
from sqlalchemy.sql.expression import text as sql_text

# TODO: get correct prefix, in case someone defined rdfs or owl wrong?
LOGIC_PREDICATES = [
    "rdfs:subClassOf",
    "owl:equivalentClass",
    "owl:disjointWith",
    "rdfs:subPropertyOf",
    "rdf:type",
    "rdfs:domain",
    "rdfs:range"
]


def export(
    conn: Connection,
    terms: list,
    predicates: list = None,
    statements: str = "statements",
    where: str = None,
) -> dict:
    """Retrieve details for given terms.
    This is returned as a dictionary of predicate ID -> list of object dictionaries
    (object, datatype, annotation).

    :param conn: SQLAlchemy database connection
    :param terms: list of terms to export (by ID or label)
    :param predicates: list of properties to include in export
    :param statements: name of the ontology statements table
    :param where: SQL WHERE statement to include in query to get terms
    :return: string export in given format
    """
    details = {}

    # Create a tmp labels table & add all labels
    tables = [x["name"] for x in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    if "tmp_labels" not in tables:
        add_labels(conn, statements=statements)

    if terms:
        term_ids = get_ids(conn, terms)
    else:
        term_ids = []
        if where:
            # Use provided query filter to select terms
            query = f'SELECT DISTINCT subject FROM "{statements}" WHERE ' + where
        else:
            # Get all, excluding blank nodes
            query = f'SELECT DISTINCT subject FROM "{statements}" WHERE subject NOT LIKE \'_:%%\''
        for res in conn.execute(query):
            term_ids.append(res["subject"])

    predicate_ids = get_predicate_ids(conn, predicates, statements=statements)

    # Get prefixes
    prefixes = {}
    for row in conn.execute(f"SELECT DISTINCT prefix, base FROM prefix"):
        prefixes[row["prefix"]] = row["base"]

    # Get the term details
    for term in term_ids:
        term_details = get_objects(conn, term, predicate_ids, statements=statements)
        details[term] = term_details

    return details


# TODO: this will be rewritten to be rendered by wiring
def get_objects(
    conn: Connection, term: str, predicate_ids: dict, statements: str = "statements"
) -> dict:
    """Get a dict of predicate ID -> objects."""
    term_objects = defaultdict(list)
    for pred_id in predicate_ids:
        term_objects[pred_id] = list()
    query = sql_text(
        f"""SELECT DISTINCT predicate, object, datatype, annotation
            FROM "{statements}" WHERE subject = :term AND predicate IN :predicates"""
    ).bindparams(bindparam("predicates", expanding=True), bindparam("term"))
    results = conn.execute(query, {"term": term, "predicates": list(predicate_ids.keys())})
    for res in results:
        p = res["predicate"]
        if p not in term_objects:
            term_objects[p] = list()
        term_objects[p].append({"object": res["object"], "datatype": res["datatype"], "annotation": res["annotation"]})
    return term_objects
