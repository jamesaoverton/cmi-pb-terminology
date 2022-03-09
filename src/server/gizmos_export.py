import re

from collections import defaultdict
from gizmos_helpers import add_labels, get_iri
from gizmos.export import get_predicate_ids, render_output  # These still work with LDTab
from gizmos.helpers import get_ids                          # These still work with LDTab
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
    predicates: list,
    fmt: str,
    default_value_format: str = "IRI",
    no_headers: bool = False,
    split: str = "|",
    standalone: bool = True,
    statements: str = "statements",
    where: str = None,
) -> str:
    """Retrieve details for given terms and render in the given format.

    :param conn: SQLAlchemy database connection
    :param terms: list of terms to export (by ID or label)
    :param predicates: list of properties to include in export
    :param fmt: output format of export (tsv, csv, or html)
    :param default_value_format: how values should be rendered (IRI, CURIE, or label)
    :param no_headers: if true, do not include the header row in export
    :param split: character to split multiple values on in single cell
    :param standalone: if true and format is HTML, include HTML headers
    :param statements: name of the ontology statements table
    :param where: SQL WHERE statement to include in query to get terms
    :return: string export in given format
    """

    # Validate default format
    if default_value_format not in ["CURIE", "IRI", "label"]:
        raise Exception(
            f"The default value format ('{default_value_format}') must be one of: CURIE, IRI, label"
        )
    # Validate output format
    if fmt.lower() not in ["tsv", "csv", "html"]:
        raise Exception(f"Output format '{fmt}' must be one of: tsv, csv, html")

    details = {}

    # Create a tmp labels table & add all labels
    # conn.execute("DROP TABLE IF EXISTS tmp_labels")
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

    if not predicates:
        # Get all predicates if not provided
        predicate_ids = {default_value_format: default_value_format}
        value_formats = {default_value_format: default_value_format.lower()}
        predicate_ids.update(get_predicate_ids(conn, statements=statements))
        query = sql_text(
            "SELECT DISTINCT label FROM tmp_labels WHERE term IN :predicates"
        ).bindparams(bindparam("predicates", expanding=True))
        for res in conn.execute(query, {"predicates": list(predicate_ids.keys())}):
            value_formats[res["label"]] = default_value_format.lower()

    else:
        # Current predicates are IDs or labels - make sure we get all the IDs
        predicate_ids = get_predicate_ids(conn, predicates, statements=statements)
        value_formats = {}
        for p in predicates:
            if p in ["CURIE", "IRI", "label"]:
                value_format = p.lower()
            else:
                value_format = default_value_format.lower()
                m = re.match(r".+ \[(.+)]$", p)
                if m:
                    value_format = m.group(1).lower()
            value_formats[p] = value_format

    # Get prefixes
    prefixes = {}
    for row in conn.execute(f"SELECT DISTINCT prefix, base FROM prefix"):
        prefixes[row["prefix"]] = row["base"]

    # Get the term details
    for term in term_ids:
        term_details = get_term_details(conn, prefixes, term, predicate_ids, statements=statements)
        details[term] = term_details

    return render_output(
        prefixes,
        value_formats,
        predicate_ids,
        details,
        fmt,
        split=split,
        standalone=standalone,
        no_headers=no_headers,
    )


def get_objects(
    conn: Connection, prefixes: dict, term: str, predicate_ids: dict, statements: str = "statements"
) -> dict:
    """Get a dict of predicate label -> objects. The object will either be the term ID or label,
    when the label exists."""
    predicates = [x for x in predicate_ids.keys() if x not in ["CURIE", "IRI", "label"]]
    term_objects = defaultdict(list)
    query = sql_text(
        f"""SELECT DISTINCT predicate, s.object AS object, l.label AS object_label
            FROM "{statements}" s JOIN tmp_labels l ON s.object = l.term
            WHERE s.subject = :term AND s.predicate IN :predicates AND s.datatype = '_IRI'"""
    ).bindparams(bindparam("predicates", expanding=True), bindparam("term"))
    results = conn.execute(query, {"term": term, "predicates": predicates})
    for res in results:
        p = res["predicate"]
        p_label = predicate_ids[p]
        if p_label not in term_objects:
            term_objects[p_label] = list()

        obj = res["object"]
        if obj.startswith("_:"):
            # TODO - handle blank nodes
            continue
        obj_label = res["object_label"]

        d = {"id": obj, "iri": get_iri(prefixes, term)}
        # Maybe add the label
        if obj != obj_label:
            d["label"] = obj_label
        term_objects[p_label].append(d)
    return term_objects


def get_term_details(
    conn: Connection, prefixes: dict, term: str, predicate_ids: dict, statements: str = "statements"
) -> dict:
    """Get a dict of predicate label -> object or value."""
    term_details = {}

    # Handle special cases
    base_dict = {"id": term, "iri": get_iri(prefixes, term)}
    query = sql_text("SELECT label FROM tmp_labels WHERE term = :term")
    res = conn.execute(query, term=term).fetchone()
    if res:
        base_dict["label"] = res["label"]
    if "CURIE" in predicate_ids:
        term_details["CURIE"] = base_dict
    if "IRI" in predicate_ids:
        term_details["IRI"] = base_dict
    if "label" in predicate_ids:
        term_details["label"] = base_dict

    # Get all details
    term_details.update(get_values(conn, term, predicate_ids, statements=statements))
    term_details.update(get_objects(conn, prefixes, term, predicate_ids, statements=statements))
    return term_details


def get_values(
    conn: Connection, term: str, predicate_ids: dict, statements: str = "statements"
) -> dict:
    """Get a dict of predicate label -> literal values."""
    predicates = [x for x in predicate_ids.keys() if x not in ["CURIE", "IRI", "label"]]
    # Remove any non-annotation predicates so we only end up with annotation objects
    predicates = list(set(predicates) - set(LOGIC_PREDICATES))
    term_values = defaultdict(list)
    # Query for all objects using the predicates
    query = sql_text(
        f"""SELECT DISTINCT predicate, object FROM "{statements}" s
            WHERE subject = :term AND predicate IN :predicates AND object IS NOT NULL"""
    ).bindparams(bindparam("predicates", expanding=True), bindparam("term"))
    result = conn.execute(query, {"term": term, "predicates": predicates})
    for res in result:
        p = res["predicate"]
        p_label = predicate_ids[p]
        value = res["object"]
        if value:
            if p_label not in term_values:
                term_values[p_label] = list()
            term_values[p_label].append({"value": value})
    return term_values
