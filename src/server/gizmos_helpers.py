from gizmos.helpers import TOP_LEVELS
from sqlalchemy.engine.base import Connection
from sqlalchemy.sql.expression import bindparam
from sqlalchemy.sql.expression import text as sql_text


def add_labels(conn: Connection, statements="statements"):
    """Create a temporary labels table. If a term does not have a label, the label is the ID."""
    # Create a tmp labels table
    with conn.begin():
        conn.execute("CREATE TABLE tmp_labels(term TEXT PRIMARY KEY, label TEXT)")
        if str(conn.engine.url).startswith("sqlite"):
            # Add all terms with label
            conn.execute(
                f"""INSERT OR IGNORE INTO tmp_labels SELECT subject, object
                    FROM "{statements}" WHERE predicate = 'rdfs:label'"""
            )
            # Update remaining with their ID as their label
            conn.execute(
                f"""INSERT OR IGNORE INTO tmp_labels
                    SELECT DISTINCT subject, subject FROM "{statements}";"""
            )
            conn.execute(
                f"""INSERT OR IGNORE INTO tmp_labels
                    SELECT DISTINCT predicate, predicate FROM "{statements}";"""
            )
        else:
            # Do the same for a psycopg2 Cursor
            conn.execute(
                f"""INSERT INTO tmp_labels
                    SELECT subject, object FROM "{statements}" WHERE predicate = 'rdfs:label'
                    ON CONFLICT (term) DO NOTHING"""
            )
            conn.execute(
                f"""INSERT INTO tmp_labels
                    SELECT DISTINCT subject, subject FROM "{statements}"
                    ON CONFLICT (term) DO NOTHING"""
            )
            conn.execute(
                f"""INSERT INTO tmp_labels
                    SELECT DISTINCT predicate, predicate FROM "{statements}"
                    ON CONFLICT (term) DO NOTHING"""
            )


def get_descendants(conn: Connection, term_id: str, statements: str = "statements") -> set:
    """Return a set of descendants for a given term ID."""
    query = sql_text(
        f"""WITH RECURSIVE descendants(node) AS (
            VALUES (:term_id)
            UNION
             SELECT subject AS node
            FROM "{statements}"
            WHERE predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf')
              AND subject = :term_id
            UNION
            SELECT subject AS node
            FROM "{statements}", descendants
            WHERE descendants.node = "{statements}".object
              AND "{statements}".predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf')
        )
        SELECT * FROM descendants"""
    )
    results = conn.execute(query, term_id=term_id)
    return set([x[0] for x in results])


def get_entity_type(conn: Connection, term_id: str, statements="statements") -> str:
    """Get the OWL entity type for a term."""
    query = sql_text(
        f'SELECT object FROM "{statements}" WHERE subject = :term_id AND predicate = \'rdf:type\''
    )
    results = list(conn.execute(query, term_id=term_id))
    if len(results) > 1:
        for res in results:
            if res["object"] in TOP_LEVELS:
                return res["object"]
        return "owl:Individual"
    elif len(results) == 1:
        entity_type = results[0]["object"]
        if entity_type == "owl:NamedIndividual":
            entity_type = "owl:Individual"
        return entity_type
    else:
        entity_type = None
        query = sql_text(
            f'SELECT predicate FROM "{statements}" WHERE subject = :term_id'
        )
        results = conn.execute(query, term_id=term_id)
        preds = [row["predicate"] for row in results]
        if "rdfs:subClassOf" in preds:
            return "owl:Class"
        elif "rdfs:subPropertyOf" in preds:
            return "owl:AnnotationProperty"
        if not entity_type:
            query = sql_text(f"SELECT predicate FROM {statements} WHERE object = :term_id")
            results = conn.execute(query, term_id=term_id)
            preds = [row["predicate"] for row in results]
            if "rdfs:subClassOf" in preds:
                return "owl:Class"
            elif "rdfs:subPropertyOf" in preds:
                return "owl:AnnotationProperty"
    return "owl:Class"


def get_iri(prefixes: dict, term: str) -> str:
    """Get the IRI from a CURIE."""
    if term.startswith("<"):
        return term.lstrip("<").rstrip(">")
    prefix = term.split(":")[0]
    namespace = prefixes.get(prefix)
    if not namespace:
        raise Exception(f"Prefix '{prefix}' is not defined in prefix table")
    local_id = term.split(":")[1]
    return namespace + local_id


def get_labels(conn, curies, include_top=True, ontology_iri=None, ontology_title=None, statements="statements"):
    labels = {}
    query = sql_text(
        f"""SELECT subject, object FROM "{statements}"
            WHERE subject IN :ids AND predicate = 'rdfs:label' AND object IS NOT NULL"""
    ).bindparams(bindparam("ids", expanding=True))
    results = conn.execute(query, {"ids": list(curies)})
    for res in results:
        labels[res["subject"]] = res["object"]
    if include_top:
        for t, o_label in TOP_LEVELS.items():
            labels[t] = o_label
    if ontology_iri and ontology_title:
        labels[ontology_iri] = ontology_title
    return labels


def get_parent_child_pairs(
    conn: Connection, term_id: str, statements="statements",
):
    query = sql_text(
        f"""WITH RECURSIVE ancestors(parent, child) AS (
        VALUES (:term_id, NULL)
        UNION
        -- The children of the given term:
        SELECT object AS parent, subject AS child
        FROM "{statements}"
        WHERE predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf')
          AND object = :term_id
        UNION
        --- Children of the children of the given term
        SELECT object AS parent, subject AS child
        FROM "{statements}"
        WHERE object IN (SELECT subject FROM "{statements}"
                         WHERE predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf')
                         AND object = :term_id)
          AND predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf')
        UNION
        -- The non-blank parents of all of the parent terms extracted so far:
        SELECT object AS parent, subject AS child
        FROM "{statements}", ancestors
        WHERE ancestors.parent = "{statements}".subject
          AND "{statements}".predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf')
          AND "{statements}".object NOT LIKE '_:%%'
      )
      SELECT * FROM ancestors"""
    )
    results = conn.execute(query, term_id=term_id).fetchall()
    return [[x["parent"], x["child"]] for x in results]
