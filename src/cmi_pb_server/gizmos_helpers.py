import json
import re

import wiring_rs

from collections import defaultdict
from gizmos.helpers import TOP_LEVELS
from html import escape as html_escape
from sqlalchemy.engine.base import Connection
from sqlalchemy.sql.expression import bindparam
from sqlalchemy.sql.expression import text as sql_text
from typing import Dict, Optional

LOGIC_PREDICATES = [
    "rdfs:subClassOf",
    "owl:equivalentClass",
    "owl:disjointWith",
    "rdfs:subPropertyOf",
    "rdf:type",
    "rdfs:domain",
    "rdfs:range",
]


def flatten(lst):
    for el in lst:
        if isinstance(el, list) and not isinstance(el, (str, bytes)):
            yield from flatten(el)
        else:
            yield el


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


def get_entity_types(conn: Connection, term_ids: list, statement="statement") -> Dict[str, set]:
    query = sql_text(
        f"""SELECT DISTINCT subject, object FROM "{statement}"
            WHERE subject IN :term_ids AND predicate = 'rdf:type'"""
    ).bindparams(bindparam("term_ids", expanding=True))
    results = conn.execute(query, term_ids=term_ids).fetchall()
    all_types = defaultdict(list)
    for res in results:
        term_id = res["subject"]
        if term_id not in all_types:
            all_types[term_id] = list()
        all_types[term_id].append(res["object"])

    entity_types = {}
    for term_id, e_types in all_types.items():
        if len(e_types) >= 1:
            entity_types[term_id] = set(e_types)
        else:
            # Determine if this has a parent class or property and use that to infer type
            entity_type = None
            query = sql_text(f'SELECT predicate FROM "{statement}" WHERE subject = :term_id')
            results = conn.execute(query, term_id=term_id)
            preds = [row["predicate"] for row in results]
            if "rdfs:subClassOf" in preds:
                entity_types[term_id] = {"owl:Class"}
            elif "rdfs:subPropertyOf" in preds:
                entity_types[term_id] = {"owl:AnnotationProperty"}
            if not entity_type:
                query = sql_text(f"SELECT predicate FROM {statement} WHERE object = :term_id")
                results = conn.execute(query, term_id=term_id)
                preds = [row["predicate"] for row in results]
                if "rdfs:subClassOf" in preds:
                    entity_types[term_id] = {"owl:Class"}
                elif "rdfs:subPropertyOf" in preds:
                    entity_types[term_id] = {"owl:AnnotationProperty"}
        # No type could be determined, set to owl:Class
        if term_id not in entity_types:
            entity_types[term_id] = {"owl:Class"}
    return entity_types


def get_entity_type(conn: Connection, term_id: str, statements="statements") -> str:
    """Get a single OWL entity type for a term. This will not include the types of named inviduals,
    rather a named individual will have the type owl:Individual."""
    query = sql_text(
        f"SELECT object FROM \"{statements}\" WHERE subject = :term_id AND predicate = 'rdf:type'"
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
        # Check if this is used as a subClass or subProperty
        entity_type = None
        query = sql_text(f'SELECT predicate FROM "{statements}" WHERE subject = :term_id')
        results = conn.execute(query, term_id=term_id)
        preds = [row["predicate"] for row in results]
        if "rdfs:subClassOf" in preds:
            return "owl:Class"
        elif "rdfs:subPropertyOf" in preds:
            return "owl:AnnotationProperty"
        if not entity_type:
            # Check if this is used as a parent property or parent class
            query = sql_text(f"SELECT predicate FROM {statements} WHERE object = :term_id")
            results = conn.execute(query, term_id=term_id)
            preds = [row["predicate"] for row in results]
            if "rdfs:subClassOf" in preds:
                return "owl:Class"
            elif "rdfs:subPropertyOf" in preds:
                return "owl:AnnotationProperty"
    return "owl:Class"


def get_html_label(iri, labels, predicate=None):
    iri_label = iri
    if iri.startswith("<") and iri.endswith(">"):
        iri_label = iri[1:-1]
    html_label = ["a"]
    if predicate:
        html_label.append({"property": predicate, "resource": iri})
    else:
        html_label.append({"resource": iri})
    html_label.append(labels.get(iri, html_escape(iri_label)))
    return html_label


def get_ids(conn, id_or_labels, raise_exc=True, statement="statement"):
    """Create a list of IDs from a list of IDs or labels.

    :param conn: connection containing ontology statement table
    :param id_or_labels: list of ID or labels
    :param raise_exc: raise exception if any ID or label does not exist in given table
    :param statement: statement table to get IDs from
    :return: list of IDs from original list of IDs or labels"""
    ids = []
    query = sql_text(
        f"""SELECT DISTINCT subject, object FROM "{statement}"
           WHERE predicate = 'rdfs:label' AND object IN :id_or_labels""",
    ).bindparams(bindparam("id_or_labels", expanding=True))
    results = conn.execute(query, id_or_labels=id_or_labels).fetchall()
    for res in results:
        label = res["object"]
        if label in id_or_labels:
            # label should be in list, but could cause issue if two terms have same label
            id_or_labels.remove(label)
        ids.append(res["subject"])
    if id_or_labels:
        # The remainder should be IDs, but we want to check that they exist
        query = sql_text(
            f'SELECT DISTINCT subject FROM "{statement}" WHERE subject in :id_or_labels',
        ).bindparams(bindparam("id_or_labels", expanding=True))
        results = conn.execute(query, id_or_labels=id_or_labels).fetchall()
        for res in results:
            term = res["subject"]
            if term in id_or_labels:
                id_or_labels.remove(term)
            ids.append(term)
    if id_or_labels and raise_exc:
        raise Exception(
            f"The following terms do not exist in '{statement}': " + ", ".join(id_or_labels)
        )
    elif id_or_labels:
        # Did not throw exception, add the rest
        ids.extend(id_or_labels)
    return ids


def get_predicate_ids(
    conn: Connection, id_or_labels: list = None, statement: str = "statement"
) -> dict:
    """Create a map of predicate label or full header (if the header has a value format) -> ID."""
    if id_or_labels:
        predicate_ids = {}
        for id_or_label in id_or_labels:
            # Support for special export headers
            m = re.match(r"(.+) \[.+]$", id_or_label)
            if m:
                id_or_label = m.group(1)
            query = sql_text(
                f"""SELECT subject FROM "{statement}"
                    WHERE predicate = 'rdfs:label' AND object = :id_or_label"""
            )
            res = conn.execute(query, id_or_label=id_or_label).fetchone()
            if res:
                predicate_ids[res["subject"]] = id_or_label
            else:
                # TODO: we are currently allowing IDs that do not exist in database
                predicate_ids[id_or_label] = id_or_label
        return predicate_ids
    else:
        # Get all predicates
        results = conn.execute(
            f"""WITH predicate_labels AS (
                SELECT DISTINCT s1.predicate, s2.object FROM "{statement}" s1
                JOIN "{statement}" s2 ON s1.predicate = s2.subject
                WHERE s2.predicate = 'rdfs:label'
            )
            SELECT DISTINCT s.predicate AS predicate, p.object AS label FROM "{statement}" s
            LEFT JOIN predicate_labels p ON s.predicate = p.predicate"""
        ).fetchall()
        return {res["predicate"]: res["label"] for res in results}


def get_term_attributes(
    conn: Connection,
    exclude_json: bool = False,
    include_all_predicates: bool = True,
    predicates: list = None,
    statement: str = "statement",
    terms: Optional[list] = None,
    where: str = None,
) -> dict:
    """Retrieve all attributes for given terms from the SQL database. If no terms are provided,
    return details for all terms in database. This is returned as a dictionary of predicate ID ->
    list of object dictionaries (object, datatype, annotation).

    :param conn: SQLAlchemy database connection
    :param exclude_json: if True, do not include objects with the _JSON datatype (anonymous)
    :param include_all_predicates: if True, include predicates in the return dicts even if they
                                   have no values for a given term.
    :param predicates: list of properties to include in export
    :param statement: name of the ontology statements table
    :param terms: list of terms to export (by ID or label)
    :param where: SQL WHERE statement to include in query to get terms
    :return: string export in given format
    """
    if terms:
        # Use list of terms (IDs or labels) to get a list of term IDs
        term_ids = get_ids(conn, terms, statement=statement)
    elif where:
        # Use provided query filter to select terms
        query = f'SELECT DISTINCT subject FROM "{statement}" WHERE ' + where
        term_ids = [res["subject"] for res in conn.execute(query)]
    else:
        # No term IDs, we will return details for all subjects in the database
        term_ids = None

    predicate_ids = get_predicate_ids(conn, id_or_labels=predicates, statement=statement)

    # Get prefixes
    prefixes = {}
    for row in conn.execute(f"SELECT DISTINCT prefix, base FROM prefix"):
        prefixes[row["prefix"]] = row["base"]

    # Get the term details
    return get_objects(
        conn,
        predicate_ids,
        exclude_json=exclude_json,
        include_all_predicates=include_all_predicates,
        statement=statement,
        term_ids=term_ids,
    )


def get_objects(
    conn: Connection,
    predicate_ids: dict,
    exclude_json: bool = False,
    include_all_predicates: bool = True,
    statement: str = "statement",
    term_ids: list = None,
) -> dict:
    """Get a dict of predicate ID -> objects."""
    term_objects = defaultdict(defaultdict)
    if include_all_predicates:
        # Build dict of all terms with all predicates
        tmp_ids = term_ids
        if not tmp_ids:
            # If term IDs were not included, retrieve all subjects
            # We use a "temp" variable here so that we don't have to pass all to query
            tmp_ids = [
                x["subject"]
                for x in conn.execute(f'SELECT DISTINCT subject FROM "{statement}";').fetchall()
            ]
        for term_id in tmp_ids:
            term_objects[term_id] = defaultdict(list)
            for p in predicate_ids.keys():
                term_objects[term_id][p] = list()

    results = []
    if term_ids:
        # Max num of sql variables is 999 - use chunks to get around this
        chunks = [term_ids[i : i + 999] for i in range(0, len(term_ids), 999)]
        for chunk in chunks:
            query = f"""SELECT DISTINCT subject, predicate, object, datatype, annotation
                    FROM "{statement}" WHERE subject IN :terms AND predicate IN :predicates"""
            if exclude_json:
                query += " AND datatype IS NOT '_JSON'"
            query = sql_text(query).bindparams(
                bindparam("terms", expanding=True), bindparam("predicates", expanding=True)
            )
            results.extend(
                conn.execute(
                    query, {"terms": chunk, "predicates": list(predicate_ids.keys())}
                ).fetchall()
            )
    else:
        query = f"""SELECT DISTINCT subject, predicate, object, datatype, annotation
                FROM "{statement}" WHERE predicate IN :predicates"""
        if exclude_json:
            query += " AND datatype IS NOT '_JSON'"
        query = sql_text(query).bindparams(bindparam("predicates", expanding=True))
        results.extend(conn.execute(query, {"predicates": list(predicate_ids.keys())}).fetchall())

    for res in results:
        s = res["subject"]
        p = res["predicate"]
        if p not in term_objects[s]:
            term_objects[s][p] = list()
        term_objects[s][p].append(
            {"object": res["object"], "datatype": res["datatype"], "annotation": res["annotation"]}
        )
    return term_objects


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


def get_labels(conn, curies: list, ontology_iri=None, ontology_title=None, statement="statement"):
    labels = {}
    query = sql_text(
        f"""SELECT subject, object FROM "{statement}"
            WHERE subject IN :ids AND predicate = 'rdfs:label' AND object IS NOT NULL"""
    ).bindparams(bindparam("ids", expanding=True))
    results = conn.execute(query, {"ids": curies})
    for res in results:
        labels[res["subject"]] = res["object"]
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
          AND datatype = '_IRI'
        UNION
        --- Children of the children of the given term
        SELECT object AS parent, subject AS child
        FROM "{statements}"
        WHERE object IN (SELECT subject FROM "{statements}"
                         WHERE predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf')
                         AND object = :term_id)
          AND predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf')
          AND datatype = '_IRI'
        UNION
        -- The non-blank parents of all of the parent terms extracted so far:
        SELECT object AS parent, subject AS child
        FROM "{statements}", ancestors
        WHERE ancestors.parent = "{statements}".subject
          AND "{statements}".predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf')
          AND "{statements}".object NOT LIKE '_:%%' AND "{statements}".datatype = '_IRI'
      )
      SELECT * FROM ancestors"""
    )
    results = conn.execute(query, term_id=term_id).fetchall()
    return [[x["parent"], x["child"]] for x in results]


def object_to_hiccup(
    predicate, obj, labels, entity_types, as_list=False, include_annotations=False
) -> list:
    if as_list:
        ele = ["li"]
    else:
        ele = ["p"]
    dt = obj["datatype"]
    if dt.lower() == "_json":
        # TODO: change to RDFa rendering here when ready (returns hiccup)
        typed = wiring_rs.ofn_typing(json.dumps(obj["object"]), entity_types)
        labeled = wiring_rs.ofn_labeling(typed, labels)
        ele.append(json.loads(wiring_rs.object_2_rdfa(labeled)))
    elif dt.lower() == "_iri":
        obj_label = get_html_label(obj["object"], labels, predicate=predicate)
        ele.append(obj_label)
    else:
        if dt.startswith("@"):
            dt_display = dt
        else:
            dt_display = ["a", {"resource": dt}, dt]
        ele.append(obj["object"])
        ele.append(["sup", {"class": "text-black-50"}, dt_display])
    if obj["annotation"] and include_annotations:
        ann_ele = ["ul"]
        for ann_predicate, ann_objects in obj["annotation"].items():
            pred_ele = ["ul"]
            for ao in ann_objects:
                # TODO: support _json?
                if ao["datatype"].lower() == "_iri":
                    ao_label = get_html_label(ao["object"], labels, predicate=ann_predicate)
                    pred_ele.append(["li", ["small", ao_label]])
                else:
                    # TODO: render datatype/lang tags
                    pred_ele.append(["li", ["small", html_escape(ao["object"])]])
            ann_pred_label = get_html_label(ann_predicate, labels)
            ann_ele.append(["li", ["small", ann_pred_label], pred_ele])
        ele.append(ann_ele)
    return ele


def object_to_str(obj, labels, entity_types):
    dt = obj["datatype"]
    if dt.lower() == "_json":
        typed = wiring_rs.ofn_typing(json.dumps(obj["object"]), entity_types)
        labeled = wiring_rs.ofn_labeling(typed, labels)
        return wiring_rs.ofn_2_man(labeled)
    elif dt.lower() == "_iri":
        return labels.get(obj["object"], obj["object"])
    else:
        # TODO: datatypes?
        return obj["object"]


def objects_to_hiccup(
    conn, data, include_annotations=False, single_item_list=False, statement="statement"
):
    """
    :param conn:
    :param data:
    :param include_annotations: if True, include axiom annotations as sub-lists
    :param statement:
    """
    # First pass to render as OFN list and get all the needed term IDs for labeling
    pre_render, object_ids = pre_render_objects(data)

    # Get labels and entity types for Manchester rendering
    object_ids = list(object_ids)
    labels = get_labels(conn, object_ids, statement=statement)
    entity_types = get_entity_types(conn, object_ids, statement=statement)

    # Second pass to render the OFN as Manchester with labels
    rendered = {}
    for term_id, predicate_objects in pre_render.items():
        rendered_term = defaultdict()
        for predicate, objs in predicate_objects.items():
            if len(objs) > 1 or single_item_list:
                lst = ["ul", {"class": "annotations"}]
                lst.extend(
                    [
                        object_to_hiccup(
                            predicate,
                            x,
                            labels,
                            entity_types,
                            as_list=True,
                            include_annotations=include_annotations,
                        )
                        for x in objs
                    ]
                )
                rendered_term[predicate] = lst
            elif len(objs) == 1:
                rendered_term[predicate] = object_to_hiccup(
                    predicate,
                    objs[0],
                    labels,
                    entity_types,
                    include_annotations=include_annotations,
                )
            else:
                rendered_term[predicate] = []
        # term ID -> predicate IDs -> hiccup lists
        rendered[term_id] = rendered_term
    return rendered


def pre_render_objects(data):
    pre_render = {}
    object_ids = set()
    for term_id, predicate_objects in data.items():
        object_ids.add(term_id)
        pre_render_term = defaultdict()
        for predicate, objs in predicate_objects.items():
            object_ids.add(predicate)
            pre_render_po = []
            for obj in objs:
                annotation = obj["annotation"]
                pre_render_annotation = defaultdict(list)
                if annotation:
                    # TODO: do we need to support more levels of annotations?
                    annotation = json.loads(obj["annotation"])
                    for ann_predicate, anns in annotation.items():
                        object_ids.add(ann_predicate)
                        pre_render_annotation[ann_predicate] = list()
                        for ann in anns:
                            # TODO: support _json?
                            if ann["datatype"].lower() == "_iri":
                                object_ids.add(ann["object"])
                            pre_render_annotation[ann_predicate].append(
                                {"object": ann["object"], "datatype": ann["datatype"]}
                            )

                if obj["datatype"].lower() == "_json":
                    ofn = wiring_rs.object_2_ofn(obj["object"])
                    pre_render_po.append(
                        {
                            "object": json.loads(ofn),
                            "datatype": obj["datatype"],
                            "annotation": pre_render_annotation,
                        }
                    )
                    object_ids.update(wiring_rs.get_signature(ofn))
                elif obj["datatype"].lower() == "_iri":
                    pre_render_po.append(
                        {
                            "object": obj["object"],
                            "datatype": obj["datatype"],
                            "annotation": pre_render_annotation,
                        }
                    )
                    object_ids.add(obj["object"])
                else:
                    pre_render_po.append(
                        {
                            "object": obj["object"],
                            "datatype": obj["datatype"],
                            "annotation": pre_render_annotation,
                        }
                    )
            pre_render_term[predicate] = pre_render_po
        pre_render[term_id] = pre_render_term
    return pre_render, object_ids


def terms_to_dict(conn, data, sep="|", statement="statement"):
    """Transform data from SQLite database to a list of dicts suitable for DictWriters."""
    # First pass to render as OFN list and get all the needed term IDs for labeling
    pre_render, object_ids = pre_render_objects(data)

    # Get labels and entity types for Manchester rendering
    object_ids = list(object_ids)
    labels = get_labels(conn, object_ids, statement=statement)
    entity_types = get_entity_types(conn, object_ids, statement=statement)

    # Second pass to render the OFN as Manchester with labels (term
    rendered = []
    for term_id, predicate_objects in pre_render.items():
        rendered_term = {"ID": term_id}
        for predicate, objs in predicate_objects.items():
            pred_label = labels.get(predicate, predicate)
            strs = [object_to_str(o, labels, entity_types) for o in objs]
            rendered_term[pred_label] = sep.join(strs)
        rendered.append(rendered_term)
    return rendered
