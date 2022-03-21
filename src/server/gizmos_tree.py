import json

from collections import defaultdict
from gizmos_helpers import get_entity_type, get_html_label, get_iri, get_labels, get_parent_child_pairs, objects_to_hiccup
from gizmos.helpers import TOP_LEVELS
from gizmos.hiccup import render
from gizmos.tree import bootstrap_css, bootstrap_js, parent2tree, PLUS, popper_js, typeahead_js
from sqlalchemy.engine import Connection
from sqlalchemy.sql.expression import bindparam
from sqlalchemy.sql.expression import text as sql_text
from typing import Optional


def annotations2rdfa(
    treename: str,
    data: dict,
    predicate_ids: list,
    term_id: str,
    stanza: list,
    href: str = "?term={curie}",
) -> list:
    """Create a hiccup-style vector for the annotation on a term."""
    # TODO: should this use wiring to display nested?
    # The subjects in the stanza that are of type owl:Axiom:

    # The initial hiccup, which will be filled in later:
    items = ["ul", {"id": "annotations", "class": "col-md"}]
    labels = data["labels"]

    # dict of predicate -> object -> annotations (or None)
    pred_objects = defaultdict(dict)
    for row in stanza:
        if row["subject"] == term_id:
            pred = row["predicate"]
            if predicate_ids and pred not in predicate_ids:
                continue
            if pred not in pred_objects:
                pred_objects[pred] = {}
            if row["annotation"]:
                pred_objects[pred][row["object"]] = json.loads(row["annotation"])
            else:
                pred_objects[pred][row["object"]] = None

    # Loop through the rows of the stanza that correspond to the predicates of the given term:
    for predicate in predicate_ids:
        objs = pred_objects.get(predicate)
        if not objs:
            continue
        # Get a backup label of the pred ID if label cannot be found
        predicate_label = predicate
        if predicate.startswith("<"):
            predicate_label = predicate.lstrip("<").rstrip(">")
        anchor = [
            "a",
            {"href": href.format(curie=predicate, db=treename)},
            labels.get(predicate, predicate_label),
        ]
        # Initialise an empty list of "o"s, i.e., hiccup representations of objects:
        html = []
        for obj, annotations in objs.items():
            # TODO: use wiring to render HTML (replaces row2o)
            # Convert the `data` map, that has entries for the tree and for a list of the labels
            # corresponding to all of the curies in the stanza, into a hiccup object `o`:
            o = ["li", obj]

            # Check for annotations
            if annotations:
                ele = []
                for ann_pred, ann_values in annotations.items():
                    # Get a backup label of the pred ID if label cannot be found
                    predicate_label = ann_pred
                    if ann_pred.startswith("<"):
                        predicate_label = ann_pred.lstrip("<").rstrip(">")
                    ann_anchor = [
                        "li",
                        [
                            "small",
                            [
                                "a",
                                {"href": href.format(curie=ann_pred, db=treename)},
                                data["labels"].get(ann_pred, predicate_label),
                            ],
                        ],
                    ]
                    lst = []
                    for v in ann_values:
                        # TODO: render with wiring (replaces row2o)
                        lst.append(["li", ["small", v["object"]]])
                    ele.append(["ul", ann_anchor, ["ul"] + lst])
                o += ele

            # Append the `o` to the list of `os`:
            html.append(o)
        if objs:
            items.append(["li", anchor, ["ul"] + html])
    return items


def get_hierarchy(
    conn: Connection,
    term_id: str,
    entity_type: str,
    add_children: list = None,
    statements: str = "statements",
) -> (dict, set):
    """Return a hierarchy dictionary for a term and all its ancestors and descendants."""
    # Build the hierarchy
    if entity_type == "owl:Individual":
        query = sql_text(
            f"""SELECT DISTINCT object AS parent, subject AS child FROM {statements}
                WHERE subject = :term_id
                 AND predicate = 'rdf:type'
                 AND object NOT IN ('owl:Individual', 'owl:NamedIndividual')
                 AND object NOT LIKE '_:%%'
                 AND datatype = '_IRI'"""
        )
        results = [[x["parent"], x["child"]] for x in conn.execute(query, term_id=term_id)]
    else:
        results = get_parent_child_pairs(conn, term_id, statements=statements)
    if add_children:
        results.extend([[term_id, child] for child in add_children])

    hierarchy = {
        entity_type: {"parents": [], "children": []},
        term_id: {"parents": [], "children": []},
    }
    curies = set()
    for res in results:
        # Consider the parent column of the current row:
        parent = res[0]
        if not parent or parent == "owl:Thing":
            continue
        # If it is not null, add it to the list of all of the compact URIs described by this tree:
        curies.add(parent)
        # If it is not already in the tree, add a new entry for it to the tree:
        if parent not in hierarchy:
            hierarchy[parent] = {
                "parents": [],
                "children": [],
            }

        # Consider the child column of the current row:
        child = res[1]
        if not child:
            continue
        # If it is not null, add it to the list of all the compact URIs described by this tree:
        curies.add(child)
        # If the child is not already in the tree, add a new entry for it to the tree:
        if child not in hierarchy:
            hierarchy[child] = {
                "parents": [],
                "children": [],
            }

        # Fill in the appropriate relationships in the entries for the parent and child:
        hierarchy[parent]["children"].append(child)
        hierarchy[child]["parents"].append(parent)

    if not hierarchy[term_id]["parents"]:
        # Place cur term directly under top level entity
        hierarchy[term_id]["parents"].append(entity_type)
        hierarchy[entity_type]["children"].append(term_id)

    # Add entity type as top level to anything without a parent
    for term_id, mini_tree in hierarchy.items():
        if not mini_tree["parents"]:
            hierarchy[term_id]["parents"].append(entity_type)

    return hierarchy, curies


def get_ontology(conn: Connection, prefixes: dict, statements: str = "statements") -> (str, str):
    """Get the ontology IRI and title (or None).

    :param conn: database connection
    :param prefixes: dict of prefix -> base
    :param statements: name of the statements table (default: statements)
    :return: IRI, title or None
    """
    res = conn.execute(
        f"""SELECT subject FROM "{statements}"
        WHERE predicate = 'rdf:type' AND object = 'owl:Ontology'"""
    ).fetchone()
    if not res:
        return None, None
    iri = res["subject"]
    dct = "<http://purl.org/dc/terms/title>"
    for prefix, base in prefixes.items():
        if base == "http://purl.org/dc/terms/":
            dct = f"{prefix}:title"
    query = sql_text(f'SELECT object FROM "{statements}" WHERE subject = :iri AND predicate = :dct')
    res = conn.execute(query, iri=iri, dct=dct).fetchone()
    if not res:
        return iri, None
    return iri, res["object"]


def get_sorted_predicates(
    conn: Connection, exclude_ids: list = None, statements: str = "statements"
) -> list:
    """Return a list of predicates IDs sorted by their label, optionally excluding some predicate
    IDs. If the predicate does not have a label, use the ID as the label."""
    exclude = None
    if exclude_ids:
        exclude = ", ".join([f"'{x}'" for x in exclude_ids])

    # Retrieve all predicate IDs
    results = conn.execute(f'SELECT DISTINCT predicate FROM "{statements}"')
    all_predicate_ids = [x["predicate"] for x in results]
    if exclude:
        all_predicate_ids = [x for x in all_predicate_ids if x not in exclude_ids]

    # Retrieve predicates with labels
    query = sql_text(
        f"""SELECT DISTINCT subject, object
            FROM "{statements}" WHERE subject IN :ap AND predicate = 'rdfs:label';"""
    ).bindparams(bindparam("ap", expanding=True))
    results = conn.execute(query, {"ap": all_predicate_ids})
    predicate_label_map = {x["subject"]: x["object"] for x in results}

    # Add unlabeled predicates to map with label = ID
    for p in all_predicate_ids:
        if p not in predicate_label_map:
            predicate_label_map[p] = p

    # Return list of keys sorted by value (label)
    return [k for k, v in sorted(predicate_label_map.items(), key=lambda x: x[1].lower())]


def term2rdfa(
    conn: Connection,
    prefixes: dict,
    treename: str,
    predicate_ids: list,
    term_id: str,
    stanza: list,
    title: str = None,
    add_children: list = None,
    href: str = "?id={curie}",
    max_children: int = 100,
    statements: str = "statements",
) -> (str, str):
    """Create a hiccup-style HTML vector for the given term."""
    ontology_iri, ontology_title = get_ontology(conn, prefixes, statements=statements)
    if term_id not in TOP_LEVELS:
        # Get a hierarchy under the entity type
        entity_type = get_entity_type(conn, term_id, statements=statements)
        hierarchy, curies = get_hierarchy(
            conn, term_id, entity_type, add_children=add_children, statements=statements
        )
    else:
        # Get the top-level for this entity type
        entity_type = term_id
        if term_id == "ontology":
            hierarchy = {term_id: {"parents": [], "children": []}}
            curies = set()
            if ontology_iri:
                curies.add(ontology_iri)
        else:
            pred = None
            if term_id == "owl:Individual":
                # No user input, safe to use f-string for query
                tls = ", ".join([f"'{x}'" for x in TOP_LEVELS.keys()])
                results = conn.execute(
                    f"""SELECT DISTINCT subject FROM {statements}
                    WHERE subject NOT IN
                        (SELECT subject FROM {statements}
                         WHERE predicate = 'rdf:type'
                         AND object NOT IN ('owl:Individual', 'owl:NamedIndividual'))
                    AND subject IN
                        (SELECT subject FROM {statements}
                         WHERE predicate = 'rdf:type' AND object NOT IN ({tls}))"""
                )
            elif term_id == "rdfs:Datatype":
                results = conn.execute(
                    f"""SELECT DISTINCT subject FROM {statements}
                        WHERE predicate = 'rdf:type' AND object = 'rdfs:Datatype'"""
                )
            else:
                pred = "rdfs:subPropertyOf"
                if term_id == "owl:Class":
                    pred = "rdfs:subClassOf"
                # Select all classes without parents and set them as children of owl:Thing
                query = sql_text(
                    f"""SELECT DISTINCT subject FROM {statements} 
                    WHERE subject NOT IN 
                        (SELECT subject FROM {statements}
                         WHERE predicate = :pred
                         AND object != 'owl:Thing')
                    AND subject IN 
                        (SELECT subject FROM {statements} 
                         WHERE predicate = 'rdf:type'
                         AND object = :term_id AND subject NOT LIKE '_:%%'
                         AND subject NOT IN ('owl:Thing', 'rdf:type'));"""
                )
                results = conn.execute(query, pred=pred, term_id=term_id)
            children = [res["subject"] for res in results]
            child_children = defaultdict(set)
            if pred and children:
                # Get children of children for classes & properties
                query = sql_text(
                    f"""SELECT DISTINCT object AS parent, subject AS child FROM {statements}
                    WHERE predicate = :pred AND object IN :children"""
                ).bindparams(bindparam("pred"), bindparam("children", expanding=True))
                results = conn.execute(query, {"pred": pred, "children": children})
                for res in results:
                    p = res["parent"]
                    if p not in child_children:
                        child_children[p] = set()
                    child_children[p].add(res["child"])
            hierarchy = {term_id: {"parents": [], "children": children}}
            curies = {term_id}
            for c in children:
                c_children = child_children.get(c, set())
                hierarchy[c] = {"parents": [term_id], "children": list(c_children)}
                curies.update(c_children)
                curies.add(c)

    # Add all of the other compact URIs in the stanza to the set of compact URIs:
    stanza.sort(key=lambda x: x["predicate"])
    for row in stanza:
        curies.add(row.get("subject"))
        curies.add(row.get("predicate"))
        if row.get("datatype") == "_IRI":
            curies.add(row.get("object"))
    curies.discard("")
    curies.discard(None)

    # Get all the prefixes that are referred to by the compact URIs:
    ps = set()
    for curie in curies:
        if not isinstance(curie, str) or len(curie) == 0 or curie[0] in ("_", "<"):
            continue
        prefix, local = curie.split(":")
        ps.add(prefix)

    # Get all of the rdfs:labels corresponding to all of the compact URIs, in the form of a map
    # from compact URIs to labels:
    labels = get_labels(
        conn, curies, ontology_iri=ontology_iri, ontology_title=ontology_title, statement=statements
    )

    obsolete = []
    query = sql_text(
        f"""SELECT DISTINCT subject FROM {statements}
            WHERE subject in :ids AND predicate='owl:deprecated' AND lower(object)='true'"""
    ).bindparams(bindparam("ids", expanding=True))
    results = conn.execute(query, {"ids": list(curies)})
    for res in results:
        obsolete.append(res["subject"])

    # If the compact URIs in the labels map are also in the tree, then add the label info to the
    # corresponding node in the tree:
    for key in hierarchy.keys():
        if key in labels:
            hierarchy[key]["label"] = labels[key]

    # Initialise a map with one entry for the tree and one for all of the labels corresponding to
    # all of the compact URIs in the stanza:
    data = {"labels": labels, "obsolete": obsolete, treename: hierarchy, "iri": ontology_iri}

    # Determine the label to use for the given term id when generating RDFa (the term might have
    # multiple labels, in which case we will just choose one and show it everywhere). This defaults
    # to the term id itself, unless there is a label for the term in the stanza corresponding to the
    # label for that term in the labels map:
    if term_id in labels:
        selected_label = labels[term_id]
    else:
        selected_label = term_id
    label = term_id
    for row in stanza:
        predicate = row["predicate"]
        value = row["object"]
        if predicate == "rdfs:label" and value == selected_label:
            label = value
            break

    subject = None
    si = None
    subject_label = None
    if term_id == "ontology" and ontology_iri:
        subject = ontology_iri
        subject_label = data["labels"].get(ontology_iri, ontology_iri)
        si = get_iri(prefixes, subject)
    elif term_id != "ontology":
        subject = term_id
        si = get_iri(prefixes, subject)
        subject_label = label

    rdfa_tree = term2tree(
        data, treename, term_id, entity_type, href=href, max_children=max_children
    )

    if not title:
        title = treename + " Browser"

    if (term_id in TOP_LEVELS and term_id != "ontology") or (
        term_id == "ontology" and not ontology_iri
    ):
        si = None
        if ontology_iri:
            si = get_iri(prefixes, ontology_iri)
        items = [
            "ul",
            {"id": "annotations", "class": "col-md"},
            ["p", {"class": "lead"}, "Hello! This is an ontology browser."],
            [
                "p",
                "An ",
                [
                    "a",
                    {"href": "https://en.wikipedia.org/wiki/Ontology_(information_science)"},
                    "ontology",
                ],
                " is a terminology system designed for both humans and machines to read. Click the",
                " links on the left to browse the hierarchy of terms. Terms have parent terms, ",
                "child terms, annotations, and ",
                [
                    "a",
                    {"href": "https://en.wikipedia.org/wiki/Web_Ontology_Language"},
                    "logical axioms",
                ],
                ". The page for each term is also machine-readable using ",
                ["a", {"href": "https://en.wikipedia.org/wiki/RDFa"}, "RDFa"],
                ".",
            ],
        ]
        term = [
            "div",
            ["div", {"class": "row"}, ["h2", title]],
        ]
        if si:
            # If ontology IRI, add it to the page
            term.append(["div", {"class": "row"}, ["a", {"href": si}, si]])
        term.append(["div", {"class": "row", "style": "padding-top: 10px;"}, rdfa_tree, items])
    else:
        items = objects_to_hiccup(conn, data, include_annotations=True, statement=statements)
        items = annotations2rdfa(treename, data, predicate_ids, subject, stanza, href=href)
        term = [
            "div",
            {"resource": subject},
            ["div", {"class": "row"}, ["h2", subject_label]],
            ["div", {"class": "row"}, ["a", {"href": si}, si]],
            ["div", {"class": "row", "style": "padding-top: 10px;"}, rdfa_tree, items],
        ]
    return ps, term


def term2tree(
    data: dict,
    treename: str,
    term_id: str,
    entity_type: str,
    href: str = "?id={curie}",
    max_children: int = 100,
) -> list:
    """Create a hiccup-style HTML hierarchy vector for the given term."""
    if treename not in data or term_id not in data[treename]:
        return []

    term_tree = data[treename][term_id]
    obsolete = data["obsolete"]
    child_labels = []
    obsolete_child_labels = []
    for child in term_tree["children"]:
        if child in obsolete:
            obsolete_child_labels.append([child, data["labels"].get(child, child)])
        else:
            child_labels.append([child, data["labels"].get(child, child)])
    child_labels.sort(key=lambda x: x[1].lower())
    obsolete_child_labels.sort(key=lambda x: x[1].lower())
    child_labels.extend(obsolete_child_labels)

    if entity_type == "owl:Class":
        predicate = "rdfs:subClassOf"
    elif entity_type == "owl:Individual":
        predicate = "rdf:type"
    else:
        predicate = "rdfs:subPropertyOf"

    # Get the children for our target term
    children = []
    for child, label in child_labels:
        if child not in data[treename]:
            continue
        oc = child
        object_label = tree_label(data, treename, oc)
        o = ["a", {"rev": predicate, "resource": oc}, object_label]
        # Check for children of the child and add a plus next to label if so
        if data[treename][oc]["children"]:
            o.append(PLUS)
        attrs = {}
        if len(children) >= max_children:
            attrs["style"] = "display: none"
        children.append(["li", attrs, o])

    if len(children) >= max_children:
        total = len(term_tree["children"])
        attrs = {"href": f"javascript:show_children()"}
        children.append(["li", {"id": "more"}, ["a", attrs, f"Click to show all {total} ..."]])
    children = ["ul", {"id": "children"}] + children
    if len(children) == 0:
        children = ""
    term_label = tree_label(data, treename, term_id)

    # Get the parents for our target term
    parents = term_tree["parents"]
    if parents:
        hierarchy = ["ul"]
        for p in parents:
            if p.startswith("_:"):
                continue
            hierarchy.append(parent2tree(data, treename, term_id, children.copy(), p, href=href))
    else:
        hierarchy = ["ul", ["li", term_label, children]]

    i = 0
    hierarchies = ["ul", {"id": f"hierarchy", "class": "hierarchy multiple-children col-md"}]
    for t, object_label in TOP_LEVELS.items():
        o = ["a", {"href": href.format(curie=t, db=treename)}, object_label]
        if t == entity_type:
            if term_id == entity_type:
                hierarchies.append(hierarchy)
            else:
                hierarchies.append(["ul", ["li", o, hierarchy]])
            continue
        hierarchies.append(["ul", ["li", o]])
        i += 1
    return hierarchies


def tree(
    conn: Connection,
    treename: str,
    term_id: Optional[str],
    href: str = "?id={curie}",
    title: str = None,
    predicate_ids: list = None,
    include_search: bool = False,
    standalone: bool = True,
    max_children: int = 100,
    statements: str = "statements",
) -> str:
    """Create an HTML/RDFa tree for the given term.
    If term_id is None, create the tree for owl:Class."""
    # Get the prefixes
    results = conn.execute("SELECT * FROM prefix ORDER BY length(base) DESC")
    prefixes = {res["prefix"]: res["base"] for res in results}

    ps = set()
    body = []
    if not term_id:
        p, t = term2rdfa(
            conn,
            prefixes,
            treename,
            [],
            "owl:Class",
            [],
            title=title,
            href=href,
            max_children=max_children,
            statements=statements,
        )
        ps.update(p)
        body.append(t)

    # Maybe find a * in the IDs that represents all remaining predicates
    predicate_ids_split = None
    if predicate_ids and "*" in predicate_ids:
        before = []
        after = []
        found = False
        for pred in predicate_ids:
            if pred == "*":
                found = True
                continue
            if not found:
                before.append(pred)
            else:
                after.append(pred)
        predicate_ids_split = [before, after]

    # Run for given terms if terms have not yet been filled out
    if not body:
        if predicate_ids and predicate_ids_split:
            # If some IDs were provided with *, add the remaining predicates
            # These properties go in between the before & after defined in the split
            rem_predicate_ids = get_sorted_predicates(
                conn, exclude_ids=predicate_ids, statements=statements
            )

            # Separate before & after with the remaining properties
            predicate_ids = predicate_ids_split[0]
            predicate_ids.extend(rem_predicate_ids)
            predicate_ids.extend(predicate_ids_split[1])
        elif not predicate_ids:
            predicate_ids = get_sorted_predicates(conn, statements=statements)

        # Get the stanza (every row about this term)
        query = sql_text(f'SELECT * FROM "{statements}" WHERE subject = :term_id')
        results = conn.execute(query, term_id=term_id)
        stanza = []
        for res in results:
            stanza.append(dict(res))

        p, t = term2rdfa(
            conn,
            prefixes,
            treename,
            predicate_ids,
            term_id,
            stanza,
            title=title,
            href=href,
            max_children=max_children,
            statements=statements,
        )
        ps.update(p)
        body.append(t)

    if not title:
        title = treename + " Browser"

    # Create the prefix element
    pref_strs = []
    for prefix, base in prefixes.items():
        pref_strs.append(f"{prefix}: {base}")
    pref_str = "\n".join(pref_strs)

    body_wrapper = ["div", {"id": "gizmosTree", "prefix": pref_str}]
    if include_search:
        body_wrapper.append(
            [
                "div",
                {"class": "form-row mt-2 mb-2"},
                [
                    "input",
                    {
                        "id": f"statements-typeahead",
                        "class": "typeahead form-control",
                        "type": "text",
                        "value": "",
                        "placeholder": "Search",
                    },
                ],
            ]
        )
    body = body_wrapper + body

    # JQuery
    if standalone:
        body.append(
            [
                "script",
                {
                    "src": "https://code.jquery.com/jquery-3.5.1.min.js",
                    "integrity": "sha256-9/aliU8dGd2tb6OSsuzixeV4y/faTqgFtohetphbbj0=",
                    "crossorigin": "anonymous",
                },
            ]
        )

        if include_search:
            # Add JS imports for running search
            body.append(["script", {"type": "text/javascript", "src": popper_js}])
            body.append(["script", {"type": "text/javascript", "src": bootstrap_js}])
            body.append(["script", {"type": "text/javascript", "src": typeahead_js}])

        # Custom JS for show more children
        js = """function show_children() {
                hidden = $('#children li:hidden').slice(0, 100);
                if (hidden.length > 1) {
                    hidden.show();
                    setTimeout(show_children, 100);
                } else {
                    console.log("DONE");
                }
                $('#more').hide();
            }"""

        # Custom JS for search bar using Typeahead
        if include_search:
            # Built the href to return when you select a term
            href_split = href.split("{curie}")
            before = href_split[0].format(db=treename)
            after = href_split[1].format(db=treename)
            js_funct = f'str.push("{before}" + encodeURIComponent(obj[p]) + "{after}");'

            # Build the href to return names JSON
            remote = "'?text=%QUERY&format=json'"
            if "db=" in href:
                # Add tree name to query params
                remote = f"'?db={treename}&text=%QUERY&format=json'"
            js += (
                """
        $('#search-form').submit(function () {
            $(this)
                .find('input[name]')
                .filter(function () {
                    return !this.value;
                })
                .prop('name', '');
        });
        function jump(currentPage) {
          newPage = prompt("Jump to page", currentPage);
          if (newPage) {
            href = window.location.href.replace("page="+currentPage, "page="+newPage);
            window.location.href = href;
          }
        }
        function configure_typeahead(node) {
          if (!node.id || !node.id.endsWith("-typeahead")) {
            return;
          }
          table = node.id.replace("-typeahead", "");
          var bloodhound = new Bloodhound({
            datumTokenizer: Bloodhound.tokenizers.obj.nonword('short_label', 'label', 'synonym'),
            queryTokenizer: Bloodhound.tokenizers.nonword,
            sorter: function(a, b) {
              return a.order - b.order;
            },
            remote: {
              url: """
                + remote
                + """,
              wildcard: '%QUERY',
              transform : function(response) {
                  return bloodhound.sorter(response);
              }
            }
          });
          $(node).typeahead({
            minLength: 0,
            hint: false,
            highlight: true
          }, {
            name: table,
            source: bloodhound,
            display: function(item) {
              if (item.label && item.short_label && item.synonym) {
                return item.short_label + ' - ' + item.label + ' - ' + item.synonym;
              } else if (item.label && item.short_label) {
                return item.short_label + ' - ' + item.label;
              } else if (item.label && item.synonym) {
                return item.label + ' - ' + item.synonym;
              } else if (item.short_label && item.synonym) {
                return item.short_label + ' - ' + item.synonym;
              } else if (item.short_label && !item.label) {
                return item.short_label;
              } else {
                return item.label;
              }
            },
            limit: 40
          });
          $(node).bind('click', function(e) {
            $(node).select();
          });
          $(node).bind('typeahead:select', function(ev, suggestion) {
            $(node).prev().val(suggestion.id);
            go(table, suggestion.id);
          });
          $(node).bind('keypress',function(e) {
            if(e.which == 13) {
              go(table, $('#' + table + '-hidden').val());
            }
          });
        }
        $('.typeahead').each(function() { configure_typeahead(this); });
        function go(table, value) {
          q = {}
          table = table.replace('_all', '');
          q[table] = value
          window.location = query(q);
        }
        function query(obj) {
          var str = [];
          for (var p in obj)
            if (obj.hasOwnProperty(p)) {
              """
                + js_funct
                + """
            }
          return str.join("&");
        }"""
            )

        body.append(["script", {"type": "text/javascript"}, js])

        # HTML Headers & CSS
        head = [
            "head",
            ["meta", {"charset": "utf-8"}],
            [
                "meta",
                {
                    "name": "viewport",
                    "content": "width=device-width, initial-scale=1, shrink-to-fit=no",
                },
            ],
            ["link", {"rel": "stylesheet", "href": bootstrap_css, "crossorigin": "anonymous"}],
            ["link", {"rel": "stylesheet", "href": "../style.css"}],
            ["title", title],
            [
                "style",
                """
        #annotations {
          padding-left: 1em;
          list-style-type: none !important;
        }
        #annotations ul {
          padding-left: 3em;
          list-style-type: circle !important;
        }
        #annotations ul ul {
          padding-left: 2em;
          list-style-type: none !important;
        }
        .hierarchy {
          padding-left: 0em;
          list-style-type: none !important;
        }
        .hierarchy ul {
          padding-left: 1em;
          list-style-type: none !important;
        }
        .hierarchy ul.multiple-children > li > ul {
          border-left: 1px dotted #ddd;
        }
        .hierarchy .children {
          border-left: none;
          margin-left: 2em;
          text-indent: -1em;
        }
        .hierarchy .children li::before {
          content: "\2022";
          color: #ddd;
          display: inline-block;
          width: 0em;
          margin-left: -1em;
        }
        .tt-dataset {
          max-height: 300px;
          overflow-y: scroll;
        }
        span.twitter-typeahead .tt-menu {
          cursor: pointer;
        }
        .dropdown-menu, span.twitter-typeahead .tt-menu {
          position: absolute;
          top: 100%;
          left: 0;
          z-index: 1000;
          display: none;
          float: left;
          min-width: 160px;
          padding: 5px 0;
          margin: 2px 0 0;
          font-size: 1rem;
          color: #373a3c;
          text-align: left;
          list-style: none;
          background-color: #fff;
          background-clip: padding-box;
          border: 1px solid rgba(0, 0, 0, 0.15);
          border-radius: 0.25rem; }
        span.twitter-typeahead .tt-suggestion {
          display: block;
          width: 100%;
          padding: 3px 20px;
          clear: both;
          font-weight: normal;
          line-height: 1.5;
          color: #373a3c;
          text-align: inherit;
          white-space: nowrap;
          background: none;
          border: 0; }
        span.twitter-typeahead .tt-suggestion:focus,
        .dropdown-item:hover,
        span.twitter-typeahead .tt-suggestion:hover {
            color: #2b2d2f;
            text-decoration: none;
            background-color: #f5f5f5; }
        span.twitter-typeahead .active.tt-suggestion,
        span.twitter-typeahead .tt-suggestion.tt-cursor,
        span.twitter-typeahead .active.tt-suggestion:focus,
        span.twitter-typeahead .tt-suggestion.tt-cursor:focus,
        span.twitter-typeahead .active.tt-suggestion:hover,
        span.twitter-typeahead .tt-suggestion.tt-cursor:hover {
            color: #fff;
            text-decoration: none;
            background-color: #0275d8;
            outline: 0; }
        span.twitter-typeahead .disabled.tt-suggestion,
        span.twitter-typeahead .disabled.tt-suggestion:focus,
        span.twitter-typeahead .disabled.tt-suggestion:hover {
            color: #818a91; }
        span.twitter-typeahead .disabled.tt-suggestion:focus,
        span.twitter-typeahead .disabled.tt-suggestion:hover {
            text-decoration: none;
            cursor: not-allowed;
            background-color: transparent;
            background-image: none;
            filter: "progid:DXImageTransform.Microsoft.gradient(enabled = false)"; }
        span.twitter-typeahead {
          width: 100%; }
          .input-group span.twitter-typeahead {
            display: block !important; }
            .input-group span.twitter-typeahead .tt-menu {
              top: 2.375rem !important; }""",
            ],
        ]
        body = ["body", {"class": "container"}, body]
        html = ["html", head, body]
    else:
        html = body
    return render([(k, v) for k, v in prefixes.items()], html, href=href, db=treename)


def tree_label(data: dict, treename: str, s: str) -> list:
    """Retrieve the hiccup-style vector label of a term."""
    node = data[treename][s]
    label = node.get("label", s)
    if s in data["obsolete"]:
        return ["s", label]
    return label
