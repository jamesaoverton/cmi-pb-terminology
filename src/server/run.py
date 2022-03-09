#!/usr/bin/env python3.9
import atexit
import csv
import io
import json
import logging
import os
import sqlite3
import traceback

from collections import defaultdict
from flask import abort, Flask, redirect, request, render_template, Response, url_for
from gizmos_export import export
from gizmos_helpers import get_descendants, get_entity_type, get_labels
from gizmos_search import get_search_results
from gizmos.helpers import get_children, get_ids  # These still work with LDTab
from gizmos_tree import tree
from lark import Lark, UnexpectedCharacters
from sprocket import (
    get_sql_columns,
    get_sql_tables,
    parse_order_by,
    render_database_table,
    render_html_table,
    render_tsv_table,
)
from sprocket.grammar import PARSER, SprocketTransformer
from sqlalchemy import create_engine
from sqlalchemy.sql.expression import text as sql_text
from werkzeug.datastructures import ImmutableMultiDict
from werkzeug.exceptions import HTTPException

from src.script.cmi_pb_grammar import grammar, TreeToDict
from src.script.load import configure_db, read_config_files, update_row
from src.script.validate import validate_row


BUILTIN_LABELS = {
    "rdfs:subClassOf": "parent class",
    "owl:equivalentClass": "equivalent class",
    "owl:disjointWith": "disjoint with",
    "rdfs:subPropertyOf": "parent property",
    "owl:equivalentProperty": "equivalent property",
    "owl:inverseOf": "inverse property",
    "rdfs:domain": "domain",
    "rdfs:range": "range",
    "rdf:type": "type of",
    "owl:sameAs": "same individual",
    "owl:differentFrom": "different individual",
}
FORM_ROW_ID = 0

app = Flask(__name__)

# Next line is required for running as CGI script - comment/uncomment as needed
os.chdir("../..")

# Set up logging to file
logger = logging.getLogger("droid_logger")
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler("app.log")
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)

# sqlite3 is required for executescript used in load
setup_conn = sqlite3.connect("build/cmi-pb.db", check_same_thread=False)
config = read_config_files("src/table.tsv", Lark(grammar, parser="lalr", transformer=TreeToDict()))
config["db"] = setup_conn
configure_db(config)

# SQLAlchemy connection required for sprocket/gizmos
abspath = os.path.abspath("build/cmi-pb.db")
db_url = "sqlite:///" + abspath + "?check_same_thread=False"
engine = create_engine(db_url)
conn = engine.connect()


@app.errorhandler(Exception)
def handle_exception(e):
    if isinstance(e, HTTPException):
        return e
    return (
        render_template(
            "template.html",
            tables=get_sql_tables(conn),
            html="<code>" + "<br>".join(traceback.format_exc().split("\n")),
        )
        + "</code>"
    )


@app.route("/")
def index():
    return render_template("template.html", html="<h3>Welcome</h3><p>Please select a table</p>")


# @app.route("/column")
# def column():
#    # TODO: how should this be displayed
#    table_name = request.args.get("table")
#    if not table_name:
#        return abort(406, "A 'table' is required")
#    return get_sql_columns(conn, table_name)


@app.route("/<table_name>", methods=["GET", "POST"])
def table(table_name):
    messages = defaultdict(list)
    view = request.args.get("view")
    if view == "tree":
        # Will throw an error if non-ontology table
        return render_tree(table_name)

    # Check for subclass of searches - these are automatically term table views
    subclass_of = request.args.get("subClassOf")
    subclass_of_self = request.args.get("subClassOf?")
    descendants_of = request.args.get("subClassOfplus")  # TODO: does not work as param
    descendants_of_self = request.args.get("subClassOf*")

    if subclass_of:
        return render_subclass_of(table_name, "subClassOf", subclass_of)
    elif subclass_of_self:
        return render_subclass_of(table_name, "subClassOf?", subclass_of_self)
    elif descendants_of:
        return render_subclass_of(table_name, "subClassOfplus", descendants_of)
    elif descendants_of_self:
        return render_subclass_of(table_name, "subClassOf*", descendants_of_self)

    # First check if table is an ontology table - if so, render term IDs + labels
    if is_ontology(table_name):
        # Get all terms from the ontology
        res = conn.execute(
            f"SELECT DISTINCT subject FROM {table_name} WHERE subject NOT LIKE '_:%'"
        )
        terms = [x["subject"] for x in res]
        data = get_search_results(
            conn,
            request.args.get("text", ""),
            limit=None,
            statements=table_name,
            synonyms=["IAO:0000118", "CMI-PB:alternativeTerm"],
            terms=terms,
        )
        if request.args.get("format") == "json":
            # Support for typeahead search
            return json.dumps(data)
        return render_ontology_table(table_name, data, "Showing terms from " + table_name)

    # Otherwise render default sprocket table
    html = render_database_table(
        conn,
        table_name,
        display_messages=messages,
        hide_in_row=["row_number"],
        show_help=True,
        standalone=False,
        use_view=True
    )
    tables = [x for x in get_sql_tables(conn) if not x.startswith("tmp_")]
    return render_template("template.html", html=html, table_name=table_name, tables=tables)


@app.route("/<table_name>/<term_id>", methods=["GET", "POST"])
def term(table_name, term_id):
    messages = {}
    if not is_ontology(table_name):
        row = None
        status = None
        row_number = None
        try:
            row_number = int(term_id)
        except ValueError:
            if term_id == "new" and request.args.get("view") != "form":
                return abort(400, f"Term ID ({term_id}) for data table must be an integer")

        view = request.args.get("view")

        if request.method == "POST":
            # Get the row from the form and remove the hidden param
            new_row = dict(request.form)
            del new_row["action"]

            # Manually override view, which is not included in request.args in CGI app
            view = "form"

            if request.form["action"] == "validate":
                validated_row = validate_table_row(table_name, row_number, new_row)
                if row_number:
                    # Place row_number first
                    validated_row_2 = {"row_number": row_number}
                    validated_row_2.update(validated_row)
                    validated_row = validated_row_2
                row = get_form_row(table_name, validated_row)
                status = "valid"
                if "error" in get_messages(validated_row):
                    status = "invalid"
            else:
                # Update or add new row
                if term_id != "new":
                    row_number = int(term_id)
                    # First validate the row to get the meta columns
                    validated_row = validate_table_row(table_name, row_number, new_row)
                    # Update the row regardless of results
                    # Row ID may be different than row number, if exists
                    update_row(config, table_name, validated_row, row_number)
                    messages = get_messages(validated_row)
                    if messages.get("error"):
                        warn = messages.get("warn", [])
                        warn.append(f"Row updated with {len(messages['error'])} errors")
                        messages["warn"] = warn
                    else:
                        messages["success"] = ["Row successfully updated!"]
                else:
                    # TODO: add new row
                    pass

        # Treat term ID as row ID
        try:
            row_number = int(term_id)
        except ValueError:
            if term_id != "new":
                return abort(
                    418, f"ID ({term_id}) must be an integer (row ID) for non-ontology tables"
                )
        tables = [x for x in get_sql_tables(conn) if not x.startswith("tmp_")]

        if view == "form":
            logger.info(f"Generating form view for row {row_number} from {table_name}")
            if not row and row_number != "new":
                # Get the row
                res = dict(
                    conn.execute(
                        f"SELECT * FROM {table_name}_view WHERE row_number = {row_number}"
                    ).fetchone()
                )
                row = get_form_row(table_name, res)
            elif not row:
                # Empty row
                cols = get_sql_columns(conn, table_name)
                row = {}
                if "column" in tables:
                    for c in cols:
                        if c.endswith("_meta") or c == "row_number":
                            continue
                        res = conn.execute(
                            sql_text(
                                """SELECT description FROM "column"
                                WHERE "table" = :table AND "column" = :column"""
                            ),
                            table=table_name,
                            column=c,
                        ).fetchone()
                        if res:
                            row[c] = {"description": res["description"]}
                        else:
                            row[c] = {}
                else:
                    row = {c: {} for c in cols if not c.endswith("_meta") and not c == "row_number"}
            return render_template(
                "data_form.html",
                include_back=True,
                messages=messages,
                row=row,
                status=status,
                table_name=table_name,
                tables=tables,
            )

        # Set the request.args to be in the format sprocket expects (like swagger)
        request_args = request.args.to_dict()
        request_args["offset"] = str(row_number - 1)
        request_args["limit"] = "1"
        request.args = ImmutableMultiDict(request_args)
        html = render_database_table(
            conn, table_name, show_help=True, show_options=False, standalone=False, use_view=True
        )
        return render_template(
            "template.html", html=html, include_back=True, table_name=table_name, tables=tables
        )

    # Redirect to main ontology table search, do not limit search results
    search_text = request.args.get("text")
    if search_text:
        return redirect(url_for("table", table_name=table_name, text=search_text))

    view = request.args.get("view")
    if view == "form":
        if request.method == "POST":
            term_id = update_term(table_name, term_id)
        # editable form that updates database
        return render_term_form(table_name, term_id)
    elif view == "transposed":
        # TODO: transpose columns and rows (isn't this vertical view?)
        pass
    elif view == "tree":
        return render_tree(table_name, term_id=term_id)
    elif request.args.get("format") == "json":
        return dump_search_results(table_name)
    else:
        # TODO: is this the same as transposed? Should this not be default?
        select = request.args.get("select")
        predicates = None
        if select:
            # TODO: implement form selector?
            pred_labels = select.split(",")
            predicates = get_ids(conn, pred_labels)
        data = get_data_for_term(table_name, term_id, predicates=predicates)
        fmt = request.args.get("format")
        if fmt:
            if fmt == "tsv":
                mt = "text/tab-separated-values"
            elif fmt == "csv":
                mt = "text/comma-separated-values"
            else:
                return abort(400, "Unknown format: " + fmt)
            return Response(render_tsv_table([data], fmt=fmt), mimetype=mt)
        base_url = url_for("term", table_name=table_name, term_id=term_id)
        tree_url = url_for("term", table_name=table_name, term_id=term_id, view="tree")
        html = [
            '<div class="row justify-content-end"><div class="col-auto"><div class="btn-group">',
            f'<a href="{base_url}" class="btn btn-sm btn-outline-primary active">Table</a>',
            f'<a href="{tree_url}" class="btn btn-sm btn-outline-primary">Tree</a>',
            "</div></div></div>",
            render_html_table(
                [data],
                table_name,
                [],
                request.args,
                base_url=base_url,
                hidden=["search_text"],
                include_expand=False,
                show_options=False,
                standalone=False,
            ),
        ]
        tables = [x for x in get_sql_tables(conn) if not x.startswith("tmp_")]
        return render_template(
            "template.html",
            html="\n".join(html),
            show_search=is_ontology(table_name),
            table_name=table_name,
            tables=tables,
        )


# ----- DATA TABLE METHODS -----


def get_description(table_name, column):
    res = conn.execute(
        sql_text('SELECT description FROM "column" WHERE "table" = :table AND "column" = :column'),
        table=table_name,
        column=column,
    ).fetchone()
    if res:
        return res["description"]
    return None


def get_form_row(table_name, row):
    """Transform a row either from query results or validation into a row suitable for the Jinja
    template. This is a dictionary of header -> details (value, valid, messages)."""
    form_row = {}
    for header, value in row.items():
        if header.endswith("_meta"):
            continue
        if not value or isinstance(value, str) or isinstance(value, int):
            # This row is coming from a query result
            # Check for meta row
            meta_row = row.get(header + "_meta")
            if meta_row:
                meta = json.loads(meta_row)
                if meta.get("value"):
                    value = meta["value"]
            if not value:
                # If value is still None, we couldn't find nulltype or invalid value
                value = ""
            details = {"value": value}
        else:
            # This row is coming from a validated row
            details = {
                "value": value["value"],
                "valid": value["valid"],
                "message": "<br>".join({x["message"] for x in value["messages"]}),
            }
        if "column" in get_sql_tables(conn):
            desc = get_description(table_name, header)
            if desc:
                details["description"] = desc
        form_row[header] = details
    return form_row


def get_messages(row):
    """Extract messages from a validated row into a dictionary of messages."""
    messages = defaultdict(list)
    for header, details in row.items():
        if header == "row_number":
            continue
        if details["messages"]:
            for msg in details["messages"]:
                if msg["level"] == "error":
                    if "error" not in messages:
                        messages["error"] = list()
                    messages["error"].append(msg["message"])
                elif msg["level"] == "warn":
                    if "warn" not in messages:
                        messages["warn"] = list()
                    messages["warn"].append(msg["message"])
                elif msg["level"] == "info":
                    if "info" not in messages:
                        messages["info"] = list()
                    messages["info"].append(msg["message"])
    return messages


def validate_table_row(table_name, row_number, row):
    """Perform validation on a row"""
    # Transform row into dict expected for validate
    result_row = {}
    for column, value in row.items():
        result_row[column] = {
            "value": value,
            "valid": True,
            "messages": [],
        }
    if row_number != "new":
        # Row number may be different than row ID, if this column is used
        return validate_row(config, table_name, result_row, row_number=row_number)
    return validate_row(config, table_name, result_row, existing_row=False)


# ----- ONTOLOGY TABLE METHODS -----


def build_form_field(
    table_name, input_type, header, predicate, help_msg, required, annotations=None, value=None, allow_delete=True
):
    global FORM_ROW_ID
    """Return an HTML form field for a template field."""
    if required:
        display = header + " *"
    else:
        display = header

    html = [f'<div class="row mb-3" id="{FORM_ROW_ID}">', '<label class="col-sm-3 col-form-label">']
    if allow_delete:
        html.extend(
            [
                f"<a href=\"javascript:del('{FORM_ROW_ID}')\">",
                f'<i class="bi-x-circle" style="font-size: 16px; color: #dc3545;"></i>' "</a>",
                "&nbsp;",
            ]
        )

    html.extend([display, "</label>", '<div class="col-sm-9">'])
    FORM_ROW_ID += 1

    value_html = ""
    if value:
        value = value.replace('"', "&quot;")
        value_html = f' value="{value}"'
    if not value:
        value = ""

    if input_type == "text":
        if required:
            html.append(
                f'<input type="text" class="form-control" name="{predicate}" required{value_html}>'
            )
            html.append('<div class="invalid-feedback">')
            html.append(f"{header} is required")
            html.append("</div>")
        else:
            html.append(f'<input type="text" class="form-control" name="{predicate}"{value_html}>')

    elif input_type == "textarea":
        if required:
            html.extend(
                [
                    f'<textarea class="form-control" name="{predicate}" rows="3" required>',
                    value,
                    "</textarea>",
                    '<div class="invalid-feedback">',
                    f"{header} is required",
                    "</div>",
                ]
            )
        else:
            html.append(
                f'<textarea class="form-control" name="{predicate}" rows="3">{value}</textarea>'
            )

    elif input_type == "search":
        if required:
            html.append(
                f'<input type="text" class="search form-control" name="{predicate}" '
                + f'id="{table_name}-typeahead-form" required{value_html}>'
            )
            html.append('<div class="invalid-feedback">')
            html.append(f"{header} is required")
            html.append("</div>")
        else:
            html.append(
                f'<input type="text" class="typeahead form-control" name="{predicate}" '
                + f'id="{table_name}-typeahead-form"{value_html}>'
            )

    elif input_type.startswith("select"):
        selects = input_type.split("(", 1)[1].rstrip(")").split(", ")
        html.append(f'<select class="form-select" name="{predicate}">')
        for s in selects:
            if value and s == value:
                html.append(f'<option value="{s}" selected>{s}</option>')
            else:
                html.append(f'<option value="{s}">{s}</option>')
        html.append("</select>")

    else:
        return None

    if help_msg:
        html.append(f'<div class="form-text">{help_msg}</div>')
    html.append("</div>")

    if annotations:
        # TODO: support input types for annotations
        for ann_pred, ann_values in annotations.items():
            for av in ann_values:
                # TODO: add delete button
                html.append(f'<label class="col-sm-3 col-form-label">{ann_pred}</label>')
                html.append('<div class="col-sm-9">')
                av = av.replace('"', "&quot;")
                html.append(f'<input type="text" value={av}></input>')
                html.append("</div>")
    html.append("</div>")
    return html


def dump_search_results(table_name, search_arg=None):
    search_text = request.args.get("text")
    if not search_text:
        return json.dumps([])
    if search_arg:
        terms = get_terms_from_arg(table_name, search_arg).keys()
    else:
        terms = []
    # return the raw search results to use in typeahead
    return json.dumps(
        get_search_results(
            conn,
            search_text,
            limit=None,
            statements=table_name,
            synonyms=["IAO:0000118", "CMI-PB:alternativeTerm"],
            terms=terms,
        )
    )


def get_annotation_properties(table_name):
    results = list(
        conn.execute(
            f"""SELECT DISTINCT s2.subject AS subject FROM "{table_name}" s1
            JOIN "{table_name}" s2 ON s1.predicate = s2.subject WHERE s1.object IS NOT NULL"""
        )
    )
    aps = {}
    for res in results:
        ap_id = res["subject"]
        # Skip some ontology-level APs, as well as label which we add later
        if (
            ap_id.startswith("dct:")
            or ap_id.startswith("foaf:")
            or ap_id.startswith("owl:")
            or ap_id.startswith("<")
            or ap_id == "rdfs:label"
        ):
            continue
        res = conn.execute(
            f"""SELECT object FROM {table_name} WHERE subject = ? AND predicate = 'rdfs:label'
            ORDER BY annotation""",
            (ap_id,),
        ).fetchone()
        if res:
            aps[ap_id] = res[0]
        else:
            aps[ap_id] = ap_id
    return aps


def get_data_for_term(table_name, term_id, predicates=None):
    # TODO: update to use LDTab
    prefixes = {}
    for row in conn.execute(f"SELECT DISTINCT prefix, base FROM prefix"):
        prefixes[row["prefix"]] = row["base"]

    # First get all data about this term, the "stanza"
    query = sql_text(f"SELECT * FROM {table_name} WHERE subject = :term_id")
    stanza = conn.execute(query, term_id=term_id).fetchall()

    curies = set()
    predicate_to_vals = defaultdict(list)
    annotations = defaultdict(dict)
    for row in stanza:
        if row["subject"] == term_id:
            pred = row["predicate"]
            if predicates and pred not in predicates:
                continue
            curies.add(pred)
            if row["object"] and row["datatype"] == "_IRI":
                curies.add(row["object"])
            predicate_to_vals[pred].append(dict(row))
            if row["annotation"]:
                if pred not in annotations:
                    annotations[pred] = {}
                annotations[pred][row["object"]] = json.loads(row["annotation"])
                curies.update(annotations[pred][row["object"]] .keys())

    # Get the labels of any predicate or object used
    labels = get_labels(conn, curies, statements=table_name)

    data = {}
    for predicate, rows in predicate_to_vals.items():
        # TODO: use wiring.rs to render instead
        if predicate in labels:
            pred_label = labels[predicate]
        else:
            pred_label = predicate
        pred_annotations = annotations.get(predicate, {})
        vals = []
        for row in rows:
            o = row["object"]
            if row["datatype"] == "_IRI":
                label = labels.get(o, o)
                display = f'<a href="{url_for("term", table_name=table_name, term_id=o)}">{label}</a>'
            else:
                display = f"<span>{o}</span>"
            value_annotations = pred_annotations.get(o)
            if value_annotations:
                display += "<ul>"
                for pred_2, values in value_annotations.items():
                    display += "<li>"
                    display += f'<small><a href="{url_for("term", table_name=table_name, term_id=pred_2)}">{labels.get(pred_2, pred_2)}</a></small>'
                    display += "<ul>" + "".join([f"<li><small>{v['object']}</small></li>" for v in values]) + "</ul>"
                display += "</ul>"
            vals.append(display)

        if len(vals) > 1:
            data[pred_label] = "<ul><li>" + "<li>".join(vals) + "</ul>"
        else:
            data[pred_label] = vals[0]
    return data


def get_terms_from_arg(table_name, arg):
    try:
        parsed = PARSER.parse(arg)
        res = SprocketTransformer().transform(parsed)
        if res[0] != "in":
            raise ValueError(f"Operator must be 'in', not '{res[0]}'")
        parent_terms = res[1]
    except UnexpectedCharacters:
        parent_terms = [arg]
    # We don't know if we were passed ID or label, so get both for all terms
    return get_labels(conn, parent_terms, include_top=False, statements=table_name)


def is_ontology(table_name):
    columns = get_sql_columns(conn, table_name)
    return {"subject", "predicate", "object", "datatype", "annotation"}.issubset(set(columns))


def render_ontology_table(table_name, data, title, add_params=None):
    """
    :param table_name: name of SQL table that contains terms
    :param data: data to render
    :param title: title to display at the top of table
    :param add_params: additional query parameter-arg pairs to include in search URL for typeahead
    """
    if request.args.get("order"):
        order_by = parse_order_by(request.args["order"].lower())
        for ob in order_by:
            if ob["nulls"] == "first":
                data = sorted(data, key=lambda d: d[ob["key"]])
            else:
                data = sorted(data, key=lambda d: (d[ob["key"]] == "", d[ob["key"]]))
            if ob["order"] == "desc":
                data.reverse()

    # get the columns we want and add links
    data = [
        {
            "ID": f"<a href=\"{url_for('term', table_name=table_name, term_id=itm['id'])}\">{itm['id']}</a>",
            "Label": f"<a href=\"{url_for('term', table_name=table_name, term_id=itm['id'])}\">{itm['label']}</a>",
            "Synonym": itm["synonym"] or "",
        }
        for itm in data
    ]

    param_str = ""
    if add_params:
        param_str = "&".join([f"{x}={y}" for x, y in add_params.items()])

    tables = [x for x in get_sql_tables(conn) if not x.startswith("tmp_")]
    return render_template(
        "template.html",
        html=render_html_table(
            data,
            table_name,
            [],
            request.args,
            base_url=url_for("table", table_name=table_name),
            hidden=["search_text"],
            show_options=False,
            include_expand=False,
            standalone=False,
        ),
        title=title,
        add_params=param_str,
        show_search=True,
        table_name=table_name,
        tables=tables,
    )


def render_subclass_of(table_name, param, arg):
    id_to_label = get_terms_from_arg(table_name, arg)
    hrefs = [
        f"<a href='/{url_for('term', table_name=table_name, term_id=term_id)}'>{label}</a>"
        for term_id, label in id_to_label.items()
    ]
    title = "Showing children of " + ", ".join(hrefs)

    terms = set()
    if param == "subClassOf":
        for p in id_to_label.keys():
            terms.update(get_children(conn, p, statements=table_name))
    elif param == "subClassOf?":
        terms.update(id_to_label.keys())
        for p in id_to_label.keys():
            terms.update(get_children(conn, p, statements=table_name))
    elif param == "subClassOfplus":
        for p in id_to_label.keys():
            terms.update(get_descendants(conn, p, statements=table_name))
    elif param == "subClassOf*":
        terms.update(id_to_label.keys())
        for p in id_to_label.keys():
            terms.update(get_descendants(conn, p, statements=table_name))
    else:
        # TODO: error message
        abort(400)

    data = get_search_results(
        conn,
        limit=None,
        search_text=request.args.get("text", ""),
        statements=table_name,
        terms=list(terms),
    )
    if request.args.get("format") == "json":
        # Support for searching the subset of these terms
        return json.dumps(data)
    # Show all terms
    return render_ontology_table(table_name, data, title, add_params={param: arg})


def render_term_form(table_name, term_id):
    global FORM_ROW_ID
    # Get the term label
    labels = get_labels(conn, [term_id], statements=table_name)
    label = labels.get(term_id, term_id)
    entity_type = get_entity_type(conn, term_id, statements=table_name)

    # Get all annotation properties used in the ontology
    aps = get_annotation_properties(table_name)
    if "rdfs:label" in aps:
        del aps["rdfs:label"]

    # Export annotations
    # TODO: error handling
    annotations = export(
        conn,
        [term_id],
        sorted(aps.keys()),
        "tsv",
        default_value_format="label",
        statements=table_name,
    )
    reader = csv.reader(io.StringIO(annotations), delimiter="\t")
    ann_headers = next(reader)
    ann_details = next(reader)
    logger.error(json.dumps(ann_details, indent=2))

    # Build the metadata form elements
    metadata_html = []
    # Add the ontology ID element
    field = build_form_field(
        table_name, "text", "ontology ID", "ID", None, True, value=term_id, allow_delete=False
    )
    if field:
        metadata_html.extend(field)
    # Add the label element
    field = build_form_field(
        table_name, "text", "label", "rdfs:label", None, True, value=label, allow_delete=False
    )
    if field:
        metadata_html.extend(field)
    # Add the rest of the annotations
    i = 0
    while i < len(ann_headers):
        header = ann_headers[i]
        detail = ann_details[i]
        i += 1
        if detail == "":
            # This annotation doesn't exist for this term
            continue

        ap_label = aps[header]
        input_type = "text"
        if ap_label in ["definition", "comment"]:
            input_type = "textarea"

        # Handle multi-value annotations (separated by pipes)
        # TODO: this can cause issues if there's a pipe in the annotation
        if "|" in detail:
            vals = detail.split("|")
        else:
            vals = [detail]
        for v in vals:
            field = build_form_field(table_name, input_type, ap_label, header, None, False, value=v)
            if not field:
                logger.warning("Could not build field for property: " + ap_label)
                continue
            metadata_html.extend(field)

    # Export logic
    if entity_type == "owl:Class":
        predicates = ["rdfs:subClassOf", "owl:equivalentClass", "owl:disjointWith"]
    elif entity_type == "owl:ObjectProperty" or entity_type == "owl:DatatypeProperty":
        predicates = [
            "rdfs:subPropertyOf",
            "owl:equivalentProperty",
            "owl:inverseOf",
            "rdfs:domain",
            "rdfs:range",
        ]
    elif entity_type == "owl:Individual":
        predicates = ["rdf:type", "owl:sameAs", "owl:differentFrom"]
    else:
        # datatypes & annotation properties
        predicates = None
    if predicates:
        logic = export(
            conn, [term_id], predicates, "tsv", default_value_format="label", statements=table_name,
        )
    else:
        logic = None

    logic_html = []
    if logic:
        # TODO: these are not including anon classes, wait for thick triples?
        reader = csv.DictReader(io.StringIO(logic), delimiter="\t")
        logic_details = next(reader)
        for header, value in logic_details.items():
            if value == "":
                continue
            predicate = BUILTIN_LABELS[header]
            field = build_form_field(
                table_name, "search", predicate, header, None, False, value=value
            )
            if not field:
                logger.warning("Could not build field for parent class")
            else:
                logic_html.extend(field)

    if label and " " in label:
        # Encase in single quotes when label has a space
        label = f"'{label}'"

    # Reset form row ID for next time
    FORM_ROW_ID = 0
    return render_template(
        "ontology_form.html",
        include_back=True,
        table_name=table_name,
        tables=get_sql_tables(conn),
        term_id=term_id,
        title=f"Update " + label or term_id,
        annotation_properties=aps,
        metadata="\n".join(metadata_html),
        logic="\n".join(logic_html),
        entity_type=entity_type,
    )


def render_tree(table_name, term_id: str = None):
    if not is_ontology(table_name):
        return abort(418, "Cannot show tree view for non-ontology table")

    search_text = request.args.get("text")
    if search_text:
        if term_id:
            terms = get_terms_from_arg(table_name, term_id).keys()
        else:
            terms = []
        # show a table of search results
        data = get_search_results(
            conn,
            search_text,
            terms=terms,
            limit=None,
            synonyms=["IAO:0000118", "CMI-PB:alternativeTerm"],
            statements=table_name,
        )
        title = f"Showing search results for '{search_text}'"
        return render_ontology_table(table_name, data, title)

    # nothing to search, just return the tree view
    html = ""
    if term_id:
        term_url = url_for("term", table_name=table_name, term_id=term_id)
        tree_url = url_for("term", table_name=table_name, term_id=term_id, view="tree")
        html += '<div class="row justify-content-end"><div class="col-auto"><div class="btn-group">'
        html += f'<a href="{term_url}" class="btn btn-sm btn-outline-primary">Table</a>'
        html += f'<a href="{tree_url}" class="btn btn-sm btn-outline-primary active">Tree</a>'
        html += "</div></div></div>"
    html += tree(
        conn,
        "ontie",
        term_id,
        href=url_for("term", table_name=table_name, term_id="{curie}", view="tree")
        .replace("%7B", "{")
        .replace("%7D", "}"),
        standalone=False,
        max_children=20,
        statements=table_name,
    )
    tables = [x for x in get_sql_tables(conn) if not x.startswith("tmp_")]
    return render_template(
        "template.html", html=html, show_search=True, table_name=table_name, tables=tables,
    )


def update_term(table_name, term_id):
    # TODO: new LDTab structure
    new_id = request.form.get("ID")
    if new_id != term_id:
        logger.info(f"Updating {term_id} to new ID {new_id}")
        query = sql_text(f"UPDATE {table_name} SET stanza = :new WHERE stanza = :old")
        conn.execute(query, new=new_id, old=term_id)
        query = sql_text(f"UPDATE {table_name} SET subject = :new WHERE subject = :old")
        conn.execute(query, new=new_id, old=term_id)
        query = sql_text(f"UPDATE {table_name} SET object = :new WHERE object = :old")
        conn.execute(query, new=new_id, old=term_id)
        term_id = new_id

    # Get current annotations for this term
    query = sql_text(
        f"SELECT predicate, object FROM {table_name} WHERE subject = :s AND object IS NOT NULL"
    )
    results = conn.execute(query, s=term_id)
    annotations = defaultdict(list)
    for res in results:
        if res["predicate"] not in annotations:
            annotations[res["predicate"]] = list()
        annotations[res["predicate"]].append(res["object"])

    # Get current logic for this term
    logic = defaultdict(list)
    query = sql_text(
        f"SELECT predicate, object FROM {table_name} WHERE subject = :s AND object IS NOT NULL"
    )
    results = conn.execute(query, s=term_id)
    for res in results:
        if res["predicate"] not in logic:
            logic[res["predicate"]] = list()
        logic[res["predicate"]].append(res["object"])

    # Get all annotation properties so we know where to put predicates
    aps = get_annotation_properties(table_name)

    form_annotations = defaultdict(list)
    form_logic = defaultdict(list)
    for predicate, value in request.form.items():
        if predicate == "ID":
            continue
        if predicate in aps or predicate == "rdfs:label":
            if predicate in annotations:
                # Predicate is already used on term, so it will be checked
                continue
            if predicate not in form_annotations:
                form_annotations[predicate] = list()
            form_annotations[predicate].append(value)
        else:
            if predicate in logic:
                # Predicate is already used on term, so it will be checked
                continue
            if predicate not in form_logic:
                form_logic[predicate] = list()
            form_logic[predicate].append(value)

    # Look for changes to existing annotation predicates on this term
    for predicate, values in annotations.items():
        new_values = request.form.getlist(predicate)
        removed_objs = set(values) - set(new_values)
        added_objs = set(new_values) - set(values)
        for rv in removed_objs:
            query = sql_text(
                f"DELETE FROM {table_name} WHERE subject = :s AND predicate = :p AND object = :v"
            )
            conn.execute(query, s=term_id, p=predicate, v=rv)
        for av in added_objs:
            query = sql_text(f"INSERT INTO {table_name} VALUES (:s, :s, :p, NULL, :v, NULL, NULL)")
            conn.execute(query, s=term_id, p=predicate, v=av)

    # Add new annotation predicates + values
    for predicate, values in form_annotations.items():
        for v in values:
            query = sql_text(f"INSERT INTO {table_name} VALUES (:s, :s, :p, NULL, :v, NULL, NULL)")
            conn.execute(query, s=term_id, p=predicate, v=v)

    # Look for changes to existing logic predicates on this term
    for predicate, objs in logic.items():
        if predicate == "rdf:type" and objs[0] in [
            "owl:Class",
            "owl:AnnotationProperty",
            "owl:DataProperty",
            "owl:ObjectProperty",
        ]:
            # Only look at type for individuals
            continue
        new_objects = request.form.getlist(predicate)
        new_obj_ids = get_ids(conn, new_objects)
        if len(new_objects) > len(new_obj_ids):
            # TODO: handle this case by getting IDs one by one?
            logger.error(
                "Cannot get IDs for one or more terms from term list: " + ", ".join(new_objects)
            )
        removed_objs = list(set(objs) - set(new_obj_ids))
        added_objs = list(set(new_obj_ids) - set(objs))
        if (
            predicate == "rdfs:subClassOf"
            and len(removed_objs) == 1
            and removed_objs[0] == "owl:Thing"
            and not added_objs
        ):
            # Do not delete owl:Thing, even though it doesn't show up on the form
            continue
        for rv in removed_objs:
            query = sql_text(
                f"""DELETE FROM {table_name}
                    WHERE subject = :s AND predicate = :p AND object = :v"""
            )
            conn.execute(query, s=term_id, p=predicate, v=rv)
        for av in added_objs:
            query = sql_text(f"INSERT INTO {table_name} VALUES (:s, :s, :p, :v, NULL, NULL, NULL)")
            conn.execute(query, s=term_id, p=predicate, v=av)

    # Add new logic predicates + objects
    for predicate, objects in form_logic.items():
        # All new predicates
        for o in objects:
            query = sql_text(f"INSERT INTO {table_name} VALUES (:s, :s, :p, :o, NULL, NULL, NULL)")
            conn.execute(query, s=term_id, p=predicate, o=o)

    return term_id


# This runs for each page in the CGI app, but for normal app it won't run until you shutdown app
@atexit.register
def clean_tables():
    tmp = [x for x in get_sql_tables(conn) if x.startswith("tmp_")]
    for t in tmp:
        logger.error(t)
        conn.execute(f'DROP TABLE "{t}"')


if __name__ == "__main__":
    # Next line is used to run as a Flask app - comment/uncomment as needed
    # app.run()
    # Next lines are required for running as CGI script - comment/uncomment as needed
    # The SCRIPT_NAME specifies the prefix to be used for url_for - currently hardcoded
    os.environ["SCRIPT_NAME"] = f"/CMI-PB/branches/next/views/src/server/run.py"
    from wsgiref.handlers import CGIHandler
    CGIHandler().run(app)

