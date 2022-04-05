import json
import logging
import os
import sqlite3
import traceback

from collections import defaultdict
from flask import (
    abort,
    Blueprint,
    Flask,
    redirect,
    request,
    render_template,
    Response,
    url_for,
)
from gadget.export import terms2dict, terms2tsv
from gadget.sql import (
    get_children,
    get_descendants,
    get_ids,
    get_labels,
    get_term_attributes,
    get_top_entity_type,
)
from gadget.tree import tree
from gadget.search import search
from hiccupy import insert_href, render
from html import escape as html_escape
from itertools import chain
from lark import Lark, UnexpectedCharacters
from logging import Logger
from sprocket import (
    get_sql_columns,
    get_sql_tables,
    parse_order_by,
    render_database_table,
    render_html_table,
    SprocketError,
)
from sprocket.grammar import PARSER, SprocketTransformer
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection
from sqlalchemy.sql.expression import bindparam
from sqlalchemy.sql.expression import text as sql_text
from typing import Optional, Tuple
from werkzeug.exceptions import HTTPException

try:
    from cmi_pb_script.cmi_pb_grammar import grammar, TreeToDict
    from cmi_pb_script.load import configure_db, insert_new_row, read_config_files, update_row
    from cmi_pb_script.validate import get_matching_values, validate_row
except ModuleNotFoundError:
    from src.cmi_pb_script.cmi_pb_grammar import grammar, TreeToDict
    from src.cmi_pb_script.load import configure_db, insert_new_row, read_config_files, update_row
    from src.cmi_pb_script.validate import get_matching_values, validate_row


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
LOGIC_PREDICATES = [
    "rdfs:subClassOf",
    "owl:equivalentClass",
    "owl:disjointWith",
    "rdfs:subPropertyOf",
    "rdf:type",
    "rdfs:domain",
    "rdfs:range",
]

BLUEPRINT = Blueprint(
    "cmi-pb",
    __name__,
    template_folder=os.path.abspath(os.path.join(os.path.dirname(__file__), "templates")),
)
CONFIG = None  # type: Optional[dict]
CONN = None  # type: Optional[Connection]
LOGGER = None  # type: Optional[Logger]
SYNONYMS = ["IAO:0000118"]


@BLUEPRINT.errorhandler(Exception)
def handle_exception(e):
    if isinstance(e, HTTPException):
        return e
    return (
        render_template(
            "template.html",
            tables=get_sql_tables(CONN),
            html="<code>" + "<br>".join(traceback.format_exc().split("\n")),
        )
        + "</code>"
    )


@BLUEPRINT.route("/")
def index():
    return render_template(
        "template.html",
        html="<h3>Welcome</h3><p>Please select a table</p>",
        tables=get_sql_tables(CONN),
    )


@BLUEPRINT.route("/<table_name>", methods=["GET", "POST"])
def table(table_name):
    messages = defaultdict(list)
    view = request.args.get("view")
    if view == "tree":
        # Will throw an error if non-ontology table
        return render_tree(table_name)

    # Check for subclass of searches - these are automatically term table views
    subclass_of = request.args.get("subClassOf")
    subclass_of_self = request.args.get("subClassOf?")
    descendants_of = request.args.get("subClassOfplus")  # '+' does not work in query params
    descendants_of_self = request.args.get("subClassOf*")

    if subclass_of:
        return render_subclass_of(table_name, "subClassOf", subclass_of)
    elif subclass_of_self:
        return render_subclass_of(table_name, "subClassOf?", subclass_of_self)
    elif descendants_of:
        return render_subclass_of(table_name, "subClassOfplus", descendants_of)
    elif descendants_of_self:
        return render_subclass_of(table_name, "subClassOf*", descendants_of_self)

    # TODO: view=form for tables to add new ontology term

    # First check if table is an ontology table - if so, render term IDs + labels
    if is_ontology(table_name):
        if request.args.get("format") == "json":
            # Support for typeahead search
            data = search(
                CONN, limit=30, search_text=request.args.get("text", ""), statement=table_name
            )
            return json.dumps(data)

        # Maybe get a set of predicates to restrict search results to
        select = request.args.get("select")
        predicates = ["rdfs:label", SYNONYMS[0]]
        if select:
            # TODO: add form at top of page for user to select predicates to show?
            pred_labels = select.split(",")
            predicates = get_ids(
                CONN, id_or_labels=pred_labels, id_type="predicate", statement=table_name
            )

        # Export the data - excluding anon objects
        data = get_term_attributes(
            CONN, exclude_json=True, predicates=predicates, statement=table_name
        )
        response = render_ontology_table(table_name, data, predicates=predicates)
        if isinstance(response, Response):
            return response
        return render_template(
            "template.html",
            html=response,
            title="Showing terms from " + table_name,
            subtitle=f'<a href="{url_for("cmi-pb.table", table_name=table_name, view="tree")}">View as tree</a>',
            show_search=True,
            table_name=table_name,
            tables=get_sql_tables(CONN),
        )

    # Typeahead for autocomplete in data forms
    if request.args.get("format") == "json":
        return json.dumps(
            get_matching_values(
                CONFIG,
                table_name,
                request.args.get("column"),
                matching_string=request.args.get("text"),
            )
        )

    form_html = None
    if request.method == "POST":
        # Override view, which isn't passed in POST
        view = "form"
        new_row = dict(request.form)
        del new_row["action"]
        validated_row = validate_table_row(table_name, new_row)
        if request.form["action"] == "validate":
            form_html = get_row_as_form(table_name, validated_row)
        elif request.form["action"] == "submit":
            row_number = insert_new_row(CONFIG, table_name, validated_row)
            return redirect(url_for("cmi-pb.term", table_name=table_name, term_id=row_number))

    if view == "form":
        if not form_html:
            row = {c: None for c in get_sql_columns(CONN, table_name) if c != "row_number"}
            form_html = get_row_as_form(table_name, row)
        return render_template(
            "data_form.html",
            include_back=True,
            messages=messages,
            row_form=form_html,
            table_name=table_name,
            tables=get_sql_tables(CONN),
        )

    # Otherwise render default sprocket table
    pk = get_primary_key(table_name)
    if pk != "row_number":
        ignore_cols = ["row_number"]
    else:
        ignore_cols = None
    try:
        response = render_database_table(
            CONN,
            table_name,
            request.args,
            display_messages=messages,
            ignore_cols=ignore_cols,
            ignore_params=["project-name", "branch-name", "view-path"],
            primary_key=pk,
            show_help=True,
            standalone=False,
            use_view=True,
        )
    except SprocketError as e:
        return abort(422, str(e))
    if isinstance(response, Response):
        return response
    return render_template(
        "template.html", html=response, table_name=table_name, tables=get_sql_tables(CONN)
    )


@BLUEPRINT.route("/<table_name>/<term_id>", methods=["GET", "POST"])
def term(table_name, term_id):
    messages = {}
    if not is_ontology(table_name):
        # Get row number based on PK
        row_number = get_row_number(table_name, term_id)
        if not row_number:
            return abort(
                500, f"'{term_id}' is not a valid primary key value for table '{table_name}'"
            )
        view = request.args.get("view")

        form_html = None
        if request.method == "POST":
            # Get the row from the form and remove the hidden param
            new_row = dict(request.form)
            del new_row["action"]

            # Manually override view, which is not included in request.args in CGI app
            view = "form"

            if request.form["action"] == "validate":
                validated_row = validate_table_row(table_name, new_row, row_number=row_number)
                # Place row_number first
                validated_row_2 = {"row_number": row_number}
                validated_row_2.update(validated_row)
                validated_row = validated_row_2
                form_html = get_row_as_form(table_name, validated_row)
            elif request.form["action"] == "submit":
                # First validate the row to get the meta columns
                validated_row = validate_table_row(table_name, new_row, row_number=row_number)
                # Update the row regardless of results
                # Row ID may be different than row number, if exists
                update_row(CONFIG, table_name, validated_row, row_number)
                messages = get_messages(validated_row)
                if messages.get("error"):
                    warn = messages.get("warn", [])
                    warn.append(f"Row updated with {len(messages['error'])} errors")
                    messages["warn"] = warn
                else:
                    messages["success"] = ["Row successfully updated!"]

        if view == "form":
            if not form_html:
                # Get the row
                res = dict(
                    CONN.execute(
                        f"SELECT * FROM {table_name}_view WHERE row_number = {row_number}"
                    ).fetchone()
                )
                form_html = get_row_as_form(table_name, res)

            if not form_html:
                return abort(500, "something went wrong - unable to render form")

            return render_template(
                "data_form.html",
                include_back=True,
                messages=messages,
                row_form=form_html,
                table_name=table_name,
                tables=get_sql_tables(CONN),
            )

        # Set the request.args to be in the format sprocket expects (like swagger)
        request_args = request.args.to_dict()
        request_args["offset"] = str(row_number - 1)
        request_args["limit"] = "1"

        pk = get_primary_key(table_name)
        if pk != "row_number":
            ignore_cols = ["row_number"]
        else:
            ignore_cols = None
        try:
            response = render_database_table(
                CONN,
                table_name,
                request_args,
                ignore_cols=ignore_cols,
                ignore_params=["project-name", "branch-name", "view-path"],
                primary_key=pk,
                show_help=True,
                show_options=False,
                standalone=False,
                use_view=True,
            )
        except SprocketError as e:
            return abort(422, str(e))
        if isinstance(response, Response):
            return response
        return render_template(
            "template.html",
            html=response,
            include_back=True,
            table_name=table_name,
            tables=get_sql_tables(CONN),
        )

    # Redirect to main ontology table search, do not limit search results
    search_text = request.args.get("text")
    if search_text:
        return redirect(url_for("cmi-pb.table", table_name=table_name, text=search_text))

    view = request.args.get("view")
    if view == "form":
        if request.method == "POST":
            return abort(501, "POST method for ontology term is not implemented")
            # term_id = update_term(table_name, pk)
        # editable form that updates database
        return render_term_form(table_name, term_id)
    elif view == "tree":
        return render_tree(table_name, term_id=term_id)
    elif request.args.get("format") == "json":
        return dump_search_results(table_name)
    else:
        select = request.args.get("select")
        predicates = None
        if select:
            # TODO: add form at top of page for user to select predicates to show?
            pred_labels = select.split(",")
            predicates = get_ids(
                CONN, id_or_labels=pred_labels, id_type="predicate", statement=table_name
            )

        data = get_term_attributes(
            CONN,
            include_all_predicates=False,
            predicates=predicates,
            statement=table_name,
            term_ids=[term_id],
        )

        response = render_ontology_table(table_name, data, predicates=predicates)
        if isinstance(response, Response):
            return response
        base_url = url_for("cmi-pb.term", table_name=table_name, term_id=term_id)
        tree_url = url_for("cmi-pb.term", table_name=table_name, term_id=term_id, view="tree")
        html = (
            render(
                [
                    "div",
                    {"class": "row justify-content-end"},
                    [
                        "div",
                        {"class": "col-auto"},
                        [
                            "div",
                            {"class": "btn-group"},
                            [
                                "a",
                                {
                                    "href": base_url,
                                    "class": "btn btn-sm btn-outline-primary active",
                                },
                                "Table",
                            ],
                            [
                                "a",
                                {"href": tree_url, "class": "btn btn-sm btn-outline-primary"},
                                "Tree",
                            ],
                        ],
                    ],
                ],
            )
            + response
        )
        return render_template(
            "template.html",
            html=html,
            show_search=is_ontology(table_name),
            table_name=table_name,
            tables=get_sql_tables(CONN),
        )


def flatten(lst):
    for el in lst:
        if isinstance(el, list) and not isinstance(el, (str, bytes)):
            yield from flatten(el)
        else:
            yield el


# ----- DATA TABLE METHODS -----


def get_hiccup_form_row(
    header,
    allow_delete=False,
    allowed_values: Optional[list] = None,
    annotations=None,
    description=None,
    display_header=None,
    html_type="text",
    message=None,
    readonly=False,
    valid=None,
    value=None,
):
    # TODO: support other HTML types: dropdown, boolean, etc...
    # TODO: handle datatypes for ontology forms (include_datatypes?)
    global FORM_ROW_ID

    if html_type in ["select", "radio", "checkbox"] and not allowed_values:
        # TODO: error handling - allowed_values should always be included for these
        raise Exception(f"A list of allowed values is required for HTML type '{html_type}'")

    # Create the header label for this form row
    header_col = ["div", {"class": "col-md-3", "id": FORM_ROW_ID}]
    if allow_delete:
        header_col.append(
            [
                "a",
                {"href": f"javascript:del({FORM_ROW_ID})"},
                ["i", {"class": "bi-x-circle", "style": "font-size: 16px; color: #dc3545;"}],
                "&nbsp",
            ]
        )
    FORM_ROW_ID += 1
    if display_header:
        header_col.append(["b", display_header])
    else:
        header_col.append(["b", header])
    if description:
        # Add a ? tooltip that displays description on hover
        header_col.append(
            [
                "button",
                {
                    "class": "btn",
                    "data-bs-toggle": "tooltip",
                    "data-bs-placement": "right",
                    "title": description,
                },
                ["i", {"class": "bi-question-circle"}],
            ]
        )

    # Create the value input for this form row
    classes = []
    if valid:
        classes.append("is-valid")
    elif valid is not None:
        classes.append("is-invalid")

    input_attrs = {}
    if readonly:
        # No name so that it isn't passed through POST data
        input_attrs["readonly"] = True
    else:
        # Everything else uses the header as the element name
        input_attrs["name"] = header

    value_col = ["div", {"class": "col-md-9"}]
    if html_type == "textarea":
        classes.insert(0, "form-control")
        input_attrs["class"] = " ".join(classes)
        textarea_element = ["textarea", input_attrs]
        if value:
            textarea_element.append(html_escape(value))
        value_col.append(textarea_element)

    elif html_type == "select":
        # TODO: what if value is not in allowed_values?
        classes.insert(0, "form-select")
        input_attrs["class"] = " ".join(classes)
        select_element = ["select", input_attrs]
        has_selected = False
        for av in allowed_values:
            av_safe = html_escape(str(av))
            if value and str(av) == str(value):
                has_selected = True
                select_element.append(
                    ["option", {"value": html_escape(av_safe), "selected": True}, av_safe]
                )
            else:
                select_element.append(["option", {"value": html_escape(av_safe)}, av_safe])
        # Add an empty string for no value at the start of the options
        if has_selected:
            select_element.insert(2, ["option", {"value": ""}])
        else:
            # If there is currently no value, make sure this one is selected
            select_element.insert(2, ["option", {"value": "", "selected": True}])
        value_col.append(select_element)

    elif html_type in ["text", "number", "search"]:
        # TODO: support a range restriction for 'number'
        classes.insert(0, "form-control")
        input_attrs["type"] = html_type
        if html_type == "search":
            classes.extend(["search", "typeahead"])
            input_attrs["id"] = f"{header}-typeahead-form"
        input_attrs["class"] = " ".join(classes)
        if value:
            input_attrs["value"] = html_escape(str(value))
        value_col.append(["input", input_attrs])

    elif html_type == "radio":
        # TODO: what if value is not in allowed_values? Or what if there is no value?
        classes.insert(0, "form-check")
        input_attrs["type"] = html_type
        for av in allowed_values:
            av_safe = html_escape(str(av))
            attrs_copy = input_attrs.copy()
            attrs_copy["value"] = av_safe
            if value and str(av) == str(value):
                attrs_copy["checked"] = True
            value_col.append(["div", ["input", attrs_copy], ["label", {"for": av_safe}, av_safe]])

    else:
        raise abort(500, f"'{html_type}' form field is not supported.")

    if message:
        cls = "invalid-feedback"
        if valid:
            cls = "valid-feedback"
        value_col.append(["div", {"class": cls}, message])
    if annotations:
        # TODO: support input types for annotations - text, textarea, search...
        ann_html = []
        for ann_pred, ann_values in annotations.items():
            for av in ann_values:
                # TODO: add delete button FORM_ROW_ID-annotation-X
                av = av["object"]
                ann_html = [
                    "div",
                    {
                        "class": "row justify-content-end",
                        "style": "padding-right: 0px; padding-top: 5px;",
                    },
                    [
                        "div",
                        {"class": "col-sm-9"},
                        [
                            "div",
                            {"class": "row"},
                            [
                                "label",
                                {
                                    "class": "col-sm-2 col-form-label",
                                    "style": "padding-left: 20px !important;",
                                },
                                ann_pred,
                            ],
                            [
                                "div",
                                {"class": "col-sm-10", "style": "padding-right: 0px !important;"},
                                [
                                    "input",
                                    {
                                        "type": "text",
                                        "class": "form-control",
                                        "value": av.replace('"', "&quot;"),
                                    },
                                ],
                            ],
                        ],
                    ],
                ]
        value_col.append(ann_html)

    return ["div", {"class": "row py-1"}, header_col, value_col]


def get_html_type_and_values(datatype, values=None) -> Tuple[Optional[str], Optional[list]]:
    res = CONN.execute(
        'SELECT parent, "HTML type", condition FROM datatype WHERE datatype = :datatype',
        datatype=datatype,
    ).fetchone()
    if res:
        if not values:
            condition = res["condition"]
            if condition and condition.startswith("in"):
                parsed = CONFIG["parser"].parse(condition)[0]
                # TODO: the in conditions are parsed with surrounding quotes
                #       looks like there are always quotes? Maybe not with numbers?
                values = [x["value"][1:-1] for x in parsed["args"]]
        html_type = res["HTML type"]
        if html_type:
            return html_type, values
        parent = res["parent"]
        if parent:
            return get_html_type_and_values(parent, values=values)
    return None, None


def get_primary_key(table_name):
    query = sql_text(
        'SELECT "column" FROM "column" WHERE "table" = :table AND "structure" LIKE "%%primary%%"'
    )
    res = CONN.execute(query, table=table_name).fetchone()
    if res:
        return res["column"]
    else:
        return "row_number"


def get_row_as_form(table_name, row):
    """Transform a row either from query results or validation into a hiccup-style HTML form."""
    html = ["form", {"method": "post"}]
    row_number = row.get("row_number")
    if row_number:
        html.append(get_hiccup_form_row("row_number", readonly=True, value=row_number))
    row_valid = None
    for header, value in row.items():
        if header == "row_number" or header.endswith("_meta"):
            continue

        # Get the details from the value,
        # which is either a JSON object (from validation) or a literal
        valid = None
        message = None
        if value and isinstance(value, dict):
            # This row is coming from a validated row
            message = "<br>".join({x["message"] for x in value["messages"]})
            valid = value["valid"]
            if valid and row_valid is None:
                row_valid = True
            elif not valid:
                row_valid = False
            value = value["value"]
        else:
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

        desc = None
        # Default HTML type is a simple text input
        html_type = "text"
        allowed_values = None
        tables = get_sql_tables(CONN)
        if "column" in tables:
            # Use column table to get description & datatype for this col
            res = CONN.execute(
                sql_text(
                    """SELECT description, datatype, structure FROM "column"
                    WHERE "table" = :table AND "column" = :column"""
                ),
                table=table_name,
                column=header,
            ).fetchone()
            if res:
                desc = res["description"]
                datatype = res["datatype"]
                structure = res["structure"]
                if structure and structure.startswith("from("):
                    # Given the from structure, we always turn the input into a search
                    html_type = "search"
                elif datatype and "datatype" in tables:
                    # Everything else uses an HTML type defined in the datatype table
                    # If a datatype does not have an HTML type, search for first ancestor type
                    html_type, allowed_values = get_html_type_and_values(datatype)
        if allowed_values and not html_type:
            # Default to search when allowed_values are provided
            # This will still allow users to input invalid values
            html_type = "search"

        readonly = False
        if html_type == "readonly":
            html_type = "text"
            readonly = True

        # Add the hiccup vector for this field as a Bootstrap row containing form elements
        html.append(
            get_hiccup_form_row(
                header,
                allowed_values=allowed_values,
                description=desc,
                html_type=html_type,
                message=message,
                readonly=readonly,
                valid=valid,
                value=value,
            )
        )

    if row_valid:
        # All fields passed validation - display green button
        submit_cls = "success"
    elif row_valid is not None:
        # One or more fields failed validation - display red button
        submit_cls = "danger"
    else:
        # Row has not yet been validated - display gray button
        submit_cls = "secondary"
    html.append(
        [
            "div",
            {"class": "row", "style": "padding-top: 10px;"},
            [
                "div",
                {"class": "col-auto"},
                [
                    "button",
                    {
                        "type": "submit",
                        "name": "action",
                        "value": "validate",
                        "class": "btn btn-large btn-outline-primary",
                    },
                    "Validate",
                ],
            ],
            [
                "div",
                {"class": "col-auto"},
                [
                    "button",
                    {
                        "type": "submit",
                        "name": "action",
                        "value": "submit",
                        "class": f"btn btn-large btn-outline-{submit_cls}",
                    },
                    "Submit",
                ],
            ],
        ]
    )
    return render(html)


def get_row_number(table_name, primary_key):
    pk_col = get_primary_key(table_name)
    res = CONN.execute(
        sql_text(f'SELECT row_number FROM "{table_name}" WHERE "{pk_col}" = :pk'), pk=primary_key
    ).fetchone()
    if not res:
        return None
    return int(res["row_number"])


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


def validate_table_row(table_name, row, row_number=None):
    """Perform validation on a row"""
    # Transform row into dict expected for validate
    if row_number:
        result_row = {}
        for column, value in row.items():
            result_row[column] = {
                "value": value,
                "valid": True,
                "messages": [],
            }
        # Row number may be different than row ID, if this column is used
        return validate_row(CONFIG, table_name, result_row, row_number=row_number)
    else:
        return validate_row(CONFIG, table_name, row, existing_row=False)


# ----- ONTOLOGY TABLE METHODS -----


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
        search(CONN, limit=30, search_text=search_text, statement=table_name, term_ids=terms)
    )


def get_href_pattern(table_name, view=None):
    return (
        url_for("cmi-pb.term", table_name=table_name, view=view, term_id="{curie}")
        .replace("%7B", "{")
        .replace("%7D", "}")
    )


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
    return get_labels(CONN, parent_terms, statement=table_name)


def is_ontology(table_name):
    columns = get_sql_columns(CONN, table_name)
    return {"subject", "predicate", "object", "datatype", "annotation"}.issubset(set(columns))


def merge_dicts(d1, d2):
    d_copy = d1.copy()
    for k, v in d_copy.items():
        if k in d2:
            v.update(d2[k])
        else:
            d_copy[k] = d2[k]
    return d_copy


def render_ontology_table(table_name, data, predicates: list = None, term_id=None):
    """
    :param table_name: name of SQL table that contains terms
    :param data: data to render - dict of term ID -> predicate ID -> list of JSON objects
    """
    # TODO: do we care about displaying annotations in this table view? Or only on term view?
    # Reverse the ID -> label dictionary to translate column names to IDs
    if not predicates:
        predicates = set(chain.from_iterable([list(x.keys()) for x in data.values()]))
    predicate_labels = get_labels(CONN, list(predicates), statement=table_name)
    # Order based on raw value of 'object', don't worry about rendering
    if request.args.get("order"):
        label_to_id = {v: k for k, v in predicate_labels.items()}
        order_by = parse_order_by(request.args["order"])
        for ob in order_by:
            reverse = False
            if ob["order"] == "desc":
                reverse = True

            if ob["key"] == "ID":
                # ID is never null
                # sorted is more efficient here so we don't need to turn dict into list of tuples
                data = {k: v for k, v in sorted(data.items(), reverse=reverse)}
                continue

            key = label_to_id.get(ob["key"], ob["key"])  # e.g., rdfs:label

            # Separate out the items with no list entries for this predicate
            nulls = {k: v for k, v in data.items() if not v[key]}
            non_nulls = [[k, v] for k, v in data.items() if v[key]]

            # Sort the items with entries for this predicate
            non_nulls.sort(key=lambda itm: itm[1][key][0]["object"], reverse=reverse)

            # ... then put the nulls in the correct spot
            if ob["nulls"] == "first":
                data = nulls
                data.update({k: v for k, v in non_nulls})
            else:
                data = {k: v for k, v in non_nulls}
                data.update(nulls)

    # Offset and limit used to determine which terms to render
    # Rendering objects for all terms is very slow
    offset = int(request.args.get("offset", "0"))
    limit = int(request.args.get("limit", "100")) + offset

    data = [[k, v] for k, v in data.items()]
    data_subset = {k: v for k, v in data[offset:limit]}
    post_subset = {k: v for k, v in data[limit:]}
    data = {k: v for k, v in data[:offset]}

    fmt = request.args.get("format")

    if not fmt:
        # Convert objects to hiccup with ofn
        rendered = terms2dict(
            CONN, data_subset, hiccup=True, include_annotations=True, statement=table_name
        )
        for term_id, predicate_objects in rendered.items():
            # Render using hiccup module
            rendered_term = {}
            for predicate_id, hiccup in predicate_objects.items():
                if hiccup:
                    hiccup = insert_href(hiccup, href=get_href_pattern(table_name))
                    rendered_term[predicate_id] = render(hiccup)
                else:
                    rendered_term[predicate_id] = None
            data[term_id] = rendered_term
        data.update(post_subset)

        predicates = set(chain.from_iterable([list(x.keys()) for x in rendered.values()]))
        predicate_labels = get_labels(CONN, list(predicates), statement=table_name)

        # Create the HTML output of data
        table_data = []
        for term_id, predicate_objects in data.items():
            # We always display the ID, regardless of other columns
            term_id = html_escape(term_id)
            term_data = {
                "ID": render(
                    [
                        "a",
                        {"href": url_for("cmi-pb.term", table_name=table_name, term_id=term_id)},
                        term_id,
                    ],
                )
            }
            for predicate, objs in predicate_objects.items():
                term_data[predicate_labels.get(predicate, predicate)] = objs
            table_data.append(term_data)
        if term_id:
            base_url = url_for("cmi-pb.term", table_name=table_name, term_id=term_id)
        else:
            base_url = url_for("cmi-pb.table", table_name=table_name)
        try:
            predicates = list(predicates)
            predicates.insert(0, "ID")
            return render_html_table(
                table_data,
                table_name,
                request.args,
                base_url=base_url,
                columns=[predicate_labels.get(p, p) for p in predicates],
                hidden=["search_text"],
                ignore_params=["project-name", "branch-name", "view-path"],
                include_expand=False,
                show_options=False,
                standalone=False,
            )
        except SprocketError as e:
            abort(422, str(e))
    elif fmt.lower() in ["tsv", "csv"]:
        # Create TSV or CSV export of data
        field_sep = request.args.get("sep", "|")
        mt = "text/tab-separated-values"
        delimiter = "\t"
        if fmt.lower() == "csv":
            delimiter = ","
            mt = "text/comma-separated-values"
        return Response(
            terms2tsv(CONN, data_subset, delimiter=delimiter, sep=field_sep, statement=table_name),
            mimetype=mt,
        )
    else:
        return abort(400, "Unknown export format: " + fmt)


def render_subclass_of(table_name, param, arg):
    id_to_label = get_terms_from_arg(table_name, arg)
    hrefs = [
        f"<a href='/{url_for('cmi-pb.term', table_name=table_name, term_id=term_id)}'>{label}</a>"
        for term_id, label in id_to_label.items()
    ]
    title = "Showing children of " + ", ".join(hrefs)

    terms = set()
    if param == "subClassOf":
        for p in id_to_label.keys():
            terms.update(get_children(CONN, p, statement=table_name))
    elif param == "subClassOf?":
        terms.update(id_to_label.keys())
        for p in id_to_label.keys():
            terms.update(get_children(CONN, p, statement=table_name))
    elif param == "subClassOfplus":
        for p in id_to_label.keys():
            terms.update(get_descendants(CONN, p, statement=table_name))
    elif param == "subClassOf*":
        terms.update(id_to_label.keys())
        for p in id_to_label.keys():
            terms.update(get_descendants(CONN, p, statement=table_name))
    else:
        abort(400, "Unknown 'subClassOf' query parameter: " + param)

    if request.args.get("format") == "json":
        # Support for searching the subset of these terms
        data = search(
            CONN,
            limit=30,
            search_text=request.args.get("text", ""),
            statement=table_name,
            term_ids=list(terms),
        )
        return json.dumps(data)
    # Maybe get a set of predicates to restrict search results to
    select = request.args.get("select")
    predicates = ["rdfs:label", SYNONYMS[0]]
    if select:
        # TODO: add form at top of page for user to select predicates to show?
        pred_labels = select.split(",")
        predicates = get_ids(
            CONN, id_or_labels=pred_labels, id_type="predicate", statement=table_name
        )

    data = get_term_attributes(
        CONN, exclude_json=True, predicates=predicates, statement=table_name, term_ids=list(terms)
    )
    response = render_ontology_table(table_name, data, predicates=predicates)
    if isinstance(response, Response):
        return response
    return render_template(
        "template.html",
        html=response,
        title=title,
        add_params=f"{param}={arg}",
        show_search=True,
        table_name=table_name,
        tables=get_sql_tables(CONN),
    )


def render_term_form(table_name, term_id):
    global FORM_ROW_ID
    entity_type = get_top_entity_type(CONN, term_id, statements=table_name)

    # Get all annotation properties
    query = sql_text(
        f'SELECT DISTINCT predicate FROM "{table_name}" WHERE predicate NOT IN :logic',
    ).bindparams(bindparam("logic", expanding=True))
    results = CONN.execute(query, {"logic": LOGIC_PREDICATES}).fetchall()
    aps = get_labels(CONN, [x["predicate"] for x in results], statement=table_name)

    term_details = get_term_attributes(CONN, statement=table_name, term_ids=[term_id])
    if not term_details:
        return abort(400, f"Unable to find term {term_id} in '{table_name}' table")
    term_details = term_details[term_id]

    # Separate details into metadata & logic
    metadata = {k: v for k, v in term_details.items() if k not in LOGIC_PREDICATES}
    logic = {k: v for k, v in term_details.items() if k in LOGIC_PREDICATES}

    # Build the metadata form elements, starting with term ID (always displayed first)
    metadata_html = [
        get_hiccup_form_row(
            "ID", display_header="ontology ID", html_type="text", readonly=True, value=term_id
        )
    ]

    # Add the label element (always displayed second)
    label_details = term_details.get("rdfs:label")
    label = None
    if label_details:
        # TODO: handle multiple labels
        label_details = label_details[0]
        label_annotation = label_details.get("annotation")
        if label_annotation:
            label_annotation = json.loads(label_annotation)
        label = label_details.get("object")
        # TODO: get label label
        metadata_html.append(
            get_hiccup_form_row(
                "rdfs:label",
                annotations=label_annotation,
                display_header="label",
                html_type="text",
                value=label,
            )
        )

    # Add the rest of the annotations
    for predicate_id, detail in metadata.items():
        if predicate_id == "rdfs:label":
            continue
        # TODO: support other HTML types (dropdown, boolean, etc.)
        pred_label = aps.get(predicate_id, predicate_id)
        html_type = "text"
        if pred_label in ["definition", "comment", "rdfs:comment"]:
            html_type = "textarea"

        for d in detail:
            d_annotations = None
            if d.get("annotation"):
                d_annotations = json.loads(d.get("annotation"))
            metadata_html.append(
                get_hiccup_form_row(
                    predicate_id,
                    allow_delete=True,
                    annotations=d_annotations,
                    display_header=pred_label,
                    html_type=html_type,
                    value=d.get("object"),
                )
            )

    logic_html = []
    # TODO: use wiring to render the object
    for predicate_id, objects in logic.items():
        pred_label = BUILTIN_LABELS.get(predicate_id, aps.get(predicate_id, predicate_id))
        for o in objects:
            o_annotations = None
            if o.get("annotation"):
                o_annotations = json.loads(o.get("annotation"))
            logic_html.append(
                get_hiccup_form_row(
                    predicate_id,
                    allow_delete=True,
                    annotations=o_annotations,
                    display_header=pred_label,
                    html_type="search",
                    value=o.get("object"),
                )
            )

    if label and " " in label:
        # Encase in single quotes when label has a space
        label = f"'{label}'"

    metadata_html.insert(0, {"class": "row", "id": "term-metadata"})
    metadata_html.insert(0, "div")

    logic_html.insert(0, {"class": "row", "id": "term-logic"})
    logic_html.insert(0, "div")

    # Reset form row ID for next time
    FORM_ROW_ID = 0
    return render_template(
        "ontology_form.html",
        include_back=True,
        table_name=table_name,
        tables=get_sql_tables(CONN),
        term_id=term_id,
        title=f"Update " + label or term_id,
        annotation_properties=aps,
        metadata=render(metadata_html),
        logic=render(logic_html),
        entity_type=entity_type,
    )


def render_tree(table_name, term_id: str = None):
    if not is_ontology(table_name):
        return abort(418, "Cannot show tree view for non-ontology table")

    # TODO: add support for select
    search_text = request.args.get("text")
    if search_text:
        if term_id:
            terms = get_terms_from_arg(table_name, term_id).keys()
        else:
            terms = []
        # Get matching terms (label or synonym - need to support other cols if select is provided)
        data = search(CONN, limit=30, search_text=search_text, statement=table_name, term_ids=terms)
        data = get_term_attributes(
            CONN,
            predicates=["rdfs:label", SYNONYMS[0]],
            statement=table_name,
            term_ids=[x["id"] for x in data],
        )
        response = render_ontology_table(table_name, data, predicates=["rdfs:label", SYNONYMS[0]])
        if isinstance(response, Response):
            return response
        return render_template(
            "template.html",
            html=response,
            title=f"Showing search results for '{search_text}'",
            show_search=True,
            table_name=table_name,
            tables=get_sql_tables(CONN),
        )

    # nothing to search, just return the tree view
    html = ""
    if term_id:
        term_url = url_for("cmi-pb.term", table_name=table_name, term_id=term_id)
        tree_url = url_for("cmi-pb.term", table_name=table_name, term_id=term_id, view="tree")
        html += '<div class="row justify-content-end"><div class="col-auto"><div class="btn-group">'
        html += f'<a href="{term_url}" class="btn btn-sm btn-outline-primary">Table</a>'
        html += f'<a href="{tree_url}" class="btn btn-sm btn-outline-primary active">Tree</a>'
        html += "</div></div></div>"
    html += tree(
        CONN,
        href=get_href_pattern(table_name, view="tree"),
        include_search=False,
        standalone=False,
        max_children=20,
        statement=table_name,
        term_id=term_id,
    )
    tables = [x for x in get_sql_tables(CONN) if not x.startswith("tmp_")]
    return render_template(
        "template.html", html=html, show_search=True, table_name=table_name, tables=tables,
    )


def update_term(table_name, term_id):
    # TODO: new LDTab structure
    # Get current annotations for this term
    query = sql_text(
        f"""SELECT predicate, object, datatype, annotation FROM "{table_name}"
        WHERE subject = :s AND object IS NOT NULL AND predicate NOT IN :logic"""
    ).bindparams(bindparam("s"), bindparam("logic", expanding=True))
    results = CONN.execute(query, s=term_id, logic=LOGIC_PREDICATES)
    annotations = defaultdict(list)
    for res in results:
        if res["predicate"] not in annotations:
            annotations[res["predicate"]] = list()
        annotations[res["predicate"]].append(res)

    # Get current logic for this term
    logic = defaultdict(list)
    query = sql_text(
        f"""SELECT predicate, object, datatype, annotation FROM {table_name}
        WHERE subject = :s AND object IS NOT NULL AND predicate IN :logic"""
    ).bindparams(bindparam("s"), bindparam("logic", expanding=True))
    results = CONN.execute(query, s=term_id, logic=LOGIC_PREDICATES)
    for res in results:
        if res["predicate"] not in logic:
            logic[res["predicate"]] = list()
        logic[res["predicate"]].append(res)

    # Get all annotation properties so we know where to put predicates
    # aps = get_annotation_properties(table_name)

    form_annotations = defaultdict(list)
    form_logic = defaultdict(list)
    for predicate, value in request.form.items():
        if predicate == "ID":
            continue
        if predicate not in LOGIC_PREDICATES:
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
    for predicate, value_objects in annotations.items():
        new_values = request.form.getlist(predicate)
        # Removed objects
        removed = [vo for vo in value_objects if vo["object"] not in new_values]
        # Added values
        added = [nv for nv in new_values if nv not in [vo["object"] for vo in value_objects]]
        for r in removed:
            query = sql_text(
                f'DELETE FROM "{table_name}" WHERE subject = :s AND predicate = :p AND object = :v'
            )
            CONN.execute(query, s=term_id, p=predicate, v=r)
        for a in added:
            # TODO: set datatype (need it on form first) & annotation,
            #       defaulting to xsd:string for everything for now
            query = sql_text(
                f"""INSERT INTO "{table_name}" (subject, predicate, object, datatype)
                VALUES (:s, :p, :v, 'xsd:string')"""
            )
            CONN.execute(query, s=term_id, p=predicate, v=a)

    # Add new annotation predicates + values (predicates that have not been used on this term)
    for predicate, values in form_annotations.items():
        for v in values:
            query = sql_text(
                f"""INSERT INTO "{table_name}" (subject, predicate, object, datatype)
                VALUES (:s, :p, :v, 'xsd:string')"""
            )
            CONN.execute(query, s=term_id, p=predicate, v=v)

    # Look for changes to existing logic predicates on this term
    for predicate, logic_objects in logic.items():
        if predicate == "rdf:type" and any(
            [
                o
                for o in [lo["object"] for lo in logic_objects]
                if o
                in [
                    "owl:Class",
                    "owl:AnnotationProperty",
                    "owl:DataProperty",
                    "owl:ObjectProperty",
                    "owl:Datatype",
                ]
            ]
        ):
            # Only look at type for individuals
            continue

        new_objects = request.form.getlist(predicate)
        # TODO: instead of getting IDs, use wiring to translate manchester into JSON object
        new_obj_ids = get_ids(CONN, id_or_labels=new_objects, statement=table_name)
        if len(new_objects) > len(new_obj_ids):
            LOGGER.error(
                "Cannot get IDs for one or more terms from term list: " + ", ".join(new_objects)
            )

        # TODO: Compare JSON objects for _json datatypes, IRIs for everything else
        removed = [lo for lo in logic_objects if lo["object"] not in new_objects]
        added = [no for no in new_objects if no not in [vo["object"] for vo in logic_objects]]

        # TODO: Do not remove owl:Thing
        for r in removed:
            # TODO: this will not work for the JSON objects
            query = sql_text(
                f"""DELETE FROM {table_name}
                    WHERE subject = :s AND predicate = :p AND object = :v"""
            )
            CONN.execute(query, s=term_id, p=predicate, v=r)
        for a in added:
            # TODO: support adding annotations
            query = sql_text(
                f"""INSERT INTO "{table_name}" (subject, predicate, object, datatype)
                VALUES (:s, :p, :o, '_IRI')"""
            )
            CONN.execute(query, s=term_id, p=predicate, o=a)

    # Add new logic predicates + objects
    for predicate, objects in form_logic.items():
        # All new predicates
        for o in objects:
            query = sql_text(
                f"""INSERT INTO "{table_name}" (subject, predicate, object, datatype)
                            VALUES (:s, :p, :o, '_IRI')"""
            )
            CONN.execute(query, s=term_id, p=predicate, o=o)
    return term_id


def run(db, table_config, cgi_path=None, log_file=None, synonyms=None):
    """
    :param db:
    :param table_config:
    :param cgi_path:
    :param log_file:
    :param synonyms: list of synonyms to include in search results
                     (the first item of this list is used as the synonym displayed in table view)
    """
    global CONFIG, CONN, LOGGER, SYNONYMS

    if synonyms:
        # Override default (only IAO 'alternative term')
        SYNONYMS = synonyms

    app = Flask(__name__)
    app.register_blueprint(BLUEPRINT)
    app.url_map.strict_slashes = False

    # Set up logging to file
    LOGGER = logging.getLogger("cmi_pb_logger")
    LOGGER.setLevel(logging.DEBUG)
    if log_file:
        fh = logging.FileHandler(log_file)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S")
        )
        LOGGER.addHandler(fh)

    # sqlite3 is required for executescript used in load
    setup_conn = sqlite3.connect(db, check_same_thread=False)
    CONFIG = read_config_files(table_config, Lark(grammar, parser="lalr", transformer=TreeToDict()))
    CONFIG["db"] = setup_conn
    configure_db(CONFIG)

    # SQLAlchemy connection required for sprocket/gizmos
    abspath = os.path.abspath(db)
    db_url = "sqlite:///" + abspath + "?check_same_thread=False"
    engine = create_engine(db_url)
    CONN = engine.connect()

    if cgi_path:
        os.environ["SCRIPT_NAME"] = cgi_path
        from wsgiref.handlers import CGIHandler

        CGIHandler().run(app)
    else:
        LOGGER.error(os.path.abspath(os.path.join(os.path.dirname(__file__), "templates")))
        app.run()
