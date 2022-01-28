#!/usr/bin/env python3

import csv
import json
import os.path
import re
import sqlite3
import sys

from argparse import ArgumentParser
from collections import OrderedDict


def get_columns_info(conn, table):
    """
    Given a database connection and a table name, determine the order of the table's columns. For
    tables with primary keys, sort by primary key first, then by all other columns from left to
    right. For tables without primary keys, sort by row_number. Returns a dictionary consisting of
    an unsorted and a sorted list of column names, a list of the table's primary keys sorted by
    priority, and a list of the table's unique keys. I.e., returns a dict of the form:
    {"unsorted_columns": [], "sorted_columns": [], "primary_keys": [], "unique_keys": []}
    """
    pragma_rows = conn.execute(f"PRAGMA TABLE_INFO(`{table}`)")
    columns_info = [d[0] for d in pragma_rows.description]
    pragma_rows = list(map(lambda r: OrderedDict(zip(columns_info, r)), pragma_rows))
    primary_keys = OrderedDict()
    if not any([row["pk"] == 1 for row in pragma_rows]):
        sorted_columns = ["row_number"]
    else:

        def add_meta(columns):
            columns_with_meta = []
            for column in columns:
                columns_with_meta.append(column)
                columns_with_meta.append(column + "_meta")
            return columns_with_meta

        other_columns = []
        for row in pragma_rows:
            if row["pk"] != 0:
                primary_keys[row["pk"]] = row["name"]
            elif not row["name"].endswith("_meta") and not row["name"] == "row_number":
                other_columns.append(row["name"])
        primary_keys = OrderedDict(sorted(primary_keys.items()))
        sorted_columns = add_meta([primary_keys[key] for key in primary_keys] + other_columns)
    unsorted_columns = [p["name"] for p in pragma_rows]

    # Two steps are needed (INDEX_LIST and INDEX_INFO) to get the list of unique keys:
    pragma_rows = conn.execute(f"PRAGMA INDEX_LIST(`{table}`)")
    columns_info = [d[0] for d in pragma_rows.description]
    pragma_rows = list(map(lambda r: dict(zip(columns_info, r)), pragma_rows))
    unique_constraints = [
        key["name"] for key in pragma_rows if key["unique"] == 1 and key["origin"] == "u"
    ]
    unique_keys = []
    for uni in unique_constraints:
        pragma_rows = conn.execute(f"PRAGMA INDEX_INFO(`{uni}`)")
        columns_info = [d[0] for d in pragma_rows.description]
        pragma_rows = list(map(lambda r: dict(zip(columns_info, r)), pragma_rows))
        [unique_keys.append(p["name"]) for p in pragma_rows]

    return {
        "unsorted_columns": unsorted_columns,
        "sorted_columns": sorted_columns,
        "primary_keys": [primary_keys[key] for key in primary_keys],
        "unique_keys": unique_keys,
    }


def export_data(args):
    """
    Given a dictionary containing: the filename of a database, an output directory, and a list of
    tables, export all of the given database tables to .tsv files in the output directory.
    """
    db = args["db"]
    output_dir = os.path.normpath(args["output_dir"])
    tables = args["tables"]
    with sqlite3.connect(db) as conn:
        for table in tables:
            try:
                columns_info = get_columns_info(conn, table)
                sorted_columns = columns_info["sorted_columns"]
                unsorted_columns = columns_info["unsorted_columns"]
                primary_keys = columns_info["primary_keys"]
                unique_keys = columns_info["unique_keys"]

                select = list(map(lambda x: f"`{x}`", unsorted_columns))
                select = ", ".join(select)

                conflict_select = []
                for s in unsorted_columns:
                    if s in primary_keys + unique_keys:
                        conflict_select.append(
                            f"""
                            CASE
                              WHEN `{s}` IS NOT NULL THEN `{s}`
                              ELSE JSON_EXTRACT(
                                     RTRIM(
                                       SUBSTRING(`{s}_meta`, 6),
                                       ")"),
                                     '$.value'
                                   )
                              END AS `{s}`
                            """
                        )
                    else:
                        conflict_select.append(f"`{s}`")
                conflict_select = ", ".join(conflict_select)

                order_by = list(map(lambda x: f"`{x}`", sorted_columns))
                order_by = ", ".join(order_by)

                # Fetch the rows from the table and write them to a corresponding TSV file in the
                # output directory:
                # TODO: WE CAN SIMPLIFY. JUST USE `{table}_view`.
                rows = conn.execute(
                    f"SELECT {select} FROM `{table}` "
                    f"UNION "
                    f"SELECT {conflict_select} FROM `{table}_conflict` "
                    f"ORDER BY {order_by}"
                )
                colnames = [d[0] for d in rows.description]
                rows = map(lambda r: dict(zip(colnames, r)), rows)

                fieldnames = [c for c in colnames if not c.endswith("_meta") and c != "row_number"]
                with open(f"{output_dir}/{table}.tsv", "w", newline="\n") as csvfile:
                    writer = csv.DictWriter(
                        csvfile,
                        fieldnames=fieldnames,
                        delimiter="\t",
                        doublequote=False,
                        strict=True,
                        lineterminator="\n",
                        quoting=csv.QUOTE_NONE,
                        escapechar="",
                        quotechar="",
                    )
                    writer.writeheader()
                    for row in rows:
                        del row["row_number"]
                        # TODO: WE DON'T NEED TO DO THIS HERE. DO IT DIRECTLY IN THE SQL FOR ALL
                        # COLUMNS, NOT JUST PRIMARY KEYS:
                        for meta_col in [c for c in row if c.endswith("_meta")]:
                            meta_data = json.loads(re.sub(r"^json\((.*)\)$", r"\1", row[meta_col]))
                            col = meta_col.removesuffix("_meta")
                            row[col] = row[col] if meta_data["valid"] else meta_data["value"]
                            del row[meta_col]
                        writer.writerow(row)
            except sqlite3.OperationalError as e:
                print(f"ERROR: {e}", file=sys.stderr)


def export_messages(args):
    """
    Given a dictionary containing: the filename, "db", of a database, an output directory, "output",
    a list of tables, "tables", and a flag, "a1", indicating whether to use A1 format, export all of
    the error messages contained in the given database tables to a file called messages.tsv in the
    output directory.
    """
    db = args["db"]
    output_dir = os.path.normpath(args["output_dir"])
    tables = args["tables"]
    a1 = args["a1"]

    def col_to_a1(column, columns):
        col = columns.index(column) + 1
        div = col
        columnid = ""
        while div:
            (div, mod) = divmod(div, 26)
            if mod == 0:
                mod = 26
                div -= 1
            columnid = chr(mod + 64) + columnid
        return columnid

    def create_message_rows(table, row, primary_keys):
        if a1 or not primary_keys:
            row_number = "{}".format(row["row_number"])
        else:
            row_number = []
            for pk in primary_keys:
                rn = row[pk]
                if not rn:
                    rn = json.loads(re.sub(r"^json\((.*)\)$", r"\1", row[f"{pk}_meta"]))["value"]
                row_number.append(rn)
            row_number = "###".join(row_number)

        message_rows = []
        for column_key in [ckey for ckey in row if ckey.endswith("_meta")]:
            meta = json.loads(re.sub(r"^json\((.*)\)$", r"\1", row[column_key]))
            if not meta["valid"]:
                columnid = column_key.removesuffix("_meta")
                if a1:
                    columnid = col_to_a1(
                        columnid, [c for c in row if c != "row_number" and not c.endswith("_meta")]
                    )

                for message in meta["messages"]:
                    m = {
                        "table": table,
                        "level": message["level"],
                        "rule_id": message["rule"],
                        "message": message["message"],
                        "value": meta["value"],
                    }
                    if not a1:
                        m.update({"row": row_number, "column": columnid})
                    else:
                        m.update({"cell": f"{columnid}{row_number}"})
                    message_rows.append(m)
        return message_rows

    with sqlite3.connect(db) as conn:
        if a1:
            fieldnames = ["table", "cell", "level", "rule_id", "message", "value"]
        else:
            fieldnames = ["table", "row", "column", "level", "rule_id", "message", "value"]
        with open(f"{output_dir}/messages.tsv", "w", newline="\n") as csvfile:
            writer = csv.DictWriter(
                csvfile,
                fieldnames=fieldnames,
                delimiter="\t",
                doublequote=False,
                strict=True,
                lineterminator="\n",
                quoting=csv.QUOTE_NONE,
                escapechar="\\",
                quotechar="",
            )
            writer.writeheader()
            for table in tables:
                try:
                    columns_info = get_columns_info(conn, table)
                    unsorted_columns = columns_info["unsorted_columns"]
                    sorted_columns = columns_info["sorted_columns"]
                    primary_keys = columns_info["primary_keys"]

                    select = ", ".join([f"`{c}`" for c in unsorted_columns])
                    order_by = ", ".join([f"`{c}`" for c in sorted_columns])
                    rows = conn.execute(f"SELECT {select} FROM `{table}_view` ORDER BY {order_by}")
                    columns_info = [d[0] for d in rows.description]
                    rows = map(lambda r: OrderedDict(zip(columns_info, r)), rows)
                    for row in rows:
                        message_rows = create_message_rows(table, row, primary_keys)
                        writer.writerows(message_rows)
                except sqlite3.OperationalError as e:
                    print(f"ERROR: {e}", file=sys.stderr)


if __name__ == "__main__":
    prog_parser = ArgumentParser(description="Database table export utility")
    sub_parsers = prog_parser.add_subparsers(help="Possible sub-commands")

    sub1 = sub_parsers.add_parser(
        "data",
        description="Export table data",
        help="Export table data. For command-line options, run: `%(prog)s data --help`",
    )
    # TODO: We aren't actually handling this case:
    sub1.add_argument(
        "--raw", action="store_true", help="Include _meta columns in table data export"
    )
    sub1.set_defaults(func=export_data)

    sub2 = sub_parsers.add_parser(
        "messages",
        description="Export error messages",
        help="Export error messages. For command-line options, run: `%(prog)s messages --help`",
    )
    sub2.add_argument("--a1", action="store_true", help="Output error messages in A1 format")
    sub2.set_defaults(func=export_messages)

    for sub in [sub1, sub2]:
        sub.add_argument("db", help="The name of the database file")
        sub.add_argument("output_dir", help="The name of the directory in which to save TSV files")
        sub.add_argument(
            "tables", metavar="table", nargs="+", help="The name of a table to export to TSV"
        )

    args = prog_parser.parse_args()
    if not os.path.exists(args.db):
        print(f"The database '{args.db}' does not exist.", file=sys.stderr)
        sys.exit(1)

    if not os.path.isdir(args.output_dir):
        print(f"The directory: {args.output_dir} does not exist", file=sys.stderr)
        sys.exit(1)

    args.func(vars(args))
