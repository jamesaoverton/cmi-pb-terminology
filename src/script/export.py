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
    right. For tables without primary keys, sort by rowid. Returns a tuple consisting of an unsorted
    and a sorted list of column names in the first and second position of the tuple, respectively,
    and a list of the table's primary keys sorted by priority in the third position.
    """
    pragma_rows = conn.execute(f"PRAGMA TABLE_INFO(`{table}`)")
    columns_info = [d[0] for d in pragma_rows.description]
    pragma_rows = list(map(lambda r: OrderedDict(zip(columns_info, r)), pragma_rows))
    primary_keys = OrderedDict()
    if not any([row["pk"] == 1 for row in pragma_rows]):
        sorted_columns = ["ROWID"]
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
            elif not row["name"].endswith("_meta"):
                other_columns.append(row["name"])
        primary_keys = OrderedDict(sorted(primary_keys.items()))
        sorted_columns = add_meta([primary_keys[key] for key in primary_keys] + other_columns)
    unsorted_columns = [p["name"] for p in pragma_rows]
    return unsorted_columns, sorted_columns, [primary_keys[key] for key in primary_keys]


def export_data(db, output_dir, tables):
    """
    Given the filename of a database, an output directory, and a list of tables, export all of the
    given database tables to .tsv files in the output directory.
    """
    with sqlite3.connect(db) as conn:
        for table in tables:
            try:
                _, sorted_columns, _ = get_columns_info(conn, table)
                order_by = list(map(lambda x: f"`{x}`", sorted_columns))
                order_by = ", ".join(order_by)

                # Fetch the rows from the table and write them to a corresponding TSV file in the
                # output directory:
                rows = conn.execute(f"SELECT * FROM `{table}` ORDER BY {order_by}")
                with open(f"{output_dir}/{table}.tsv", "w", newline="\n") as csvfile:
                    writer = csv.writer(
                        csvfile,
                        delimiter="\t",
                        doublequote=False,
                        strict=True,
                        lineterminator="\n",
                        quoting=csv.QUOTE_NONE,
                        escapechar="\\",
                        quotechar="",
                    )
                    column_names = [d[0] for d in rows.description]
                    writer.writerow(column_names)
                    for row in rows:
                        row = map(
                            lambda c: (
                                re.sub(r"^json\((.*)\)$", r"\1", c)
                                if c and re.match(r"^json\((.*)\)$", c)
                                else c
                            ),
                            row,
                        )
                        writer.writerow(row)
            except sqlite3.OperationalError as e:
                print(f"ERROR: {e}", file=sys.stderr)


def export_messages(db, output_dir, tables, a1=False):
    """
    TODO: Add a docstring 'ere.
    """

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

    def create_message_rows(table, row, row_number, primary_keys):
        if a1 or not primary_keys:
            rowid = f"{row_number}"
        else:
            rowid = "###".join([row.get(f"{pk}") or "" for pk in primary_keys])

        message_rows = []
        for column_key in [ckey for ckey in row if ckey.endswith("_meta")]:
            meta = json.loads(re.sub(r"^json\((.*)\)$", r"\1", row[column_key]))
            if not meta["valid"]:
                columnid = re.sub(r"(.+)_meta$", r"\1", column_key)
                if a1:
                    columnid = col_to_a1(columnid, [c for c in row if not c.endswith("_meta")])

                for message in meta["messages"]:
                    m = {
                        "table": table,
                        "level": message["level"],
                        "rule_id": message["rule"],
                        "message": message["message"],
                        "value": meta["value"],
                    }
                    if not a1:
                        m.update({"row": rowid, "column": columnid})
                    else:
                        m.update({"cell": f"{columnid}{rowid}"})
                    message_rows.append(m)
        return message_rows

    def add_conflict_tables(tables):
        more_tables = []
        for table in tables:
            more_tables.append(table)
            more_tables.append(f"{table}_conflict")
        return more_tables

    with sqlite3.connect(db) as conn:
        tables = add_conflict_tables(tables)
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
                    unsorted_columns, sorted_columns, primary_keys = get_columns_info(conn, table)
                    select = ", ".join([f"`{c}`" for c in unsorted_columns])
                    order_by = ", ".join([f"`{c}`" for c in sorted_columns])
                    rows = conn.execute(f"SELECT {select} FROM `{table}` ORDER BY {order_by}")
                    columns_info = [d[0] for d in rows.description]
                    rows = map(lambda r: OrderedDict(zip(columns_info, r)), rows)
                    for row_number, row in enumerate(rows):
                        message_rows = create_message_rows(table, row, row_number + 1, primary_keys)
                        writer.writerows(message_rows)
                except sqlite3.OperationalError as e:
                    print(f"ERROR: {e}", file=sys.stderr)


if __name__ == "__main__":
    p = ArgumentParser()
    p.add_argument("db", help="The name of the database file")
    p.add_argument("output_dir", help="The name of the directory in which to save TSV files")
    p.add_argument(
        "tables", metavar="table", nargs="+", help="The name of a table to export to TSV"
    )
    p.add_argument(
        "--messages", "-m", action="store_true", help="Output error messages instead of table data"
    )
    p.add_argument("--a1", action="store_true", help="Output error messages in A1 format")
    args = p.parse_args()
    db = args.db
    output_dir = os.path.normpath(args.output_dir)
    tables = args.tables

    if not os.path.exists(db):
        print(f"The database '{db}' does not exist.", file=sys.stderr)
        sys.exit(1)

    if not os.path.isdir(output_dir):
        print(f"The directory: {output_dir} does not exist", file=sys.stderr)
        sys.exit(1)

    if not args.messages:
        export_data(db, output_dir, tables)
    else:
        export_messages(db, output_dir, tables, args.a1)
