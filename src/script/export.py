#!/usr/bin/env python3

import csv
import os.path
import re
import sqlite3
import sys

from argparse import ArgumentParser


def export(db, output_dir, tables):
    """
    Given the filename of a database, an output directory, and a list of tables, export all of the
    given database tables to .tsv files in the output directory.
    """
    with sqlite3.connect(db) as conn:
        for table in tables:
            try:
                # Determine the sorting order of the table's columns for export. For tables with
                # primary keys, sort by primary key first, then by all other columns from left to
                # right. For tables without primary keys, sort by rowid.
                pragma_rows = conn.execute(f"PRAGMA TABLE_INFO(`{table}`)")
                column_names = [d[0] for d in pragma_rows.description]
                pragma_rows = list(map(lambda r: dict(zip(column_names, r)), pragma_rows))
                if not any([row["pk"] == 1 for row in pragma_rows]):
                    order_by = "ROWID"
                else:
                    sorted_column_names = []
                    for row in pragma_rows:
                        if row["pk"] == 1:
                            sorted_column_names.insert(0, row["name"])
                        else:
                            sorted_column_names.append(row["name"])
                    order_by = list(map(lambda x: f"`{x}`", sorted_column_names))
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


if __name__ == "__main__":
    p = ArgumentParser()
    p.add_argument("db", help="The name of the database file")
    p.add_argument("output_dir", help="The name of the directory in which to save TSV files")
    p.add_argument(
        "tables", metavar="table", nargs="+", help="The name of a table to export to TSV"
    )
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

    export(db, output_dir, tables)
