#!/usr/bin/env python3

import glob
import os
import sqlite3
import sys

from argparse import ArgumentParser
from lark import Lark
from os.path import basename, isfile, isdir, realpath
from pprint import pformat
from subprocess import run

pwd = os.path.dirname(os.path.realpath(__file__))
sys.path.append("{}/../src/script".format(pwd))

from load import grammar, TreeToDict, read_config_files, create_db_and_write_sql, update_row
from validate import validate_row


def test_load_contents(db_file, expected_dir, this):
    return_status = 0
    for exp in glob.glob(expected_dir + "/*"):
        exp = realpath(exp)
        tmpname = "/tmp/{}_{}.{}".format(basename(this), basename(exp), os.getpid())
        with open(tmpname, "w") as tmp:
            select = "select * from `{}`".format(basename(exp))
            run(["sqlite3", db_file, select], stdout=tmp)
            status = run(["diff", "-q", exp, tmpname])
            if status.returncode != 0:
                actual = realpath("{}_actual".format(basename(exp)))
                os.rename(tmpname, actual)
                print(
                    "The actual contents of {} are not as expected. Saving them in {}".format(
                        basename(exp), actual
                    ),
                    file=sys.stderr,
                )
                return_status = 1
            else:
                os.unlink(tmpname)
    return return_status


def test_validate_and_update_row(config):
    row = {
        "id": {"messages": [], "valid": True, "value": "ZOB:0000013"},
        "label": {"messages": [], "valid": True, "value": "bar"},
        "parent": {"messages": [], "valid": True, "value": "car"},
        "source": {"messages": [], "valid": True, "value": "ZOB"},
        "type": {"messages": [], "valid": True, "value": "owl:Class"},
    }

    expected_row = {
        "id": {"messages": [], "valid": True, "value": "ZOB:0000013"},
        "label": {"messages": [], "valid": True, "value": "bar"},
        "parent": {
            "messages": [
                {
                    "rule": "tree:cycle",
                    "level": "error",
                    "message": "Cyclic dependency: (label: car, parent: foo), (label: foo, parent: bar), (label: bar, parent: None), (label: bar, parent: car) for tree(parent) of label",
                }
            ],
            "valid": False,
            "value": "car",
        },
        "source": {
            "messages": [
                {
                    "rule": "key:foreign",
                    "level": "error",
                    "message": "Value ZOB of column source is not in prefix.prefix",
                }
            ],
            "valid": False,
            "value": "ZOB",
        },
        "type": {"messages": [], "valid": True, "value": "owl:Class"},
        "duplicate": False,
    }

    actual_row = validate_row(config, "import", row, is_existing_row=True)
    if actual_row != expected_row:
        print(
            "Actual result of validate_row() differs from expected.\nActual:\n{}\n\nExpected:\n{}".format(
                pformat(actual_row), pformat(expected_row)
            )
        )
        return 1

    # We happen to know that this is the 7th row in the table. If we change the test data this may change.
    update_row(config, "import", row, 7)
    actual_row = config["db"].execute("SELECT * FROM import WHERE rowid = 7").fetchall()[0]
    expected_row = (
        None,
        'json({"messages": [{"rule": "key:foreign", "level": "error", "message": "Value ZOB of column source is not in prefix.prefix"}], "valid": false, "value": "ZOB"})',
        "ZOB:0000013",
        'json({"messages": [], "valid": true})',
        "bar",
        'json({"messages": [], "valid": true})',
        "owl:Class",
        'json({"messages": [], "valid": true})',
        None,
        'json({"messages": [{"rule": "tree:cycle", "level": "error", "message": "Cyclic dependency: (label: car, parent: foo), (label: foo, parent: bar), (label: bar, parent: None), (label: bar, parent: car) for tree(parent) of label"}], "valid": false, "value": "car"})',
    )
    if actual_row != expected_row:
        print(
            "Actual result of update_row() differs from expected.\nActual:\n{}\n\nExpected:\n{}".format(
                pformat(actual_row, width=500), pformat(expected_row, width=500)
            )
        )
        return 1
    return 0


def main():
    p = ArgumentParser()
    p.add_argument("db_file", help="The name of the database file to use for testing")
    p.add_argument(
        "expected_dir", help="The directory where the files with expected contents are located"
    )
    args = p.parse_args()
    db_file = args.db_file
    expected_dir = args.expected_dir
    this = __file__
    assert isdir(expected_dir)
    if isfile(db_file):
        os.unlink(db_file)

    config = read_config_files("src/table.tsv")
    with sqlite3.connect("build/cmi-pb.db") as conn:
        config["db"] = conn
        config["parser"] = Lark(grammar, parser="lalr", transformer=TreeToDict())
        config["constraints"] = {
            "foreign": {},
            "unique": {},
            "primary": {},
            "tree": {},
            "under": {},
        }
        old_stdout = sys.stdout
        with open(os.devnull, "w") as black_hole:
            sys.stdout = black_hole
            create_db_and_write_sql(config)
            sys.stdout = old_stdout

    ret = test_load_contents(db_file, expected_dir, this)
    if ret != 0:
        sys.exit(ret)

    ret = test_validate_and_update_row(config)
    if ret != 0:
        sys.exit(ret)


if __name__ == "__main__":
    main()
