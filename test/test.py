#!/usr/bin/env python3

import glob
import os
import sqlite3
import sys

from argparse import ArgumentParser
from lark import Lark
from os.path import basename, isfile, realpath
from pprint import pformat
from subprocess import DEVNULL, run

pwd = os.path.dirname(os.path.realpath(__file__))
sys.path.append("{}/../src/script".format(pwd))

from load import grammar, TreeToDict, read_config_files, create_db_and_write_sql, update_row
from export import export_data, export_messages
from validate import validate_existing_row


def test_load_contents(db_file, this_script):
    expected_dir = f"{pwd}/expected"
    return_status = 0
    for exp in glob.glob(expected_dir + "/*.load"):
        exp = realpath(exp)
        tmpname = "/tmp/{}_{}.{}".format(basename(this_script), basename(exp), os.getpid())
        with open(tmpname, "w") as tmp:
            select = "select * from `{}`".format(basename(exp.removesuffix(".load")))
            run(["sqlite3", db_file, select], stdout=tmp)
            status = run(["diff", "-q", exp, tmpname], stdout=DEVNULL)
            if status.returncode != 0:
                actual = realpath("{}/output/{}_actual".format(pwd, basename(exp)))
                os.rename(tmpname, actual)
                print(
                    "The loaded contents of {} are not as expected. Saving them in {}".format(
                        basename(exp), actual
                    ),
                    file=sys.stderr,
                )
                return_status = 1
            else:
                os.unlink(tmpname)
    return return_status


def test_export(db_file):
    output_dir = f"{pwd}/output"
    expected_dir = f"{pwd}/expected"
    return_status = 0
    for table in glob.glob(expected_dir + "/*.export"):
        table = basename(table.removesuffix(".export"))
        export_data({"db": db_file, "output_dir": output_dir, "tables": [table]})
        expected = f"{expected_dir}/{table}.export"
        actual = f"{output_dir}/{table}.export_actual"
        os.rename(f"{output_dir}/{table}.tsv", actual)
        status = run(["diff", "-q", expected, actual], stdout=DEVNULL)
        if status.returncode != 0:
            print(
                "The exported contents of {} are not as expected. Saving them in {}".format(
                    table, actual
                ),
                file=sys.stderr,
            )
            return_status = 1
        else:
            os.unlink(actual)
    return return_status


def test_messages(db_file):
    output_dir = f"{pwd}/output"
    expected_dir = f"{pwd}/expected"
    return_status = 0

    export_messages(
        {
            "db": db_file,
            "output_dir": output_dir,
            "tables": ["prefix", "import", "foobar"],
            "a1": False,
        }
    )
    expected = f"{expected_dir}/messages_non_a1.tsv"
    actual = f"{output_dir}/messages_non_a1.tsv"
    os.rename(f"{output_dir}/messages.tsv", actual)
    status = run(["diff", "-q", expected, actual], stdout=DEVNULL)
    if status.returncode != 0:
        print(
            f"Exported contents of messages_non_a1.tsv not as expected. Saving them in {actual}",
            file=sys.stderr,
        )
        return_status = 1
    else:
        os.unlink(actual)

    export_messages(
        {
            "db": db_file,
            "output_dir": output_dir,
            "tables": ["prefix", "import", "foobar"],
            "a1": True,
        }
    )
    expected = f"{expected_dir}/messages_a1.tsv"
    actual = f"{output_dir}/messages_a1.tsv"
    os.rename(f"{output_dir}/messages.tsv", actual)
    status = run(["diff", "-q", expected, actual], stdout=DEVNULL)
    if status.returncode != 0:
        print(
            f"Exported contents of messages_a1.tsv are not as expected. Saving them in {actual}",
            file=sys.stderr,
        )
        return_status = 1
    else:
        os.unlink(actual)

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
        "parent": {"messages": [], "valid": True, "value": "car"},
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
    }

    actual_row = validate_existing_row(config, "import", row, row_number=2)
    if actual_row != expected_row:
        print(
            "Actual result of validate_existing_row() differs from expected.\nActual:\n{}\n\nExpected:\n{}".format(
                pformat(actual_row), pformat(expected_row)
            )
        )
        return 1

    # We happen to know that this is the 2nd row in the table. If we change the test data this may change.
    update_row(config, "import", row, 2)
    actual_row = config["db"].execute("SELECT * FROM import WHERE row_number = 2").fetchall()[0]
    expected_row = (
        2,
        None,
        'json({"messages": [{"rule": "key:foreign", "level": "error", "message": "Value ZOB of column source is not in prefix.prefix"}], "valid": false, "value": "ZOB"})',
        "ZOB:0000013",
        'json({"messages": [], "valid": true})',
        "bar",
        'json({"messages": [], "valid": true})',
        "owl:Class",
        'json({"messages": [], "valid": true})',
        "car",
        'json({"messages": [], "valid": true})',
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
    args = p.parse_args()
    db_file = args.db_file
    this_script = __file__
    if isfile(db_file):
        os.unlink(db_file)

    config = read_config_files(
        "test/src/table.tsv", Lark(grammar, parser="lalr", transformer=TreeToDict())
    )
    with sqlite3.connect(db_file) as conn:
        config["db"] = conn
        config["db"].execute("PRAGMA foreign_keys = ON")
        old_stdout = sys.stdout
        with open(os.devnull, "w") as black_hole:
            sys.stdout = black_hole
            create_db_and_write_sql(config)
            sys.stdout = old_stdout

    ret = test_load_contents(db_file, this_script)
    ret += test_export(db_file)
    ret += test_messages(db_file)
    ret += test_validate_and_update_row(config)
    sys.exit(ret)


if __name__ == "__main__":
    main()
