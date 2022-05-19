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

from cmi_pb_script.load import (
    grammar,
    TreeToDict,
    read_config_files,
    configure_and_load_db,
    update_row,
    insert_new_row,
)
from cmi_pb_script.export import export_data, export_messages
from cmi_pb_script.validate import validate_row, get_matching_values


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


def test_validate_and_insert_new_row(config):
    row = {
        "id": {"messages": [], "valid": True, "value": "BFO:0000027"},
        "label": {"messages": [], "valid": True, "value": "car"},
        "parent": {
            "messages": [
                {"level": "error", "message": "An unrelated error", "rule": "custom:unrelated"}
            ],
            "valid": False,
            "value": "barrie",
        },
        "source": {"messages": [], "valid": True, "value": "BFOBBER"},
        "type": {"messages": [], "valid": True, "value": "owl:Class"},
    }
    # The result of the validation should be identical to the original row since there are no
    # problems with it:
    expected_row = {
        "id": {"messages": [], "valid": True, "value": "BFO:0000027"},
        "label": {
            "messages": [
                {
                    "level": "error",
                    "message": "Values of label must be unique",
                    "rule": "key:primary",
                },
                {
                    "level": "error",
                    "message": "Values of label must be unique",
                    "rule": "tree:child-unique",
                },
            ],
            "valid": False,
            "value": "car",
        },
        "parent": {
            "messages": [
                {"level": "error", "message": "An unrelated error", "rule": "custom:unrelated"},
                {
                    "level": "error",
                    "message": "Value barrie of column parent is not in " "column label",
                    "rule": "tree:foreign",
                },
            ],
            "valid": False,
            "value": "barrie",
        },
        "source": {
            "messages": [
                {
                    "level": "error",
                    "message": "Value BFOBBER of column source is not in " "prefix.prefix",
                    "rule": "key:foreign",
                }
            ],
            "valid": False,
            "value": "BFOBBER",
        },
        "type": {"messages": [], "valid": True, "value": "owl:Class"},
    }
    actual_row = validate_row(config, "import", row)
    if actual_row != expected_row:
        print(
            "Actual result of validate_row() differs from expected.\n"
            + "Actual:\n{}\n\nExpected:\n{}".format(pformat(actual_row), pformat(expected_row))
        )
        return 1

    expected_new_row_num = (
        config["db"].execute("SELECT MAX(`row_number`) FROM `import`").fetchall()[0][0]
    )
    expected_new_row_num = 1 if expected_new_row_num is None else expected_new_row_num + 1
    actual_new_row_num = insert_new_row(config, "import", row)
    if actual_new_row_num != expected_new_row_num:
        print(
            "New row number: {} does not match expected new row number: {}".format(
                actual_new_row_num, expected_new_row_num
            )
        )
        return 1

    actual_row = (
        config["db"]
        .execute(f"SELECT * FROM `import` WHERE `row_number` = {actual_new_row_num}")
        .fetchall()[0]
    )
    expected_row = (
        10,
        None,
        '{"messages":[{"rule":"key:foreign","level":"error","message":"Value BFOBBER of column source is not in prefix.prefix"}],"valid":false,"value":"BFOBBER"}',
        "BFO:0000027",
        None,
        None,
        '{"messages":[{"rule":"key:primary","level":"error","message":"Values of label must be unique"},{"rule":"tree:child-unique","level":"error","message":"Values of label must be unique"}],"valid":false,"value":"car"}',
        "owl:Class",
        None,
        None,
        '{"messages":[{"level":"error","message":"An unrelated error","rule":"custom:unrelated"},{"rule":"tree:foreign","level":"error","message":"Value barrie of column parent is not in column label"}],"valid":false,"value":"barrie"}',
    )
    if actual_row != expected_row:
        print(
            "Actual result of insert_new_row() differs from expected.\n"
            + "Actual:\n{}\n\nExpected:\n{}".format(
                pformat(actual_row, width=500), pformat(expected_row, width=500)
            )
        )
        return 1

    return 0


def test_validate_and_update_row(config):
    row = {
        "child": {"messages": [], "valid": True, "value": "b"},
        "parent": {"messages": [], "valid": True, "value": "f"},
        "xyzzy": {"messages": [], "valid": True, "value": "w"},
        "foo": {"messages": [], "valid": True, "value": "A"},
        "bar": {
            "messages": [
                {"level": "error", "message": "An unrelated error", "rule": "custom:unrelated"}
            ],
            "valid": False,
            "value": "B",
        },
    }

    expected_row = {
        "bar": {
            "messages": [
                {"level": "error", "message": "An unrelated error", "rule": "custom:unrelated"}
            ],
            "valid": False,
            "value": "B",
        },
        "child": {
            "messages": [
                {
                    "level": "error",
                    "message": "Values of child must be unique",
                    "rule": "tree:child-unique",
                }
            ],
            "valid": False,
            "value": "b",
        },
        "foo": {"messages": [], "valid": True, "value": "A"},
        "parent": {"messages": [], "valid": True, "value": "f"},
        "xyzzy": {
            "messages": [
                {
                    "level": "error",
                    "message": "Value w of column xyzzy is not in " "foobar.child",
                    "rule": "under:not-in-tree",
                }
            ],
            "valid": False,
            "value": "w",
        },
    }

    actual_row = validate_row(config, "foobar", row, True, row_number=1)
    if actual_row != expected_row:
        print(
            "Actual result of validate_row() differs from expected.\n"
            + "Actual:\n{}\n\nExpected:\n{}".format(pformat(actual_row), pformat(expected_row))
        )
        return 1

    # We happen to know that this is the 2nd row in the table. If we change the test data this may
    # change.
    update_row(config, "foobar", row, 1)
    actual_row = config["db"].execute("SELECT * FROM `foobar` WHERE `row_number` = 1").fetchall()[0]
    expected_row = (
        1,
        None,
        '{"messages":[{"rule":"tree:child-unique","level":"error","message":"Values of child must be unique"}],"valid":false,"value":"b"}',
        "f",
        None,
        None,
        '{"messages":[{"rule":"under:not-in-tree","level":"error","message":"Value w of column xyzzy is not in foobar.child"}],"valid":false,"value":"w"}',
        "A",
        None,
        None,
        '{"messages":[{"level":"error","message":"An unrelated error","rule":"custom:unrelated"}],"valid":false,"value":"B"}',
    )
    if actual_row != expected_row:
        print(
            "Actual result of update_row() differs from expected.\n"
            + "Actual:\n{}\n\nExpected:\n{}".format(pformat(actual_row), pformat(expected_row))
        )
        return 1
    return 0


def test_auto_complete(config):
    actual = get_matching_values(config, "foobar", "parent", "b")
    expected = [{"id": "b", "label": "b", "order": 1}]
    if actual != expected:
        print(f"Actual auto_complete values: {actual} do not match the expected values: {expected}")
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
            configure_and_load_db(config)
            sys.stdout = old_stdout

    ret = test_load_contents(db_file, this_script)
    ret += test_export(db_file)
    ret += test_messages(db_file)
    ret += test_validate_and_update_row(config)
    ret += test_validate_and_insert_new_row(config)
    ret += test_auto_complete(config)
    sys.exit(ret)


if __name__ == "__main__":
    main()
