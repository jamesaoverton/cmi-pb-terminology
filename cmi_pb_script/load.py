#!/usr/bin/env python3

import csv
import itertools
import json
import re
import sqlite3
import sys

from argparse import ArgumentParser
from graphlib import CycleError, TopologicalSorter
from collections import OrderedDict
from lark import Lark
from lark.exceptions import VisitError
from multiprocessing import cpu_count, Manager, Process

try:
    from .sql_utils import safe_sql
    from .cmi_pb_grammar import grammar, TreeToDict
    from .validate import (
        validate_rows_intra,
        validate_rows_trees,
        validate_rows_constraints,
        validate_tree_foreign_keys,
        validate_under,
    )
except ImportError:
    from sql_utils import safe_sql
    from cmi_pb_grammar import grammar, TreeToDict
    from validate import (
        validate_rows_intra,
        validate_rows_trees,
        validate_rows_constraints,
        validate_tree_foreign_keys,
        validate_under,
    )

CHUNK_SIZE = 300
MULTIPROCESSING = False
DEFAULT_CPU_COUNT = 4

# TODO include synonyms?
sqlite_types = ["text", "integer", "real", "blob"]


class ConfigError(Exception):
    pass


class TSVReadError(Exception):
    pass


def read_config_files(table_table_path, condition_parser):
    """Given the path to a table TSV file, load and check the special 'table', 'column', and
    'datatype' tables, and return a config structure."""

    def read_tsv(path):
        """Given a path, read a TSV file and return a list of row dicts."""
        with open(path) as f:
            rows = csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
            rows = list(rows)
            if len(rows) < 1:
                raise TSVReadError(f"No rows in {path}")
            return rows

    def compile_condition(condition):
        """Given a datatype condition, parse it using the configured parser and pre-compile the
        regular expression and function corresponding to it. Return both the parsed expression and
        the re-compiled regular expression."""

        # "null" and "not null" conditions do not get assigned a condition but are dealt
        # with specially. We also return Nones if the incoming condition is None.
        if condition in [None, "null", "not null"]:
            return None, None

        parsed_condition = config["parser"].parse(condition)
        if len(parsed_condition) != 1:
            raise ConfigError(
                f"Condition: '{condition}' is invalid. Only one condition per column is allowed."
            )

        parsed_condition = parsed_condition[0]
        if parsed_condition["type"] == "function" and parsed_condition["name"] == "equals":
            expected = re.sub(r"^['\"](.*)['\"]$", r"\1", parsed_condition["args"][0]["value"])
            return parsed_condition, lambda x: x == expected
        elif parsed_condition["type"] == "function" and parsed_condition["name"] in (
            "exclude",
            "match",
            "search",
        ):
            pattern = re.sub(r"^['\"](.*)['\"]$", r"\1", parsed_condition["args"][0]["pattern"])
            flags = parsed_condition["args"][0]["flags"]
            flags = "(?" + "".join(flags) + ")" if flags else ""
            pattern = re.compile(flags + pattern)
            if parsed_condition["name"] == "exclude":
                return parsed_condition, lambda x: not bool(pattern.search(x))
            elif parsed_condition["name"] == "match":
                return parsed_condition, lambda x: bool(pattern.fullmatch(x))
            else:
                return parsed_condition, lambda x: bool(pattern.search(x))
        elif parsed_condition["type"] == "function" and parsed_condition["name"] == "in":
            alternatives = [
                re.sub(r"^['\"](.*)['\"]$", r"\1", arg["value"]) for arg in parsed_condition["args"]
            ]
            return parsed_condition, lambda x: x in alternatives
        elif (
            parsed_condition["type"] != "function"
            and parsed_condition["value"] in config["datatype"]
        ):
            # If the condition is a recognized (thus, already compiled) datatype, just return that:
            condition_name = parsed_condition["value"]
            return (
                config["datatype"][condition_name]["parsed_condition"],
                config["datatype"][condition_name]["compiled_condition"],
            )
        else:
            raise ConfigError(f"Unrecognized condition: {condition}")

    config = {
        "table": {},
        "datatype": {},
        "special": {},
        "parser": condition_parser,
        "rule": {},
        "constraints": {"foreign": {}, "unique": {}, "primary": {}, "tree": {}, "under": {},},
    }

    special_table_types = {
        "table": {"required": True},
        "column": {"required": True},
        "datatype": {"required": True},
        "rule": {"required": False},
        "index": {"required": False}
    }
    path = table_table_path
    rows = read_tsv(path)

    for t in special_table_types:
        config["special"][t] = None

    # Load table table
    for row in rows:
        for column in ["table", "path", "type"]:
            if column not in row or row[column] is None:
                raise ConfigError(f"Missing required column '{column}' reading '{path}'")
        for column in ["table", "path"]:
            if row[column].strip() == "":
                raise ConfigError(f"Missing required value for '{column}' reading '{path}'")
        for column in ["type"]:
            if row[column].strip() == "":
                row[column] = None
        if row["type"] == "table":
            if row["path"] != path:
                raise ConfigError(
                    "Special 'table' path '{}' does not match this path '{}'".format(
                        row["path"], path
                    )
                )
        if row["type"] in special_table_types:
            if config["special"][row["type"]]:
                raise ConfigError(
                    "Multiple tables with type '{}' declared in '{}'".format(row["type"], path)
                )
            config["special"][row["type"]] = row["table"]
        if row["type"] and row["type"] not in special_table_types:
            raise ConfigError("Unrecognized table type '{}' in '{}'".format(row["type"], path))
        row["column"] = {}
        config["table"][row["table"]] = row

    for table_type, table_spec in special_table_types.items():
        if table_spec["required"] and config["special"][table_type] is None:
            raise ConfigError(f"Missing required '{table_type}' table in '{path}'")

    # Load datatype table
    table_name = config["special"]["datatype"]
    path = config["table"][table_name]["path"]
    rows = read_tsv(path)
    for row in rows:
        for column in ["datatype", "parent", "condition", "SQL type"]:
            if column not in row or row[column] is None:
                raise ConfigError(f"Missing required column '{column}' reading '{path}'")
        for column in ["datatype"]:
            if row[column].strip() == "":
                raise ConfigError(f"Missing required value for '{column}' reading '{path}'")
        for column in ["parent", "condition", "SQL type"]:
            if row[column].strip() == "":
                row[column] = None
        config["datatype"][row["datatype"]] = row
        condition = config["datatype"][row["datatype"]]["condition"]
        parsed_condition, compiled_condition = compile_condition(condition)
        config["datatype"][row["datatype"]]["compiled_condition"] = compiled_condition
        config["datatype"][row["datatype"]]["parsed_condition"] = parsed_condition

    for dt in ["text", "empty", "line", "word"]:
        if dt not in config["datatype"]:
            raise ConfigError(f"Missing required datatype: '{dt}'")

    # Load column table
    table_name = config["special"]["column"]
    path = config["table"][table_name]["path"]
    rows = read_tsv(path)
    for row in rows:
        for column in ["table", "column", "nulltype", "datatype"]:
            if column not in row or row[column] is None:
                raise ConfigError(f"Missing required column '{column}' reading '{path}'")
        for column in ["table", "column", "datatype"]:
            if row[column].strip() == "":
                raise ConfigError(f"Missing required value for '{column}' reading '{path}'")
        for column in ["nulltype"]:
            if row[column].strip() == "":
                row[column] = None
        if row["table"] not in config["table"]:
            raise ConfigError("Undefined table '{}' reading '{}'".format(row["table"], path))
        if row["nulltype"] and row["nulltype"] not in config["datatype"]:
            raise ConfigError("Undefined nulltype '{}' reading '{}'".format(row["nulltype"], path))
        if row["datatype"] not in config["datatype"]:
            raise ConfigError("Undefined datatype '{}' reading '{}'".format(row["datatype"], path))
        if row["structure"]:
            try:
                row["parsed_structure"] = config["parser"].parse(row["structure"])[0]
            except Exception as e:
                raise ConfigError(
                    "While parsing structure: '{}' for column: '{}.{}' got error:\n{}".format(
                        row["structure"], row["table"], row["column"], e
                    )
                ) from None
        else:
            row["parsed_structure"] = None
        config["table"][row["table"]]["column"][row["column"]] = row

    # Load rule table if it exists
    table_name = config["special"].get("rule")
    if table_name:
        path = config["table"][table_name]["path"]
        rows = read_tsv(path)
        for row in rows:
            for column in [
                "table",
                "when column",
                "when condition",
                "then column",
                "then condition",
                "level",
                "description",
            ]:
                if column not in row or row[column] is None:
                    raise ConfigError(f"Missing required column '{column}' reading '{path}'")
                if row[column].strip() == "":
                    raise ConfigError(f"Missing required value for '{column}' reading '{path}'")

            if row["table"] not in config["table"]:
                raise ConfigError("Undefined table '{}' reading '{}'".format(row["table"], path))

            for column in ["when column", "then column"]:
                if row[column] not in config["table"][row["table"]]["column"]:
                    raise ConfigError(
                        "Undefined column '{}.{}' reading '{}'".format(
                            row["table"], row[column], path
                        )
                    )

            # Compile the when and then conditions:
            for column in ["when condition", "then condition"]:
                parsed_condition, compiled_condition = compile_condition(row[column])
                row[f"parsed {column}"] = parsed_condition
                row[f"compiled {column}"] = compiled_condition

            # Add the rule specified in the given row to the list of rules associated with the
            # value of the when column:
            if row["table"] not in config["rule"]:
                config["rule"][row["table"]] = {}
            if row["when column"] not in config["rule"][row["table"]]:
                config["rule"][row["table"]][row["when column"]] = []
            config["rule"][row["table"]][row["when column"]].append(row)

    return config


def verify_table_deps_and_sort(table_list, constraints):
    """Takes as arguments a list of tables and a dictionary describing all of the constraints
    between tables. The dictionary should include a sub-dictionary of foreign key constraints,
    a sub-dictionary of tree constraints, and a sub-dictionary of under constraints. The first
    should be of the form:
    {'my_table': [{'column': 'my_column', 'fcolumn': 'foreign_column', 'ftable': 'foreign_table'},
                  ...]
     ...}.
    The second should be of the form:
    {'my_table': [{'child': 'my_child', 'parent': 'my_parent'},
                  ...],
     ...}.
    The third should be of the form:
    {'my_table': [{'column': 'my_column', 'ttable': 'tree_table', 'tcolumn': 'tree_column',
                   'value': 'under_value'}
                  ...],
     ...}.

    After validating that there are no cycles amongst the foreign, tree, and under dependencies,
    returns the list of tables sorted according to their foreign key dependencies, such that if
    table_a depends on table_b, then table_b comes before table_a in the list."""

    trees = constraints["tree"]
    for table_name in table_list:
        ts = TopologicalSorter()
        for tree in trees[table_name]:
            ts.add(tree["child"], tree["parent"])
        try:
            list(ts.static_order())
        except CycleError as e:
            cycle = e.args[1]
            message = "Cyclic tree dependency in table '{}': ".format(table_name)
            end_index = len(cycle) - 1
            for i, child in enumerate(cycle):
                if i < end_index:
                    dep_name = cycle[i + 1]
                    dep = [d for d in trees[table_name] if d["child"] == child].pop()
                    message += "tree({}) references {}".format(child, dep["parent"])
                if i < (end_index - 1):
                    message += " and "
            raise CycleError(message)

    foreign_keys = constraints["foreign"]
    under_keys = constraints["under"]
    ts = TopologicalSorter()
    for table_name in table_list:
        deps = set(dep["ftable"] for dep in foreign_keys.get(table_name, []))
        ts.add(table_name, *deps)
        for ukey in under_keys.get(table_name, []):
            if ukey["ttable"] != table_name:
                if not [
                    tkey
                    for tkey in constraints["tree"][ukey["ttable"]]
                    if tkey["child"] == ukey["tcolumn"]
                ]:
                    raise ConfigError(
                        "under({}.{}, {}) refers to a non-existent tree.".format(
                            ukey["ttable"], ukey["tcolumn"], ukey["value"]
                        )
                    )
                ts.add(table_name, ukey["ttable"])

    try:
        return list(ts.static_order())
    except CycleError as e:
        cycle = e.args[1]
        message = "Cyclic dependency between tables {}: ".format(", ".join(cycle))
        end_index = len(cycle) - 1
        for i, table in enumerate(cycle):
            if i < end_index:
                dep_name = cycle[i + 1]
                dep = (
                    [d for d in foreign_keys[table] if d["ftable"] == dep_name]
                    or [d for d in under_keys[table] if d["ttable"] == dep_name]
                ).pop()
                message += "{}.{} depends on {}.{}".format(
                    table,
                    dep["column"],
                    dep.get("ftable") or dep.get("ttable"),
                    dep.get("fcolumn") or dep.get("tcolumn"),
                )
            if i < (end_index - 1):
                message += " and "
        raise CycleError(message) from None


def validate_and_insert_chunks(config, table_name, chunks):
    """Given a configuration map, a table name, and a number of chunks of rows to insert into the
    table in the database, validate each chunk and insert the validated rows to the table."""

    def validate_rows_inter_and_insert(intra_validated_rows, chunk_num):
        # First do the tree validation:
        validated_rows = validate_rows_trees(config, table_name, intra_validated_rows)
        # Try to insert the rows to the db without first validating unique and foreign
        # constraints. If there are constraint violations this will cause the db to
        # raise an IntegrityError, in which case we then explicitly do the constraint
        # validation and insert the resulting rows:
        main, conflict = make_inserts(config, table_name, validated_rows, chunk_num)
        try:
            config["db"].execute(main)
        except sqlite3.IntegrityError:
            validated_rows = validate_rows_constraints(config, table_name, intra_validated_rows)
            main, conflict = make_inserts(config, table_name, validated_rows, chunk_num)
            config["db"].execute(main)
            config["db"].execute(conflict)
            config["db"].commit()
            print("{}\n".format(main))
            print("{}\n".format(conflict))

    # Determine the number of CPUs on this system, which we will use below to determine the number
    # of worker processes to fork at a time:
    if MULTIPROCESSING:
        try:
            num_cpus = cpu_count()
        except NotImplementedError:
            num_cpus = DEFAULT_CPU_COUNT

        # Initialize a manager with an associated dictionary which we will use to accumulate
        # the intra-row validation results from each worker process. Each process is assigned
        # one chunk of data to work on.
        manager = Manager()
        results = manager.dict()
        procs = []
        for chunk_number, chunk in enumerate(chunks):
            chunk = filter(None, chunk)
            proc = Process(
                target=validate_rows_intra, args=(config, table_name, chunk, chunk_number, results),
            )
            procs.append(proc)

        # Run only as many worker processes at a time as there are CPUs, progressively working
        # through the data chunks:
        proc_blocks = itertools.zip_longest(*([iter(procs)] * num_cpus))
        for i, proc_block in enumerate(proc_blocks):
            proc_block = list(filter(None, proc_block))
            for proc in proc_block:
                proc.start()

            for proc in proc_block:
                proc.join()
                proc.close()

            # Once the intra-row validation is done, do inter-row validation on all of
            # the chunks assigned to this process block, one at a time:
            proc_block_results = OrderedDict(sorted(results.items()))
            results.clear()
            for chunk_number, intra_validated_rows in proc_block_results.items():
                validate_rows_inter_and_insert(intra_validated_rows, chunk_number)
    else:
        results = OrderedDict()
        for chunk_number, chunk in enumerate(chunks):
            chunk = filter(None, chunk)
            intra_results = validate_rows_intra(config, table_name, chunk, chunk_number, results)
            validate_rows_inter_and_insert(intra_results, chunk_number)


def configure_db(config, write_sql_to_stdout=False, write_to_db=False):
    """Given a config map, read in the TSV files corresponding to the tables defined in the config,
    and use that information to fill in table-specific info in the config map. If the flag
    `write_sql_to_stdout` is set to True, emit SQL to create the database schema to STDOUT. If the
    flag `write_to_db` is set to True, execute the SQL in the database, whose connection is
    given in `config` under the "db" key."""
    # Begin by reading in the TSV files corresponding to the tables defined in config, and use
    # that information to create the associated database tables, while saving constraint information
    # to the config map.
    for table_name in list(config["table"]):
        path = config["table"][table_name]["path"]
        with open(path) as f:
            # Open a DictReader to get the first row from which we will read the column names of the
            # table. Note that although we discard the rest this should not be inefficient. Since
            # DictReader is implemented as an Iterator it does not read the whole file.
            rows = csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE)

            # Update columns
            defined_columns = config["table"][table_name]["column"]
            try:
                actual_columns = next(rows)
            except StopIteration:
                # Handle empty placeholder files - these only have the headers
                try:
                    f.close()
                    with open(path, "r") as f2:
                        rows = csv.reader(f2, delimiter="\t", quoting=csv.QUOTE_NONE)
                        actual_columns = next(rows)
                except StopIteration:
                    raise StopIteration(f"No rows in {path}") from None

            all_columns = {}
            for column_name in actual_columns:
                column = {
                    "table": table_name,
                    "column": column_name,
                    "nulltype": "empty",
                    "datatype": "text",
                }
                if column_name in defined_columns:
                    column = defined_columns[column_name]
                all_columns[column_name] = column
            config["table"][table_name]["column"] = all_columns

            # Create the table and its corresponding conflict table:
            for table in [table_name, table_name + "_conflict"]:
                table_sql, table_constraints = create_table(config, table)
                if not table.endswith("_conflict"):
                    config["constraints"]["foreign"][table_name] = table_constraints["foreign"]
                    config["constraints"]["unique"][table_name] = table_constraints["unique"]
                    config["constraints"]["primary"][table_name] = table_constraints["primary"]
                    config["constraints"]["tree"][table_name] = table_constraints["tree"]
                    config["constraints"]["under"][table_name] = table_constraints["under"]
                if write_to_db:
                    config["db"].executescript(table_sql)
                if write_sql_to_stdout:
                    print("{}\n".format(table_sql))

            # Create a view as the union of the regular and conflict versions of the table:
            sql = "DROP VIEW IF EXISTS `{}`;\n".format(table_name + "_view")
            sql += "CREATE VIEW `{}` AS SELECT * FROM `{}` UNION SELECT * FROM `{}`;".format(
                table_name + "_view", table_name, table_name + "_conflict"
            )
            if write_sql_to_stdout:
                print("{}\n".format(sql))
            if write_to_db:
                config["db"].executescript(sql)
                config["db"].commit()


def load_db(config):
    """Given a config map, read in the data TSV files corresponding to each configured table,
    then validate and load all of the corresponding rows."""
    # Sort tables according to their foreign key dependencies so that tables are always loaded
    # after the tables they depend on:
    table_list = list(config["table"])
    table_list = verify_table_deps_and_sort(table_list, config["constraints"])

    # Now load the rows:
    for table_name in table_list:
        path = config["table"][table_name]["path"]
        with open(path) as f:
            rows = csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
            # Collect data into fixed-length chunks or blocks
            # See: https://docs.python.org/3.9/library/itertools.html#itertools-recipes
            chunks = itertools.zip_longest(*([iter(rows)] * CHUNK_SIZE))
            validate_and_insert_chunks(config, table_name, chunks)

        # We need to wait until all of the rows for a table have been loaded before validating the
        # "foreign" constraints on a table's trees, since this checks if the values of one column
        # (the tree's parent) are all contained in another column (the tree's child):
        # We also need to wait before validating a table's "under" constraints. Although the teee
        # associated with such a constraint need not be defined on the same table, it can be.
        records_to_update = validate_tree_foreign_keys(config, table_name) + validate_under(
            config, table_name
        )
        for record in records_to_update:
            table, column, meta = table_name, record["column"], record["column"] + "_meta"
            sql = safe_sql(
                f"UPDATE `{table}` SET `{column}` = NULL, `{meta}` = JSON(:mval) "
                f"WHERE `row_number` = :row_number;",
                {
                    "mval": "{}".format(json.dumps(record["meta"])),
                    "row_number": record["row_number"],
                },
            )
            config["db"].execute(sql)
        config["db"].commit()


def configure_and_load_db(config):
    """Given a config map, read the TSVs corresponding to the various defined tables, then create
    a database containing those tables and write the data from the TSVs to them, all the while
    writing the SQL strings used to generate the database to STDOUT."""
    configure_db(config, write_sql_to_stdout=True, write_to_db=True)
    load_db(config)


def get_SQL_type(config, datatype):
    """Given the config map and the name of a datatype, climb the datatype tree (as required),
    and return the first 'SQL type' found."""
    if "datatype" not in config:
        raise ConfigError("Missing datatypes in config")
    if datatype not in config["datatype"]:
        return None
    if config["datatype"][datatype]["SQL type"]:
        return config["datatype"][datatype]["SQL type"]
    return get_SQL_type(config, config["datatype"][datatype]["parent"])


def create_table(config, table_name):
    """Given the config map and a table name, generate a SQL schema string, including each
    column C and its matching C_meta column, then return the schema string as well as a list of the
    table's constraints."""
    output = [
        f"DROP TABLE IF EXISTS `{table_name}`;",
        f"CREATE TABLE `{table_name}` (",
        "  `row_number` INTEGER,",
    ]
    columns = config["table"][table_name.replace("_conflict", "")]["column"]
    table_constraints = {"foreign": [], "unique": [], "primary": [], "tree": [], "under": []}
    c = len(columns.values())
    r = 0
    for row in columns.values():
        r += 1
        sql_type = get_SQL_type(config, row["datatype"])
        if not sql_type:
            raise ConfigError("Missing SQL type for {}".format(row["datatype"]))
        if not sql_type.lower() in sqlite_types:
            raise ConfigError("Unrecognized SQL type '{}' for {}".format(sql_type, row["datatype"]))
        column_name = row["column"]
        line = f"  `{column_name}` {sql_type}"
        structure = row.get("structure")
        if structure and not table_name.endswith("_conflict"):
            parser = config["parser"]
            for expression in parser.parse(structure):
                if expression["type"] == "label" and expression["value"] == "primary":
                    line += " PRIMARY KEY"
                    table_constraints["primary"].append(column_name)
                elif expression["type"] == "label" and expression["value"] == "unique":
                    line += " UNIQUE"
                    table_constraints["unique"].append(column_name)
                elif expression["type"] == "function" and expression["name"] == "from":
                    if len(expression["args"]) != 1 or expression["args"][0]["type"] != "field":
                        raise ConfigError(f"Invalid foreign key: {structure} for: {table_name}")
                    table_constraints["foreign"].append(
                        {
                            "column": column_name,
                            "ftable": expression["args"][0]["table"],
                            "fcolumn": expression["args"][0]["column"],
                        }
                    )
                elif expression["type"] == "function" and expression["name"] == "tree":
                    if len(expression["args"]) != 1 or expression["args"][0]["type"] != "label":
                        raise ConfigError(
                            f"Invalid 'tree' constraint: {structure} for: {table_name}"
                        )
                    child = expression["args"][0]["value"]
                    child_datatype = columns.get(child, {}).get("datatype")
                    if not child_datatype:
                        raise ConfigError(
                            f"Could not determine SQL datatype for {child} of tree({child})"
                        )
                    parent = column_name
                    child_sql_type = get_SQL_type(config, child_datatype)
                    if sql_type != child_sql_type:
                        raise ConfigError(
                            f"SQL type '{child_sql_type}' of '{child}' in 'tree({child})' for "
                            f"table '{table_name}' does not match SQL type: '{sql_type}' of "
                            f"parent: '{parent}'."
                        )
                    table_constraints["tree"].append({"parent": column_name, "child": child})
                elif expression["type"] == "function" and expression["name"] == "under":
                    if (
                        len(expression["args"]) != 2
                        or expression["args"][0]["type"] != "field"
                        or expression["args"][1]["type"] != "label"
                    ):
                        raise ConfigError(
                            f"Invalid 'under' constraint: {structure} for: {table_name}"
                        )
                    table_constraints["under"].append(
                        {
                            "column": column_name,
                            "ttable": expression["args"][0]["table"],
                            "tcolumn": expression["args"][0]["column"],
                            "value": expression["args"][1]["value"],
                        }
                    )
                else:
                    unparsed_condition = reverse_parse(config, expression)
                    raise ConfigError(
                        f"Unrecognized structure expression: {unparsed_condition} "
                        + f"for column: {table_name}.{column_name}"
                    )
        line += ","
        output.append(line)
        metacol = column_name + "_meta"
        line = f"  `{metacol}` TEXT"
        if r >= c and not table_constraints["foreign"]:
            line += ""
        else:
            line += ","
        output.append(line)

    num_fkeys = len(table_constraints["foreign"])
    for i, fkey in enumerate(table_constraints["foreign"]):
        output.append(
            "  FOREIGN KEY (`{}`) REFERENCES `{}`(`{}`){}".format(
                fkey["column"], fkey["ftable"], fkey["fcolumn"], "," if i < (num_fkeys - 1) else ""
            )
        )
    output.append(");")
    # Loop through the tree constraints and if any of their associated child columns do not already
    # have an associated unique or primary index, create one implicitly here:
    for i, tree in enumerate(table_constraints["tree"]):
        if tree["child"] not in (table_constraints["unique"] + table_constraints["primary"]):
            output.append(
                "CREATE UNIQUE INDEX `{}_{}_idx` ON `{}`(`{}`);".format(
                    table_name, tree["child"], table_name, tree["child"]
                )
            )
    # Finally, create a further unique index on row_number:
    output.append(
        f"CREATE UNIQUE INDEX `{table_name}_row_number_idx` ON `{table_name}`(`row_number`);"
    )
    return "\n".join(output), table_constraints


def make_inserts(config, table_name, rows, chunk_number):
    """Given a config map, a table name, and a list of rows (dicts from column names to column
    values), return a two-place tuple containing SQL strings for INSERT statement with VALUES for
    all the rows in the normal and conflict versions of the table, respectively."""

    def generate_sql(table_name, rows):
        lines = []
        for row in rows:
            values = [":row_number"]
            params = {"row_number": row["row_number"]}
            for column, cell in row.items():
                if column == "row_number":
                    continue
                cell = cell.copy()
                column = column.replace(" ", "_")
                value = None
                if "nulltype" in cell and cell["nulltype"]:
                    value = None
                elif cell["valid"]:
                    value = cell["value"]
                    cell.pop("value")
                values.append(f":{column}")
                values.append(f"JSON(:{column}_meta)")
                params[column] = value
                # If the cell value is valid and there is no extra information (e.g., nulltype),
                # then just set the metadata to None, which can be taken to represent a "plain"
                # valid cell:
                if cell["valid"] and all([k in ["value", "valid", "messages"] for k in cell]):
                    params[column + "_meta"] = None
                else:
                    params[column + "_meta"] = "{}".format(json.dumps(cell))
            line = ", ".join(values)
            line = f"({line})"
            lines.append(safe_sql(line, params))

        output = ""
        if lines:
            output += f"INSERT INTO `{table_name}` VALUES"
            output += "\n"
            output += ",\n".join(lines)
            output += ";"
        return output

    def has_conflict(row, conflict_columns):
        for col in row:
            if col in conflict_columns and not row[col]["valid"]:
                return True
        return False

    conflict_columns = set(
        config["constraints"]["primary"][table_name]
        + config["constraints"]["unique"][table_name]
        + [tree["child"] for tree in config["constraints"]["tree"][table_name]]
    )

    main_rows = []
    conflict_rows = []
    for i, row in enumerate(rows, start=1):
        row["row_number"] = i + chunk_number * CHUNK_SIZE
        if has_conflict(row, conflict_columns):
            conflict_rows.append(row)
        else:
            main_rows.append(row)
    return (
        generate_sql(table_name, main_rows),
        generate_sql(table_name + "_conflict", conflict_rows),
    )


def update_row(config, table_name, row, row_number):
    """Given a config map, a table name, a row (a dict from column names to column values), and the
    row_number to update, update the corresponding row in the database with new values as specified
    by `row`."""

    assignments = []
    params = {}
    for column, cell in row.items():
        variable = column.replace(" ", "_")
        value = None
        if "nulltype" in cell and cell["nulltype"]:
            value = None
        elif cell["valid"]:
            value = cell["value"]
            cell.pop("value")
        assignments.append(f"`{column}` = :{variable}")
        assignments.append(f"`{column}_meta` = JSON(:{variable}_meta)")
        params[variable] = value
        # If the cell value is valid and there is no extra information (e.g., nulltype), then
        # just set the metadata to None, which can be taken to represent a "plain" valid cell:
        if cell["valid"] and all([k in ["value", "valid", "messages"] for k in cell]):
            params[variable + "_meta"] = None
        else:
            params[variable + "_meta"] = "{}".format(json.dumps(cell))

    update_stmt = f"UPDATE `{table_name}` SET "
    update_stmt += safe_sql(", ".join(assignments), params)
    update_stmt += safe_sql(" WHERE `row_number` = :row_number", {"row_number": row_number})
    config["db"].execute(update_stmt).fetchall()
    config["db"].commit()


def insert_new_row(config, table_name, row):
    """Given a config map, a table name, and a row (a dict from column names to column values),
    assign a new row number to the row and insert it to the database. Returns the new row number."""

    new_row_number = (
        config["db"].execute(f"SELECT MAX(`row_number`) FROM `{table_name}`").fetchall()[0][0]
    )
    new_row_number = 1 if new_row_number is None else new_row_number + 1

    insert_columns = ["`row_number`"]
    insert_values = [":row_number"]
    insert_params = {"row_number": new_row_number}
    for column, cell in row.items():
        variable = column.replace(" ", "_")
        value = None
        if "nulltype" in cell and cell["nulltype"]:
            value = None
        elif cell["valid"]:
            value = cell["value"]
            cell.pop("value")
        insert_columns += [f"`{column}`", f"`{column}_meta`"]
        insert_values += [f":{variable}", f"JSON(:{variable}_meta)"]
        insert_params[variable] = value
        # If the cell value is valid and there is no extra information (e.g., nulltype), then
        # just set the metadata to None, which can be taken to represent a "plain" valid cell:
        if cell["valid"] and all([k in ["value", "valid", "messages"] for k in cell]):
            insert_params[variable + "_meta"] = None
        else:
            insert_params[variable + "_meta"] = "{}".format(json.dumps(cell))

    insert_stmt = safe_sql(
        f"INSERT INTO `{table_name}` "
        + "("
        + ", ".join(insert_columns)
        + ") "
        + "VALUES ("
        + ", ".join(insert_values)
        + ")",
        insert_params,
    )
    config["db"].execute(insert_stmt).fetchall()
    config["db"].commit()
    return new_row_number


if __name__ == "__main__":
    try:
        p = ArgumentParser()
        p.add_argument(
            "table",
            help="A TSV file containing high-level information about the data in the database",
        )
        p.add_argument("db_dir", help="The directory in which to save the database file")
        args = p.parse_args()

        CONFIG = read_config_files(
            args.table, Lark(grammar, parser="lalr", transformer=TreeToDict())
        )

        with sqlite3.connect(f"{args.db_dir}/cmi-pb.db") as CONN:
            CONFIG["db"] = CONN
            CONFIG["db"].execute("PRAGMA foreign_keys = ON")
            configure_and_load_db(CONFIG)
    except (
        CycleError,
        FileNotFoundError,
        StopIteration,
        ConfigError,
        VisitError,
        TSVReadError,
    ) as e:
        sys.exit(e)
