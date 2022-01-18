#!/usr/bin/env python3

import csv
import itertools
import json
import sqlite3
import sys

from graphlib import CycleError, TopologicalSorter
from lark import Lark
from lark.exceptions import VisitError

from sql_utils import safe_sql
from cmi_pb_grammar import grammar, TreeToDict
from validate import validate_rows, validate_tree_foreign_keys, validate_under

CHUNK_SIZE = 2

# TODO include synonyms?
sqlite_types = ["text", "integer", "real", "blob"]


def read_config_files(table_table_path):
    """Given the path to a table TSV file, load and check the special 'table', 'column', and
    'datatype' tables, and return a config structure."""

    def read_tsv(path):
        """Given a path, read a TSV file and return a list of row dicts."""
        with open(path) as f:
            rows = csv.DictReader(f, delimiter="\t")
            rows = list(rows)
            if len(rows) < 1:
                raise Exception(f"No rows in {path}")
            return rows

    special_table_types = ["table", "column", "datatype"]
    path = table_table_path
    rows = read_tsv(path)
    config = {"table": {}, "datatype": {}, "special": {}}
    for t in special_table_types:
        config["special"][t] = None

    # Load table table
    for row in rows:
        for column in ["table", "path", "type"]:
            if column not in row or row[column] is None:
                raise Exception(f"Missing required column '{column}' reading '{path}'")
        for column in ["table", "path"]:
            if row[column].strip() == "":
                raise Exception(f"Missing required value for '{column}' reading '{path}'")
        for column in ["type"]:
            if row[column].strip() == "":
                row[column] = None
        if row["type"] == "table":
            if row["path"] != path:
                raise Exception(
                    "Special 'table' path '{}' does not match this path '{}'".format(
                        row["path"], path
                    )
                )
        if row["type"] in special_table_types:
            if config["special"][row["type"]]:
                raise Exception(
                    "Multiple tables with type '{}' declared in '{}'".format(row["type"], path)
                )
            config["special"][row["type"]] = row["table"]
        if row["type"] and row["type"] not in special_table_types:
            raise Exception("Unrecognized table type '{}' in '{}'".format(row["type"], path))
        row["column"] = {}
        config["table"][row["table"]] = row

    for table_type in special_table_types:
        if config["special"][table_type] is None:
            raise Exception(f"Missing required '{table_type}' table in '{path}'")

    # Load datatype table
    table_name = config["special"]["datatype"]
    path = config["table"][table_name]["path"]
    rows = read_tsv(path)
    for row in rows:
        for column in ["datatype", "parent", "condition", "SQL type"]:
            if column not in row or row[column] is None:
                raise Exception(f"Missing required column '{column}' reading '{path}'")
        for column in ["datatype"]:
            if row[column].strip() == "":
                raise Exception(f"Missing required value for '{column}' reading '{path}'")
        for column in ["parent", "condition", "SQL type"]:
            if row[column].strip() == "":
                row[column] = None
        # TODO: validate conditions
        config["datatype"][row["datatype"]] = row
    # TODO: Check for required datatypes: text, empty, line, word

    # Load column table
    table_name = config["special"]["column"]
    path = config["table"][table_name]["path"]
    rows = read_tsv(path)
    for row in rows:
        for column in ["table", "column", "nulltype", "datatype"]:
            if column not in row or row[column] is None:
                raise Exception(f"Missing required column '{column}' reading '{path}'")
        for column in ["table", "column", "datatype"]:
            if row[column].strip() == "":
                raise Exception(f"Missing required value for '{column}' reading '{path}'")
        for column in ["nulltype"]:
            if row[column].strip() == "":
                row[column] = None
        if row["table"] not in config["table"]:
            raise Exception("Undefined table '{}' reading '{}'".format(row["table"], path))
        if row["nulltype"] and row["nulltype"] not in config["datatype"]:
            raise Exception("Undefined nulltype '{}' reading '{}'".format(row["nulltype"], path))
        if row["datatype"] not in config["datatype"]:
            raise Exception("Undefined datatype '{}' reading '{}'".format(row["datatype"], path))
        row["configured"] = True
        config["table"][row["table"]]["column"][row["column"]] = row

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
                    raise ValueError(
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
        raise CycleError(message)


def create_db_and_write_sql(config):
    """Given a config map, read TSVs and write out SQL strings."""
    table_list = list(config["table"].keys())
    for table_name in table_list:
        path = config["table"][table_name]["path"]
        with open(path) as f:
            # Open a DictReader to get the first row from which we will read the column names of the
            # table. Note that although we discard the rest this should not be inefficient. Since
            # DictReader is implemented as an Iterator it does not read the whole file.
            rows = csv.DictReader(f, delimiter="\t")

            # Update columns
            defined_columns = config["table"][table_name]["column"]
            try:
                actual_columns = list(next(rows).keys())
            except StopIteration:
                raise StopIteration(f"No rows in {path}")

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
                table_sql, table_constraints = create_schema(config, table)
                if not table.endswith("_conflict"):
                    config["constraints"]["foreign"][table_name] = table_constraints["foreign"]
                    config["constraints"]["unique"][table_name] = table_constraints["unique"]
                    config["constraints"]["primary"][table_name] = table_constraints["primary"]
                    config["constraints"]["tree"][table_name] = table_constraints["tree"]
                    config["constraints"]["under"][table_name] = table_constraints["under"]
                config["db"].executescript(table_sql)
                print("{}\n".format(table_sql))

            # Create a view as the union of the regular and conflict versions of the table:
            sql = "DROP VIEW IF EXISTS `{}`;\n".format(table_name + "_view")
            sql += "CREATE VIEW `{}` AS SELECT * FROM `{}` UNION SELECT * FROM `{}`;".format(
                table_name + "_view", table_name, table_name + "_conflict"
            )
            config["db"].executescript(sql)
            print("{}\n".format(sql))
            config["db"].commit()

    # Sort tables according to their foreign key dependencies so that tables are always loaded
    # after the tables they depend on:
    table_list = verify_table_deps_and_sort(table_list, config["constraints"])

    # Now load the rows:
    for table_name in table_list:
        path = config["table"][table_name]["path"]
        with open(path) as f:
            rows = csv.DictReader(f, delimiter="\t")
            # Collect data into fixed-length chunks or blocks
            # See: https://docs.python.org/3.9/library/itertools.html#itertools-recipes
            chunks = itertools.zip_longest(*([iter(rows)] * CHUNK_SIZE))
            for i, chunk in enumerate(chunks):
                chunk = filter(None, chunk)
                sql = insert_rows(config, table_name, chunk)
                config["db"].executescript(sql)
                config["db"].commit()
                print("{}\n\n".format(sql))
                print("-- end of chunk {}\n\n".format(i))

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
                f"UPDATE `{table}` SET `{column}` = NULL, `{meta}` = :mval WHERE rowid = :rowid;",
                {
                    "mval": "json({})".format(json.dumps(record["meta"])),
                    "rowid": record["rowid"],
                },
            )
            config["db"].execute(sql)
        config["db"].commit()


def get_SQL_type(config, datatype):
    """Given the config structure and the name of a datatype, climb the datatype tree (as required),
    and return the first 'SQL type' found."""
    if "datatype" not in config:
        raise Exception("Missing datatypes in config")
    if datatype not in config["datatype"]:
        return None
    if config["datatype"][datatype]["SQL type"]:
        return config["datatype"][datatype]["SQL type"]
    return get_SQL_type(config, config["datatype"][datatype]["parent"])


def create_schema(config, table_name):
    """Given the config structure and a table name, generate a SQL schema string, including each
    column C and its matching C_meta column, then return the schema string as well as a list of the
    table's constraints."""
    output = [f"DROP TABLE IF EXISTS `{table_name}`;", f"CREATE TABLE `{table_name}` ("]
    columns = config["table"][table_name.replace("_conflict", "")]["column"]
    table_constraints = {"foreign": [], "unique": [], "primary": [], "tree": [], "under": []}
    c = len(columns.values())
    r = 0
    for row in columns.values():
        r += 1
        sql_type = get_SQL_type(config, row["datatype"])
        if not sql_type:
            raise Exception("Missing SQL type for {}".format(row["datatype"]))
        if not sql_type.lower() in sqlite_types:
            raise Exception("Unrecognized SQL type '{}' for {}".format(sql_type, row["datatype"]))
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
                        raise ValueError(f"Invalid foreign key: {structure} for: {table_name}")
                    table_constraints["foreign"].append(
                        {
                            "column": column_name,
                            "ftable": expression["args"][0]["table"],
                            "fcolumn": expression["args"][0]["column"],
                        }
                    )
                elif expression["type"] == "function" and expression["name"] == "tree":
                    if len(expression["args"]) != 1 or expression["args"][0]["type"] != "label":
                        raise ValueError(
                            f"Invalid 'tree' constraint: {structure} for: {table_name}"
                        )
                    child = expression["args"][0]["value"]
                    child_datatype = columns.get(child, {}).get("datatype")
                    if not child_datatype:
                        raise ValueError(
                            f"Could not determine SQL datatype for {child} of tree({child})"
                        )
                    parent = column_name
                    child_sql_type = get_SQL_type(config, child_datatype)
                    if sql_type != child_sql_type:
                        raise ValueError(
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
                        raise ValueError(
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

    return "\n".join(output), table_constraints


def insert_rows(config, table_name, rows):
    """Given a config map, a table name, and a list of rows (dicts from column names to column
    values), return a SQL string for an INSERT statement with VALUES for all the rows."""

    def generate_sql(table_name, rows):
        lines = []
        for row in rows:
            # The 'duplicate' flag has already served its purpose (see below). Here we delete it
            # from the row record to prevent it from being interpreted as a cell:
            del row["duplicate"]
            values = []
            params = {}
            for column, cell in row.items():
                column = column.replace(" ", "_")
                value = None
                if "nulltype" in cell and cell["nulltype"]:
                    value = None
                elif cell["valid"]:
                    value = cell["value"]
                    cell.pop("value")
                values.append(f":{column}")
                values.append(f":{column}_meta")
                params[column] = value
                params[column + "_meta"] = "json({})".format(json.dumps(cell))
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

    result_rows = validate_rows(config, table_name, rows)
    main_rows = []
    conflict_rows = []
    for row in result_rows:
        if row["duplicate"]:
            conflict_rows.append(row)
        else:
            main_rows.append(row)
    return (
        generate_sql(table_name, main_rows)
        + "\n"
        + generate_sql(table_name + "_conflict", conflict_rows)
    )


def update_row(config, table_name, row, rowid):
    """Given a config map, a table name, a row (a dict from column names to column values), and the
    rowid to update, update the corresponding row in the database with new values as specified by
    `row`."""

    # As a result of validation, the row will have a duplicate flag, which is unneeded here and can
    # simply be removed:
    del row["duplicate"]
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
        assignments.append(f"`{column}_meta` = :{variable}_meta")
        params[variable] = value
        params[variable + "_meta"] = "json({})".format(json.dumps(cell))

    update_stmt = f"UPDATE `{table_name}` SET "
    update_stmt += safe_sql(", ".join(assignments), params)
    update_stmt += safe_sql(" WHERE ROWID = :rowid", {"rowid": rowid})
    config["db"].execute(update_stmt).fetchall()
    config["db"].commit()


if __name__ == "__main__":
    try:
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
            create_db_and_write_sql(config)
    except (CycleError, FileNotFoundError, StopIteration, ValueError, VisitError) as e:
        sys.exit(e)
