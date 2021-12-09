#!/usr/bin/env python3

import csv
import itertools
import json
import re
import sqlite3
import sys

from graphlib import CycleError, TopologicalSorter
from sqlalchemy.sql.expression import text as sql_text

from validate import validate_rows

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
    between tables. The dictionary should include a sub-dictionary of foreign key constraints and
    a sub-dictionary of tree constraints. The former should be of the form:
    {'my_table': [{'column': 'my_column',
                   'fcolumn': 'foreign_column',
                   'ftable': 'foreign_table'},
                  ...]
     ...}.
    The latter should be of the form:
    {'my_table': [{'child': 'my_child', 'parent': 'my_parent'},
                  ...],
     ...}.

    After validating that there are no cycles amongst the foreign and tree dependencies, returns
    the list of tables sorted according to their foreign key dependencies, such that if table_a
    depends on table_b, then table_b comes before table_a in the list."""

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
    ts = TopologicalSorter()
    for table_name in table_list:
        deps = set(dep["ftable"] for dep in foreign_keys.get(table_name, []))
        ts.add(table_name, *deps)
    try:
        return list(ts.static_order())
    except CycleError as e:
        cycle = e.args[1]
        message = "Cyclic dependency between tables {}: ".format(", ".join(cycle))
        end_index = len(cycle) - 1
        for i, table in enumerate(cycle):
            if i < end_index:
                dep_name = cycle[i + 1]
                dep = [d for d in foreign_keys[table] if d["ftable"] == dep_name].pop()
                message += "{}.{} depends on {}.{}".format(
                    table, dep["column"], dep["ftable"], dep["fcolumn"]
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
                config["db"].executescript(table_sql)
                print("{}\n".format(table_sql))

            # Create a view as the union of the regular and conflict versions of the table:
            sql = safe_sql("DROP VIEW IF EXISTS :view;\n", {"view": table_name + "_view"})
            sql += safe_sql(
                "CREATE VIEW :view AS SELECT * FROM :table UNION SELECT * FROM :conflict;",
                {
                    "view": table_name + "_view",
                    "table": table_name,
                    "conflict": table_name + "_conflict",
                },
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
    output = [
        safe_sql("DROP TABLE IF EXISTS :table;", {"table": table_name}),
        safe_sql("CREATE TABLE :table (", {"table": table_name}),
    ]
    columns = config["table"][table_name.replace("_conflict", "")]["column"]
    table_constraints = {"foreign": [], "unique": [], "primary": [], "tree": []}
    c = len(columns.values())
    r = 0
    for row in columns.values():
        r += 1
        sql_type = get_SQL_type(config, row["datatype"])
        if not sql_type:
            raise Exception("Missing SQL type for {}".format(row["datatype"]))
        if not sql_type.lower() in sqlite_types:
            raise Exception("Unrecognized SQL type '{}' for {}".format(sql_type, row["datatype"]))
        line = f"  :col {sql_type}"
        params = {"col": row["column"]}
        structure = row.get("structure")
        if structure and not table_name.endswith("_conflict"):
            keys = re.split(r"\s+", structure)
            for key in keys:
                key = key.strip().lower()
                if key == "primary":
                    line += " PRIMARY KEY"
                    table_constraints["primary"].append(row["column"])
                elif key == "unique":
                    line += " UNIQUE"
                    table_constraints["unique"].append(row["column"])
                else:
                    match = re.fullmatch(r"^(from|tree)\((.+)\)$", key)
                    if match and match.group(1) == "from":
                        foreign = match.group(2).split(".", 1)
                        if len(foreign) != 2:
                            raise ValueError(
                                "Invalid foreign key: {} for: {}".format(structure, table_name)
                            )
                        table_constraints["foreign"].append(
                            {"column": row["column"], "ftable": foreign[0], "fcolumn": foreign[1]}
                        )
                    elif match and match.group(1) == "tree":
                        child = match.group(2)
                        child_datatype = columns.get(child, {}).get("datatype")
                        if not child_datatype:
                            raise ValueError(
                                f"Could not determine SQL datatype for {child} of tree({child})"
                            )
                        parent = row["column"]
                        child_sql_type = get_SQL_type(config, child_datatype)
                        if sql_type != child_sql_type:
                            raise ValueError(
                                f"SQL type '{child_sql_type}' of '{child}' in 'tree({child})' for "
                                f"table '{table_name}' does not match SQL type: '{sql_type}' of "
                                f"parent: '{parent}'."
                            )
                        table_constraints["tree"].append({"parent": row["column"], "child": child})

        line += ","
        output.append(safe_sql(line, params))
        line = "  :meta TEXT"
        if r >= c and not table_constraints["foreign"]:
            line += ""
        else:
            line += ","
        output.append(safe_sql(line, {"meta": row["column"] + "_meta"}))

    num_fkeys = len(table_constraints["foreign"])
    for i, fkey in enumerate(table_constraints["foreign"]):
        output.append(
            safe_sql(
                "  FOREIGN KEY (:column) REFERENCES :ftable(:fcolumn){}".format(
                    "," if i < (num_fkeys - 1) else ""
                ),
                {"column": fkey["column"], "ftable": fkey["ftable"], "fcolumn": fkey["fcolumn"]},
            )
        )
    output.append(");")
    # Loop through the tree constraints and if any of their associated child columns do not already
    # have an associated unique or primary index, create one implicitly here:
    for i, tree in enumerate(table_constraints["tree"]):
        if tree["child"] not in (table_constraints["unique"] + table_constraints["primary"]):
            output.append(
                "CREATE UNIQUE INDEX {}_{}_idx ON {}({});".format(
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
            output += safe_sql("INSERT INTO :table VALUES", {"table": table_name})
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


def safe_sql(template, params):
    """Given a SQL query template with variables and a dict of parameters,
    return an escaped SQL string."""
    stmt = sql_text(template).bindparams(**params)
    return str(stmt.compile(compile_kwargs={"literal_binds": True}))


if __name__ == "__main__":
    try:
        config = read_config_files("src/table.tsv")
        with sqlite3.connect("build/cmi-pb.db") as conn:
            config["db"] = conn
            config["constraints"] = {"foreign": {}, "unique": {}, "primary": {}, "tree": {}}
            create_db_and_write_sql(config)
    except (CycleError, FileNotFoundError, StopIteration, ValueError) as e:
        sys.exit(e)
