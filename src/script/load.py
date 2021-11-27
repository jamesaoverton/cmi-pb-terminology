#!/usr/bin/env python3

import csv
import json
import itertools
import sys

from sqlalchemy.sql.expression import text as sql_text

from validate import validate_rows

CHUNK_SIZE = 3

# TODO include synonyms?
sqlite_types = ["text", "integer", "real", "blob"]


def read_config_files(table_table_path):
    """Given the path to a table table TSV file,
    load and check the special 'table', 'column', and 'datatype' tables,
    and return a config structure."""

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
                    f"Special 'table' path '{row['path']}' does not match this path '{path}'"
                )
        if row["type"] in special_table_types:
            if config["special"][row["type"]]:
                raise Exception(f"Multiple tables with type '{row['type']}' declared in '{path}'")
            config["special"][row["type"]] = row["table"]
        if row["type"] and row["type"] not in special_table_types:
            raise Exception(f"Unrecognized table type '{row['type']}' in '{path}'")
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
            raise Exception(f"Undefined table '{row['table']}' reading '{path}'")
        if row["nulltype"] and row["nulltype"] not in config["datatype"]:
            raise Exception(f"Undefined nulltype '{row['nulltype']}' reading '{path}'")
        if row["datatype"] not in config["datatype"]:
            raise Exception(f"Undefined datatype '{row['datatype']}' reading '{path}'")
        row["configured"] = True
        config["table"][row["table"]]["column"][row["column"]] = row

    return config


def generate_and_write_sql_from_files(config):
    """Given a config, read TSVs and write out SQL strings."""
    # TODO: determine table load sequence by foreign key relations
    # fail on circularity
    table_list = list(config["table"].keys())

    for table_name in table_list:
        path = config["table"][table_name]["path"]
        with open(path) as f:
            rows = csv.DictReader(f, delimiter="\t")

            # update columns
            defined_columns = config["table"][table_name]["column"]
            actual_columns, rows = itertools.tee(rows)
            try:
                actual_columns = list(next(actual_columns).keys())
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

            sql = create_schema(config, table_name)
            print("{}\n\n".format(sql))

            # Collect data into fixed-length chunks or blocks
            # See: https://docs.python.org/3.9/library/itertools.html#itertools-recipes
            chunks = itertools.zip_longest(*([iter(rows)] * CHUNK_SIZE))
            for i, chunk in enumerate(chunks):
                chunk = filter(None, chunk)
                sql = insert_rows(config, table_name, chunk)
                print("{}\n\n".format(sql))
                print("-- end of chunk {}\n\n".format(i))


def get_SQL_type(config, datatype):
    """Given the config structure and the name of a datatype,
    climb the datatype tree (as required),
    and return the first 'SQL type' found."""
    if "datatype" not in config:
        raise Exception("Missing datatypes in config")
    if datatype not in config["datatype"]:
        return None
    if config["datatype"][datatype]["SQL type"]:
        return config["datatype"][datatype]["SQL type"]
    return get_SQL_type(config, config["datatype"][datatype]["parent"])


def create_schema(config, table_name):
    """Given the config structure and a table name,
    generate a SQL schema string,
    including each column C and its matching C_meta column."""
    output = [
        safe_sql("DROP TABLE IF EXISTS :table;", {"table": table_name}),
        safe_sql("CREATE TABLE :table (", {"table": table_name}),
    ]
    columns = config["table"][table_name]["column"]
    c = len(columns.values())
    r = 0
    for row in columns.values():
        r += 1
        sql_type = get_SQL_type(config, row["datatype"])
        if not sql_type:
            raise Exception(f"Missing SQL type for {row['datatype']}")
        if not sql_type.lower() in sqlite_types:
            raise Exception(f"Unrecognized SQL type '{sql_type}' for {row['datatype']}")
        line = f"  :col {sql_type}"
        params = {"col": row["column"]}
        # if row['structure'].strip().lower() == 'primary':
        #    line += ' PRIMARY KEY'
        line += ","
        output.append(safe_sql(line, params))
        line = f"  :meta TEXT{',' if r < c else ''}"
        output.append(safe_sql(line, {"meta": row["column"] + "_meta"}))
    output.append(");")
    return "\n".join(output)


def insert_rows(config, table_name, rows):
    """Given the config structure, table name, and list of row dicts,
    return a SQL string for an INSERT statement with VALUES for all the rows."""
    result_rows = validate_rows(config, table_name, rows)
    lines = []
    for row in result_rows:
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
            params[column + "_meta"] = f"json({json.dumps(cell)})"
        line = ", ".join(values)
        line = f"({line})"
        lines.append(safe_sql(line, params))
    output = safe_sql("INSERT INTO :table VALUES", {"table": table_name})
    output += "\n"
    output += ",\n".join(lines)
    output += ";"
    return output


def safe_sql(template, params):
    """Given a SQL query template with variables and a dict of parameters,
    return an escaped SQL string."""
    stmt = sql_text(template).bindparams(**params)
    return str(stmt.compile(compile_kwargs={"literal_binds": True}))


if __name__ == "__main__":
    try:
        config = read_config_files("src/table.tsv")
        generate_and_write_sql_from_files(config)
    except (FileNotFoundError, StopIteration) as e:
        sys.exit(e)
