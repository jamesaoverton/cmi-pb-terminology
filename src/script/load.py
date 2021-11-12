#!/usr/bin/env python3

import csv
import json
import re

required_table_columns = {
    "table",
    "path"
}

required_column_columns = {
    "table",
    "column",
    "datatype",
    "nulltype"
}

required_datatype_columns = {
    "datatype",
    "parent",
    "condition",
    "SQL type",
}

def load_required(path, required_columns):
    """Load a table from a path, checking for the set of required columns,
    and return a list of dicts."""
    try:
        with open(path) as f:
            rows = csv.DictReader(f, delimiter="\t")
            row = next(rows)
            if not row:
                raise Exception(f"No rows in {path}")
            missing_columns = required_columns - set(row.keys())
            if missing_columns:
                raise Exception(f"Missing required keys {path}: {', '.join(missing_columns)}")
            return [row] + list(rows)
    except FileNotFoundError as e:
        raise Exception(f"There was an error reading {path}", e)

def get_SQL_type(datatypes, datatype):
    """Given the datatypes dict and the name of a datatype,
    climb the datatype tree (as required),
    and return the first 'SQL type' found."""
    if not datatype:
        return None
    if datatype not in datatypes:
        return None
    if datatypes[datatype]['SQL type']:
       return datatypes[datatype]['SQL type']
    return get_SQL_type(datatypes, datatypes[datatype]['parent'])

def create_bad_schema(columns, datatypes, table):
    """Given the columns dict, datatypes dict, and table name,
    generate a SQL schema string,
    including each column C and its matching C_meta column."""
    output = []
    output.append(f"DROP TABLE IF EXISTS '{table}';")
    output.append(f"CREATE TABLE '{table}' (")
    c = len(columns[table].values())
    r = 0
    for row in columns[table].values():
        r += 1
        sql_type = get_SQL_type(datatypes, row['datatype'])
        if not sql_type:
            raise Exception(f"Missing SQL type for {row['datatype']}")
        line = f"  '{row['column']}' {sql_type.upper()}"
        if row['schema'].strip().lower() == 'primary':
            line += ' PRIMARY KEY'
        line += ','
        if row['description']:
            line += ' -- ' + row['description']
        output.append(line)
        line = f"  '{row['column']}_meta' TEXT{',' if r < c else ''} -- JSON metadata for {row['column']}"
        output.append(line)
    output.append(");")
    return '\n'.join(output)

def validate_condition(condition, value):
    """Given a condition string and a value string,
    return True of the condition holds, False otherwise."""
    # TODO: Implement real conditions.
    if condition == "equals('')":
        return value == ""
    elif condition == "exclude(/\n/)":
        return '\n' in value
    elif condition == "exclude(/\s/)":
        return bool(re.search('\s', value))
    elif condition == "exclude(/^\s+|\s+$/)":
        return bool(re.search('^\s+|\s+$', value))
    elif condition == "in('table', 'column', 'datatype')":
        return value in ('table', 'column', 'datatype')
    elif condition == "match(/\w+/)":
        return bool(re.matches('\w+', value))
    elif condition == "search(/:/)":
        return ':' in value
    else:
        raise Exception(f"Unhandled condition: {condition}")

def validate_cell(datatypes, dt_name, nt_name, value):
    """Given the datatypes dict, a datatyoe name, a nulltype name, and a value string,
    return the pair of a value and the meta
    to be inserted into the SQL table."""
    # TODO: Hierarchical validation
    # TODO: none of this SQL is properly escaped
    if nt_name:
        nulltype = datatypes[nt_name]
        result = validate_one(nulltype['condition'], value)
        if result:
            meta = json.dumps({
                'value': value,
                'nulltype': nt_name,
            })
            return 'NULL', f"json('{meta}')"
    datatype = datatypes[dt_name]
    condition = datatype['condition']
    if condition:
        result = validate_condition(condition, value)
        if not result:
            meta = json.dumps({
                'value': value,
                'datatype': dt_name,
                'messages': [{
                    'rule': f"datatype:{dt_name}",
                    'level': 'error',
                    'message': 'Validation failure',
                }],
            })
            return 'NULL', f"json('{meta}')"
    return f"'{value}'", 'NULL'

def insert_rows(columns, datatypes, table, rows):
    """Given the columns dict, datatypes dict, table name, and list of row dicts,
    return a SQL string for an INSERT statement with VALUES for all the rows."""
    # TODO: None of the SQL is properly escaped
    output = []
    output.append(f"INSERT INTO '{table}' VALUES")
    for row in rows:
        values = []
        for k, v in row.items():
            column = columns[table][k]
            datatype = column['datatype']
            nulltype = column['nulltype']
            value, meta = validate_cell(datatypes, datatype, nulltype, v)
            values += [value, meta]
        line = ', '.join(values)
        line = f"({line})"
        if row == rows[-1]:
            line += ';'
        else:
            line += ','
        output.append(line)
    return '\n'.join(output)

if __name__ == "__main__":
    # Read the special tables
    table_rows = load_required('src/table.tsv', required_table_columns)
    column_rows = load_required('src/column.tsv', required_column_columns)
    datatype_rows = load_required('src/datatype.tsv', required_datatype_columns)

    # Build some more convenient dicts
    datatypes = {x['datatype']:x for x in datatype_rows}
    columns = {}
    for row in column_rows:
        if row['table'] not in columns:
            columns[row['table']] = {}
        columns[row['table']][row['column']] = row

    # Generate table schemas and inserts
    table_schema = create_bad_schema(columns, datatypes, 'table')
    print(table_schema)
    table_rows = insert_rows(columns, datatypes, 'table', table_rows)
    print(table_rows)

