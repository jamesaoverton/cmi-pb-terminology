#!/usr/bin/env python3

import re

def validate_rows(config, table_name, rows):
    """Given a validation config, a table name,
    and a list of row dicts (from column names to value strings),
    return a list row dicts (from column names to cell dicts)."""
    result_rows = []
    for row in rows:
        result_row = {}
        for column, value in row.items():
            result_row[column] = {
                'value': value,
                'valid': True,
            }
        result_rows.append(validate_row(config, table_name, result_row))
    return result_rows

def validate_row(config, table_name, row):
    """Given a validation config, a table name,
    and a row dict (from column names to value strings),
    return a row dict (from column names to cell dicts)."""
    for column_name, cell in row.items():
        row[column_name] = validate_cell(config, table_name, column_name, cell)
    return row

def validate_cell(config, table_name, column_name, cell):
    """Given a validation config, a table name, a column name, and a cell dict,
    return an updated cell dict."""
    # TODO: Hierarchical validation
    column = config['table'][table_name]['column'][column_name]
    nt_name = column['nulltype']
    if nt_name:
        nulltype = config['datatype'][nt_name]
        result = validate_condition(nulltype['condition'], cell['value'])
        if result:
            cell['nulltype'] = nt_name
    return cell

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

