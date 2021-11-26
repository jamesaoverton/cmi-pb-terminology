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
                "value": value,
                "valid": True,
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
    # If the value of the cell is one of the allowable null-type values for this column, then mark
    # it as such:
    column = config["table"][table_name]["column"][column_name]
    nt_name = column["nulltype"]
    if nt_name:
        nulltype = config["datatype"][nt_name]
        result = validate_condition(nulltype["condition"], cell["value"])
        if result:
            cell["nulltype"] = nt_name

    # Validate that the value of the cell conforms to the datatypes associated with the column:
    def get_datatypes_to_check(dt_name):
        datatypes = []
        if dt_name is not None:
            datatype = config["datatype"][dt_name]
            if datatype["condition"] is not None:
                datatypes.append(datatype)
            datatypes += get_datatypes_to_check(datatype["parent"])
        return datatypes

    cell["messages"] = []
    if not cell.get("nulltype"):
        dt_name = column["datatype"]
        datatypes_to_check = get_datatypes_to_check(dt_name)
        # We use while and pop() instead of a for loop so as to check conditions in LIFO order:
        while datatypes_to_check:
            datatype = datatypes_to_check.pop()
            if datatype["condition"].startswith("exclude") == validate_condition(
                datatype["condition"], cell["value"]
            ):
                cell["messages"].append(
                    {
                        "rule": "datatype:{}".format(datatype["datatype"]),
                        "level": "error",
                        "message": "{} should be {}".format(column_name, datatype["description"]),
                    }
                )
                cell["valid"] = False
    return cell


def validate_condition(condition, value):
    """Given a condition string and a value string,
    return True of the condition holds, False otherwise."""
    # TODO: Implement real conditions.
    if condition == "equals('')":
        return value == ""
    elif condition == r"exclude(/\n/)":
        return "\n" in value
    elif condition == r"exclude(/\s/)":
        return bool(re.search(r"\s", value))
    elif condition == r"exclude(/^\s+|\s+$/)":
        return bool(re.search(r"^\s+|\s+$", value))
    elif condition == r"in('table', 'column', 'datatype')":
        return value in ("table", "column", "datatype")
    elif condition == r"match(/\w+/)":
        return bool(re.fullmatch(r"\w+", value))
    elif condition == r"search(/:/)":
        return ":" in value
    else:
        raise Exception(f"Unhandled condition: {condition}")
