#!/usr/bin/env python3

import re


def validate_rows(config, table_name, rows):
    """Given a config map, a table name, and a list of rows (dicts from column names to column
    values), and a list of previously validated rows, return a list of validated rows."""
    result_rows = []
    for row in rows:
        result_row = {}
        for column, value in row.items():
            result_row[column] = {
                "value": value,
                "valid": True,
            }
        result_rows.append(validate_row(config, table_name, result_row, result_rows))
    return result_rows


def validate_row(config, table_name, row, prev_results):
    """Given a config map, a table name, a row to validate (a dict from column names to column
    values), and a list of previously validated rows, return the validated row."""
    duplicate = False
    for column_name, cell in row.items():
        cell = validate_cell(config, table_name, column_name, cell, row, prev_results)
        # If a cell violates either the unique or primary constraints, mark the row as a duplicate:
        if [
            msg
            for msg in cell["messages"]
            if msg["rule"] in ("key:primary", "key:unique", "tree:child-unique")
        ]:
            duplicate = True
        row[column_name] = cell
    row["duplicate"] = duplicate
    return row


def validate_cell(config, table_name, column_name, cell, context, prev_results):
    """Given a config map, a table name, a column name, a cell to validate, the row, `context`,
    to which the cell belongs, and a list of previously validated rows (dicts mapping column names
    to column values), return the validated cell."""
    column = config["table"][table_name]["column"][column_name]
    cell["messages"] = []

    # If the value of the cell is one of the allowable null-type values for this column, then
    # mark it as such and return immediately:
    if column["nulltype"]:
        nt_name = column["nulltype"]
        nulltype = config["datatype"][nt_name]
        result = validate_condition(nulltype["condition"], cell["value"])
        if result:
            cell["nulltype"] = nt_name
            return cell

    # If the column has a primary or unique key constraint, or if it is the child associated with
    # a tree, then if the value of the cell is a duplicate either of one of the previously validated
    # rows in the batch, or a duplicate of a validated row that has already been inserted into the
    # table, mark it with the corresponding error:
    constraints = config["constraints"]
    is_primary = column_name in constraints["primary"][table_name]
    is_unique = False if is_primary else column_name in constraints["unique"][table_name]
    is_tree_child = column_name in [c["child"] for c in constraints["tree"][table_name]]
    if any([is_primary, is_unique, is_tree_child]):

        def make_error(rule):
            return {
                "rule": rule,
                "level": "error",
                "message": "Values of {} must be unique".format(column_name),
            }

        if [
            p[column_name]
            for p in prev_results
            if p[column_name]["value"] == cell["value"] and p[column_name]["valid"]
        ] or config["db"].execute(
            "SELECT 1 FROM `{}` WHERE `{}` = '{}' LIMIT 1".format(
                table_name, column["column"], cell["value"]
            )
        ).fetchall():
            cell["valid"] = False
            if is_primary or is_unique:
                cell["messages"].append(make_error("key:primary" if is_primary else "key:unique"))
            if is_tree_child:
                cell["messages"].append(make_error("tree:child-unique"))

    # Check the cell value against any foreign keys:
    fkeys = [fkey for fkey in constraints["foreign"][table_name] if fkey["column"] == column_name]
    for fkey in fkeys:
        rows = config["db"].execute(
            "SELECT 1 FROM `{}` WHERE `{}` = '{}' LIMIT 1".format(
                fkey["ftable"], fkey["fcolumn"], cell["value"]
            )
        )
        if not rows.fetchall():
            cell["valid"] = False
            cell["messages"].append(
                {
                    "rule": "key:foreign",
                    "level": "error",
                    "message": "Value {} of column {} is not in {}.{}".format(
                        cell["value"], column_name, fkey["ftable"], fkey["fcolumn"]
                    ),
                }
            )

    # If the current column is the parent column of a tree, validate that adding the current value
    # will not result in a cycle between this and the parent column:
    tkeys = [tkey for tkey in constraints["tree"][table_name] if tkey["parent"] == column_name]
    for tkey in tkeys:
        parent_col = column_name
        child_col = tkey["child"]
        parent_val = cell["value"]
        child_val = context[child_col]["value"]

        # In order to check if the current row will cause a dependency cycle, we need to query
        # against all previously validated rows. Since previously validated rows belonging to the
        # current batch will not have been validated yet, we explicitly add them into our query:
        prev_selects = " UNION ".join(
            [
                "SELECT '{}', '{}'".format(p[parent_col]["value"], p[child_col]["value"])
                for p in prev_results
                if all([p[parent_col]["valid"], p[child_col]["valid"]])
            ]
        )
        table_name_ext = table_name if not prev_selects else table_name + "_ext"
        ext_clause = (
            (
                f"    WITH `{table_name_ext}` AS ( "
                f"        SELECT `{parent_col}`, `{child_col}` "
                f"            FROM `{table_name}` "
                f"            UNION "
                f"        {prev_selects} "
                f"    )"
            )
            if prev_selects
            else ""
        )

        sql = (
            f"WITH RECURSIVE `hierarchy` AS ( "
            f"{ext_clause} "
            f"    SELECT `{parent_col}`, `{child_col}` "
            f"        FROM `{table_name_ext}` "
            f"        WHERE `{parent_col}` = '{child_val}' "
            f"        UNION ALL "
            f"    SELECT `t1`.`{parent_col}`, `t1`.`{child_col}` "
            f"        FROM `{table_name_ext}` AS `t1` "
            f"        JOIN `hierarchy` AS `t2` ON `t2`.`{child_col}` = `t1`.`{parent_col}`"
            f") "
            f"SELECT * "
            f"FROM `hierarchy` "
        )
        rows = config["db"].execute(sql).fetchall()
        if rows:
            rows.append((parent_val, child_val))
            cycle_msg = ", ".join(
                ["({}: {}, {}: {})".format(parent_col, row[0], child_col, row[1]) for row in rows]
            )
            cell["valid"] = False
            cell["messages"].append(
                {
                    "rule": "tree:cycle",
                    "level": "error",
                    "message": (
                        f"Cyclic dependency: {cycle_msg} for tree({child_col}) of {parent_col}"
                    ),
                }
            )

    # Validate that the value of the cell conforms to the datatypes associated with the column:
    def get_datatypes_to_check(dt_name):
        datatypes = []
        if dt_name is not None:
            datatype = config["datatype"][dt_name]
            if datatype["condition"] is not None:
                datatypes.append(datatype)
            datatypes += get_datatypes_to_check(datatype["parent"])
        return datatypes

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
