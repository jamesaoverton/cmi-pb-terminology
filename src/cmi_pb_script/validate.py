import json

from .sql_utils import safe_sql


class ValidationException(Exception):
    pass


def validate_row(config, table_name, row, existing_row=True, row_number=None):
    """
    Given a config map, a table name, an existing row to validate, and its associated row number,
    perform both intra- and inter-row validation and return the validated row.
    """
    if existing_row:
        result_row = row
    else:
        result_row = {}
        for column, value in row.items():
            result_row[column] = {
                "value": value,
                "valid": True,
                "messages": [],
            }

    # We check all the cells for nulltype first, since the rules validation requires that we
    # have this information for all cells.
    for column_name, cell in result_row.items():
        cell = validate_cell_nulltype(config, table_name, column_name, cell)
        result_row[column_name] = cell

    for column_name, cell in result_row.items():
        cell = validate_cell_rules(config, table_name, column_name, result_row, cell)
        if cell.get("nulltype") is None:
            cell = validate_cell_datatype(config, table_name, column_name, cell)
            cell = validate_cell_trees(
                config, table_name, column_name, cell, result_row, prev_results=[]
            )
            cell = validate_cell_foreign_constraints(config, table_name, column_name, cell)
            cell = validate_unique_constraints(
                config,
                table_name,
                column_name,
                cell,
                result_row,
                prev_results=[],
                existing_row=existing_row,
                row_number=row_number,
            )
        result_row[column_name] = cell
    return result_row


def get_matching_values(config, table_name, column_name, matching_string=""):
    """
    Given a config map, a table name, a column name, and (optionally) a string to match, return a
    JSON array of possible valid values for the given column which contain the matching string as a
    substring (or all of them if no matching string is given). The JSON array returned is formatted
    for Typeahead, i.e., it takes the form:
    [{"id": id, "label": label, "order": order}, ...].
    This function may throw a `ValidationException` if the structure used to fetch the matching
    values refers to a tree that does not exist.
    """
    # If the datatype for the given column is explicated in terms of an `in(...)` condition, then
    # the arguments to the `in(...)` function, filtered by the matching string, are the values to
    # be returned:
    dt_name = config["table"][table_name]["column"][column_name]["datatype"]
    datatype = config["datatype"][dt_name]
    dt_condition = datatype["parsed_condition"]
    values = []
    if dt_condition["type"] == "function" and dt_condition["name"] == "in":
        # Remove the enclosing quotes from the values being returned:
        values = [arg["value"].strip("'\"") for arg in dt_condition["args"]]
        values = [v for v in values if matching_string in v]
    else:
        # If the datatype for the column does not correspond to an `in(...)` function, then we check
        # the column's structure constraints. If they include a `from(foreign_table.foreign_column)`
        # condition, then the values are taken from the foreign column. Otherwise if the structure
        # includes an `under(tree_table.tree_column, value)` condition, then get the values from the
        # tree column that are under `value`
        structure = config["table"][table_name]["column"][column_name]["parsed_structure"]
        # Convert the given matching string into one suitable for substring matching in SQL:
        matching_string = "%" if not matching_string else f"%{matching_string}%"
        if structure and structure["type"] == "function" and structure["name"] == "from":
            ftable = structure["args"][0]["table"]
            fcolumn = structure["args"][0]["column"]
            sql = safe_sql(
                f"SELECT `{fcolumn}` FROM `{ftable}` WHERE `{fcolumn}` LIKE :value",
                {"value": matching_string},
            )
            rows = config["db"].execute(sql)
            values = [r[0] for r in rows.fetchall()]
        elif structure and structure["type"] == "function" and structure["name"] == "under":
            tree_col = structure["args"][0]["column"]
            tree = [c for c in config["constraints"]["tree"][table_name] if c["child"] == tree_col]
            if not tree:
                raise ValidationException(f"No tree: '{table_name}.{tree_col}' found")
            tree = tree[0]
            child_column = tree["child"]
            under_val = structure["args"][1]["value"]
            sql = safe_sql(
                with_tree_sql(tree, table_name, under_val)
                + f"SELECT `{child_column}` FROM `tree` WHERE `{child_column}` LIKE :value",
                {"value": matching_string},
            )
            rows = config["db"].execute(sql)
            values = [r[0] for r in rows.fetchall()]

    return [{"id": v, "label": v, "order": i} for i, v in enumerate(values, start=1)]


def validate_rows_intra(config, table_name, rows, chunk_number, results):
    """
    Given a config map, a table name, a chunk of rows to perform intra-row validation on, the
    chunk number assigned to the rows, and a results dictionary, validate all of the rows in the
    chunk and add the validated rows to the results dictionary using the chunk number as its key.
    In addition to adding the results to the results dictionary, also return them.
    """
    result_rows = []
    for row in rows:
        result_row = {}
        for column, value in row.items():
            result_row[column] = {
                "value": value,
                "valid": True,
                "messages": [],
            }

        # We check all the cells for nulltype first, since the rules validation requires that we
        # have this information for all cells.
        for column_name, cell in result_row.items():
            cell = validate_cell_nulltype(config, table_name, column_name, cell)
            result_row[column_name] = cell

        for column_name, cell in result_row.items():
            cell = validate_cell_rules(config, table_name, column_name, result_row, cell)
            if cell.get("nulltype") is None:
                cell = validate_cell_datatype(config, table_name, column_name, cell)
            result_row[column_name] = cell

        result_rows.append(result_row)
    results[chunk_number] = result_rows
    return result_rows


def validate_rows_trees(config, table_name, rows):
    """
    Given a config map, a table name, and a chunk of rows to validate, perform tree-validation
    on the rows and return the validated results.
    """
    result_rows = []
    for row in rows:
        result_row = {}
        for column_name, cell in row.items():
            if cell.get("nulltype") is None:
                cell = validate_cell_trees(config, table_name, column_name, cell, row, result_rows)
            result_row[column_name] = cell
        result_rows.append(result_row)
    return result_rows


def validate_rows_constraints(config, table_name, rows):
    """
    Given a config map, a table name, and a chunk of rows to validate, validate foreign and unique
    constraints, where the latter include primary and "tree child" keys (which imply unique
    constraints.
    """
    result_rows = []
    for row in rows:
        result_row = {}
        for column_name, cell in row.items():
            if cell.get("nulltype") is None:
                cell = validate_cell_foreign_constraints(config, table_name, column_name, cell)
                cell = validate_unique_constraints(
                    config,
                    table_name,
                    column_name,
                    cell,
                    row,
                    result_rows,
                    existing_row=False,
                    row_number=None,
                )
            result_row[column_name] = cell
        result_rows.append(result_row)
    return result_rows


def validate_cell_nulltype(config, table_name, column_name, cell):
    """
    Given a config map, a table name, a column name, and a cell, validate the cell's nulltype
    condition. If the cell's value is one of the allowable nulltype values for this column, then
    fill in the cell's nulltype value before returning the cell.
    """
    # If the value of the cell is one of the:
    column = config["table"][table_name]["column"][column_name]
    if column["nulltype"]:
        nt_name = column["nulltype"]
        nulltype = config["datatype"][nt_name]
        if nulltype["compiled_condition"](cell["value"]):
            cell["nulltype"] = nt_name
    return cell


def validate_cell_foreign_constraints(config, table_name, column_name, cell):
    """
    Given a config map, a table name, a column name, and a cell to validate, check the cell
    value against any foreign keys that have been defined for the column. If there is a violation,
    indicate it with an error message attached to the cell.
    """
    constraints = config["constraints"]
    fkeys = [fkey for fkey in constraints["foreign"][table_name] if fkey["column"] == column_name]
    for fkey in fkeys:
        ftable, fcolumn = fkey["ftable"], fkey["fcolumn"]
        rows = config["db"].execute(
            safe_sql(
                f"SELECT 1 FROM `{ftable}` WHERE `{fcolumn}` = :value LIMIT 1",
                {"value": cell["value"]},
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
    return cell


def validate_cell_trees(config, table_name, column_name, cell, context, prev_results):
    """
    Given a config map, a table name, a column name, a cell to validate, the row, `context`,
    to which the cell belongs, and a list of previously validated rows (dicts mapping column names
    to column values), validate that none of the "tree" constraints on the column are violated,
    and indicate any violations by attaching error messages to the cell.
    """
    constraints = config["constraints"]
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
        # current batch will not have been inserted to the db yet, we explicitly add them in:
        prev_selects = " UNION ".join(
            [
                safe_sql(
                    "SELECT :c_value, :p_value",
                    {"c_value": p[child_col]["value"], "p_value": p[parent_col]["value"]},
                )
                for p in prev_results
                if all([p[child_col]["valid"], p[parent_col]["valid"]])
            ]
        )
        table_name_ext = table_name if not prev_selects else table_name + "_ext"
        extra_clause = (
            (
                f"    WITH `{table_name_ext}` AS ( "
                f"        SELECT `{child_col}`, `{parent_col}` "
                f"            FROM `{table_name}` "
                f"            UNION "
                f"        {prev_selects} "
                f"    )"
            )
            if prev_selects
            else ""
        )

        sql = with_tree_sql(tkey, table_name_ext, parent_val, extra_clause) + "SELECT * FROM `tree`"
        rows = config["db"].execute(sql).fetchall()
        # If there is a row in the tree whose parent is the to-be-inserted child, then
        # inserting the new row would result in a cycle.
        if [row for row in rows if row[1] == child_val]:
            rows.append((child_val, parent_val))
            cycle_msg = ", ".join(
                ["({}: {}, {}: {})".format(child_col, row[0], parent_col, row[1]) for row in rows]
            )
            cell["valid"] = False
            cell["messages"].append(
                {
                    "rule": "tree:cycle",
                    "level": "error",
                    "message": (
                        f"Cyclic dependency: {cycle_msg} for tree({parent_col}) of {child_col}"
                    ),
                }
            )
    return cell


def validate_cell_datatype(config, table_name, column_name, cell):
    """
    Given a config map, a table name, a column name, and a cell to validate, validate the cell's
    datatype and return the validated cell.
    """
    column = config["table"][table_name]["column"][column_name]
    primary_dt_name = column["datatype"]
    primary_datatype = config["datatype"][primary_dt_name]
    primary_dt_description = primary_datatype["description"]
    primary_dt_condition_func = primary_datatype.get("compiled_condition")

    def get_datatypes_to_check(dt_name):
        datatypes = []
        if dt_name is not None:
            datatype = config["datatype"][dt_name]
            if (
                datatype["datatype"] != primary_dt_name
                and datatype["compiled_condition"] is not None
            ):
                datatypes.append(datatype)
            datatypes += get_datatypes_to_check(datatype["parent"])
        return datatypes

    if primary_dt_condition_func and not primary_dt_condition_func(cell["value"]):
        cell["valid"] = False
        parent_datatypes = get_datatypes_to_check(primary_dt_name)
        # If this datatype has any parents, check them beginning from the most general to the most
        # specific. We use while and pop() instead of a for loop so as to check conditions in LIFO
        # order:
        while parent_datatypes:
            datatype = parent_datatypes.pop()
            if not datatype["compiled_condition"](cell["value"]):
                cell["messages"].append(
                    {
                        "rule": "datatype:{}".format(datatype["datatype"]),
                        "level": "error",
                        "message": "{} should be {}".format(column_name, datatype["description"]),
                    }
                )
        if primary_dt_description:
            cell["messages"].append(
                {
                    "rule": f"datatype:{primary_dt_name}",
                    "level": "error",
                    "message": f"{column_name} should be {primary_dt_description}",
                }
            )
    return cell


def validate_cell_rules(config, table_name, column_name, context, cell):
    """
    Given a config map, a table name, a column name, the row context, and the cell to validate,
    look in the rule table (if it exists) and validate the cell according to any applicable rules.
    """

    def check_condition(condition_type, cell, rule):
        condition = rule[f"{condition_type} condition"]
        if condition in ["null", "not null"]:
            return (condition == "null" and cell.get("nulltype")) or (
                condition == "not null" and not cell.get("nulltype")
            )
        else:
            compiled_condition = rule[f"compiled {condition_type} condition"]
            return compiled_condition(cell["value"])

    if (
        not config.get("rule")
        or not config["rule"].get(table_name)
        or not config["rule"][table_name].get(column_name)
    ):
        return cell

    applicable_rules = config["rule"][table_name][column_name]
    for rule_number, rule in enumerate(applicable_rules, start=1):
        if check_condition("when", cell, rule):
            if not check_condition("then", context[rule["then column"]], rule):
                cell["valid"] = False
                cell["messages"].append(
                    {
                        "rule": f"rule:{column_name}-{rule_number}",
                        "level": rule["level"],
                        "message": rule["description"],
                    }
                )
    return cell


def validate_unique_constraints(
    config, table_name, column_name, cell, context, prev_results, existing_row, row_number
):
    """
    Given a config map, a table name, a column name, a cell to validate, the row, `context`,
    to which the cell belongs, and a list of previously validated rows (dicts mapping column names
    to column values), check the cell value against any unique-type keys that have been defined for
    the column. If there is a violation, indicate it with an error message attached to the cell. If
    the `existing_row` flag is set to True, then checks will be made as if the given `row_number`
    does not exist in the table.
    """

    # If the column has a primary or unique key constraint, or if it is the child associated with
    # a tree, then if the value of the cell is a duplicate either of one of the previously validated
    # rows in the batch, or a duplicate of a validated row that has already been inserted into the
    # table, mark it with the corresponding error:
    constraints = config["constraints"]
    is_primary = column_name in constraints["primary"][table_name]
    is_unique = False if is_primary else column_name in constraints["unique"][table_name]
    is_tree_child = column_name in [c["child"] for c in constraints["tree"][table_name]]

    def make_error(rule):
        return {
            "rule": rule,
            "level": "error",
            "message": "Values of {} must be unique".format(column_name),
        }

    if any([is_primary, is_unique, is_tree_child]):
        with_sql = ""
        except_table = table_name + "_exc"
        if existing_row:
            with_sql = safe_sql(
                f"WITH `{except_table}` AS ( "
                f"  SELECT * FROM `{table_name}` "
                f"  WHERE `row_number` <> :value "
                f") ",
                {"value": row_number},
            )

        query_table = except_table if with_sql else table_name
        query = with_sql + safe_sql(
            f"SELECT 1 FROM `{query_table}` WHERE `{column_name}` = :value LIMIT 1",
            {"value": cell["value"]},
        )

        if [
            p[column_name]
            for p in prev_results
            if p[column_name]["value"] == cell["value"] and p[column_name]["valid"]
        ] or config["db"].execute(query).fetchall():
            cell["valid"] = False
            if is_primary or is_unique:
                cell["messages"].append(make_error("key:primary" if is_primary else "key:unique"))
            if is_tree_child:
                cell["messages"].append(make_error("tree:child-unique"))
    return cell


def with_tree_sql(tree, table_name, root, extra_clause=""):
    """
    Given a dict representing a tree constraint, a table name, a root from which to generate a
    sub-tree of the tree, and an extra SQL clause, generate the SQL for a WITH clause representing
    the sub-tree.
    """
    child_col = tree["child"]
    parent_col = tree["parent"]
    return safe_sql(
        f"WITH RECURSIVE `tree` AS ( "
        f"{extra_clause} "
        f"    SELECT `{child_col}`, `{parent_col}` "
        f"        FROM `{table_name}` "
        f"        WHERE `{child_col}` = :parent_val "
        f"        UNION ALL "
        f"    SELECT `t1`.`{child_col}`, `t1`.`{parent_col}` "
        f"        FROM `{table_name}` AS `t1` "
        f"        JOIN `tree` AS `t2` ON `t2`.`{parent_col}` = `t1`.`{child_col}`"
        f") ",
        {"parent_val": root},
    )


def validate_under(config, table_name):
    """
    Validate any associated 'under' constraints for the current column.
    """
    ukeys = [ukey for ukey in config["constraints"]["under"][table_name]]
    results = []
    for ukey in ukeys:
        tree_table = ukey["ttable"]
        tree_child = ukey["tcolumn"]
        column = ukey["column"]
        tree = [
            tkey
            for tkey in config["constraints"]["tree"][ukey["ttable"]]
            if tkey["child"] == ukey["tcolumn"]
        ].pop()
        tree_parent = tree["parent"]

        # For each value of the column to be checked:
        # (1) Determine whether it is in the tree's child column.
        # (2) Create a sub-tree of the given tree whose root is the given "under value"
        #     (i.e., ukey["value"]). Now on the one hand, if the value to be checked is in the
        #     parent column of that sub-tree, then it follows that that value is _not_ under the
        #     under value, but above it. On the other hand, if the value to be checked is not in the
        #     parent column of the sub-tree, then if condition (1) is also satisfied it follows that
        #     it _is_ under the under_value.
        #     Note that "under" is interpreted in the inclusive sense; i.e., values are trivially
        #     understood to be under themselves.
        sql = (
            with_tree_sql(tree, ukey["ttable"], ukey["value"]) + f"SELECT "
            f"  `row_number`, "
            f"  `{table_name}`.`{column}`, "
            f"  CASE "
            f"    WHEN `{table_name}`.`{column}_meta` IS NOT NULL "
            f"      THEN JSON(`{table_name}`.`{column}_meta`) "
            '     ELSE JSON(\'{"valid": true, "messages": []}\') '
            f"  END AS `{column}_meta`, "
            f"  CASE "
            f"    WHEN `{table_name}`.`{column}` IN ( "
            f"      SELECT `{tree_child}` FROM `{tree_table}` "
            f"    ) "
            f"    THEN 1 ELSE 0 "
            f"  END, "
            f"  CASE "
            f"    WHEN `{table_name}`.`{column}` IN ( "
            f"      SELECT `{tree_parent}` FROM `tree` "
            f"    ) "
            f"    THEN 0 ELSE 1 "
            f"  END "
            f"FROM `{table_name}`"
        )

        rows = config["db"].execute(sql).fetchall()
        for row in rows:
            meta = json.loads(row[2])
            # If the value in the parent column is legitimately empty, then just skip this row:
            if meta.get("nulltype"):
                continue

            # If the value in the column already contains a different error, its value will be null
            # and it will be returned by the above query regardless of whether it is valid or
            # invalid. So we need to check the value from the meta column instead.
            column_val = meta["value"] if row[1] is None else row[1]

            if row[3] == 0:
                meta["valid"] = False
                meta["value"] = column_val
                meta["messages"].append(
                    {
                        "rule": "under:not-in-tree",
                        "level": "error",
                        "message": (
                            f"Value {column_val} of column {column} is not in "
                            f"{tree_table}.{tree_child}"
                        ),
                    }
                )
                results.append({"row_number": row[0], "column": column, "meta": meta})
            elif row[4] == 0:
                meta["valid"] = False
                meta["value"] = column_val
                under_value = ukey["value"]
                meta["messages"].append(
                    {
                        "rule": "under:not-under",
                        "level": "error",
                        "message": (
                            f"Value '{column_val}' of column {column} is not under '{under_value}'"
                        ),
                    }
                )
                results.append({"row_number": row[0], "column": column, "meta": meta})

    return results


def validate_tree_foreign_keys(config, table_name):
    """
    Given a config map and a table name, validate whether there is a 'foreign key' violation for
    any of the table's trees; i.e., for a given tree: tree(child) which has a given parent column,
    validate that all of the values in the parent column are in the child column.
    """
    tkeys = [tkey for tkey in config["constraints"]["tree"][table_name]]
    results = []
    for tkey in tkeys:
        child_col = tkey["child"]
        parent_col = tkey["parent"]
        rows = (
            config["db"]
            .execute(
                f"SELECT "
                f"  t1.row_number, t1.`{parent_col}`, "
                f"  CASE "
                f"    WHEN t1.`{parent_col}_meta` IS NOT NULL "
                f"      THEN JSON(t1.`{parent_col}_meta`) "
                '     ELSE JSON(\'{"valid": true, "messages": []}\') '
                f"  END AS `{parent_col}_meta` "
                f"FROM `{table_name}` t1 "
                f"WHERE NOT EXISTS ( "
                f"    SELECT 1 "
                f"    FROM `{table_name}` t2 "
                f"    WHERE t2.`{child_col}` = t1.`{parent_col}` "
                f")"
            )
            .fetchall()
        )

        for row in rows:
            meta = json.loads(row[2])
            # If the value in the parent column is legitimately empty, then just skip this row:
            if meta.get("nulltype"):
                continue

            # If the parent column already contains a different error, its value will be null and it
            # will be returned by the above query regardless of whether it actually violates the
            # tree's foreign constraint. So we need to check the value from the meta column instead.
            parent_val = row[1]
            if parent_val is None:
                parent_val = meta["value"]
                rows = config["db"].execute(
                    safe_sql(
                        f"SELECT 1 FROM `{table_name}` WHERE `{child_col}` = :parent_val LIMIT 1",
                        {"parent_val": parent_val},
                    )
                )
                if rows.fetchall():
                    continue

            meta["valid"] = False
            meta["value"] = parent_val
            meta["messages"].append(
                {
                    "rule": "tree:foreign",
                    "level": "error",
                    "message": (
                        f"Value {parent_val} of column {parent_col} is not in column {child_col}"
                    ),
                }
            )
            results.append({"row_number": row[0], "column": parent_col, "meta": meta})
    return results
