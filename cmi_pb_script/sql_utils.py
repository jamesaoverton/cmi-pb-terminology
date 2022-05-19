from sqlalchemy.sql.expression import text as sql_text


def safe_sql(template, params):
    """Given a SQL query template with variables and a dict of parameters,
    return an escaped SQL string."""
    stmt = sql_text(template).bindparams(**params)
    return str(stmt.compile(compile_kwargs={"literal_binds": True}))
