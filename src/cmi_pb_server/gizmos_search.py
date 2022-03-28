from sqlalchemy.sql.expression import text as sql_text


def simple_search(
    conn, label="rdfs:label", limit=None, search_text="", statement="statement", terms=None
):
    if not search_text and not terms:
        return []

    query = f"""SELECT DISTINCT subject, object FROM "{statement}"
                WHERE predicate = :label AND lower(object) LIKE :text"""
    if terms:
        # TODO: change to SQL vars, chunk by 999
        subject_in = " AND subject IN (" + ", ".join([f"'{x}'" for x in terms]) + ")"
        query += subject_in
    query += " ORDER BY LENGTH(object)"
    if limit:
        query += f" LIMIT {limit}"
    query = sql_text(query)

    i = 1
    typeahead_lst = []
    for res in conn.execute(query, label=label, text=f"%%{search_text.lower()}%%"):
        typeahead_lst.append(
            {
                "id": res["subject"],
                "label": res["object"],
                "short_label": None,
                "synonym": None,
                "property": "rdfs:label",
                "order": i,
            }
        )
        i += 1
    return typeahead_lst
