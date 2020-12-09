#!/usr/bin/env python3

import gizmos.tree
import gizmos.search
from flask import Flask, request, render_template_string

app = Flask(__name__)
predicate_ids = [
  "rdfs:label",
  "IAO:0000118",
  "IAO:0000115",
  "IAO:0000119",
  "IAO:0000112",
  "rdf:type",
  "rdfs:subClassOf",
]

@app.route('/')
@app.route('/<id>')
def cmi(id=None):
    db = "build/cmi-pb.db"
    if request.args and "text" in request.args:
        source = gizmos.search.search(db, request.args["text"])
        return source
    else:
        source = gizmos.tree.tree(db, id, href="./{curie}", predicate_ids=predicate_ids, include_search=True, standalone = False)
    source = "{% extends 'layout.html' %}\n{% block content %}\n" + source + "\n{% endblock %}"
    return render_template_string(source)
