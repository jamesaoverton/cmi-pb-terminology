#!/usr/bin/env python3

import gizmos.tree
import gizmos.search

from flask import Flask, request, render_template, Response

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

@app.route("/hook", methods=["POST"])
def update():
    print("REQUEST", request.json)
    return Response(status=200)

@app.route("/")
@app.route("/<id>")
def cmi(id=None):
    db = "build/cmi-pb.db"
    if request.args and "text" in request.args:
        return gizmos.search.search(db, request.args["text"])
    else:
        html = gizmos.tree.tree(db, id, title="CMI-PB Terminology", href="./{curie}", predicate_ids=predicate_ids, include_search=False, standalone=True)
        return html #render_template("base.html", content=html)

