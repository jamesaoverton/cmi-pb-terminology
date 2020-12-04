#!/usr/bin/env python3

import gizmos.tree
import gizmos.search
from flask import Flask, request

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
        x = gizmos.search.search(db, request.args["text"])
    else:
        x = gizmos.tree.tree(db, id, href="./{curie}", predicate_ids=predicate_ids, include_search=True)
    y = open("src/server/templates/header.html", "r").read()
    body_ind = x.find("<body")
    end_ind = x.find(">", body_ind)
    end_body = x.find("</body")
    fin = x[:(end_ind + 1)] + "\n"+ y + "\n"+x[(end_ind+1):end_body] + "\n" + open("src/server/templates/footer.html", "r").read() + x[end_body]
    return fin
