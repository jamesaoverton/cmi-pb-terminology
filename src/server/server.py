#!/usr/bin/env python3

import gizmos.tree
import gizmos.search
from flask import Flask, request, render_template_string
from jinja2 import Environment, BaseLoader

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

class TermLoader(BaseLoader):
    def __init__(self, db, id, predicate_ids):
        self.db = db
        self.id = id
        self.predicate_ids = predicate_ids
    def get_source(self, environment, template):
        source = gizmos.tree.tree(self.db, self.id, href="./{curie}", predicate_ids=self.predicate_ids, include_search=True, standalone = False)
        source = "{% extends './templates/layout.html' %}\n{% block content %}\n" + source + "\n{% endblock %}"
        return source, None, lambda: False

@app.route('/')
@app.route('/<id>')
def cmi(id=None):
    if request.args and "text" in request.args:
        x = gizmos.search.search(db, request.args["text"])
    else:



        # db = "build/cmi-pb.db"
        # x = gizmos.tree.tree(db, id, href="./{curie}", predicate_ids=predicate_ids, include_search=True, standalone = False)
        # source = "{% extends './templates/layout.html' %}\n{% block content %}\n" + source + "\n{% endblock %}"
        # return render_template_string(source)
    # y = open("src/server/templates/header.html", "r").read()
    # body_ind = x.find("<body")
    # end_ind = x.find(">", body_ind)
    # end_body = x.find("</body")
    # fin = x[:(end_ind + 1)] + "\n"+ y + "\n"+x[(end_ind+1):end_body] + "\n" + open("src/server/templates/footer.html", "r").read() + x[end_body]
    return x
