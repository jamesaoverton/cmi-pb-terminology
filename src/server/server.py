#!/usr/bin/env python3

import gizmos.tree
import gizmos.search

from flask import Flask, request, render_template, Response
from terminology import search, term

app = Flask(__name__)


@app.route("/hook", methods=["POST"])
def update():
    print("REQUEST", request.json)
    return Response(status=200)


@app.route("/")
@app.route("/<term_id>")
def cmi(term_id=None):
    if request.args and "text" in request.args:
        return search(request.args["text"])
    else:
        return term(term_id)
