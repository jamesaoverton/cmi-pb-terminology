#!/bin/sh
trap 'exit 0' 1 2 15
. .venv/bin/activate
export FLASK_ENV=development
export FLASK_APP=src/server/server.py
flask run
