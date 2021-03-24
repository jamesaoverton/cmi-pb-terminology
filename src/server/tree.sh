#!/usr/bin/env bash
#
# This simple CGI script helps create a tree browser for ONTIE

cd ../..

TITLE="CMI-PB Terminology"
URL="http://example.com?${QUERY_STRING}"
ID=$(urlp --query --query_field=id "${URL}")
DB=$(urlp --query --query_field=db "${URL}")
TEXT=$(urlp --query --query_field=text "${URL}")
BRANCH=$(git branch --show-current)

if [[ ${DB} ]]; then
    # Check that the sqlite database exists
    DB_PATH="build/${DB}.db"
    if ! [[ -s "${DB_PATH}" ]]; then
    	make "build/predicates.txt" "${DB_PATH}" > /dev/null 2>&1
    fi

    echo "Content-Type: text/html"
    echo ""

    # Generate the tree view
    if [[ ${TEXT} ]]; then
    	python3 -m gizmos.search --short-label ID "${DB_PATH}" "${TEXT}"
    elif [[ ${ID} ]]; then
    	python3 -m gizmos.tree "${DB_PATH}" ${ID} -d -P build/predicates.txt -t "${TITLE}" --include-search
    else
    	python3 -m gizmos.tree "${DB_PATH}" -d -P build/predicates.txt -t "${TITLE}" --include-search
    fi
else
    echo "Content-Type: text/html"
    echo ""
    echo "<h3>Select a tree:</h3>"
    echo "<ul>"
    echo "<li><a href=\"?db=cmi-pb\">CMI-PB</a></li>"
    # echo "<li><b>Imports:</b></li>"
    # echo "<ul>"
    # echo "<li><a href=\"?db=doid&id=DOID:4\">Human Disease Ontology (DOID)</a></li>"
    # echo "<li><a href=\"?db=obi\">Ontology for Biomedical Investigations (OBI)</a></li>"
    # echo "</ul>"
    echo "</ul>"
    echo "<p>If you are selecting a tree for the first time, it may take some time to build the database!</p>"
    echo "<a href=\"/CMI-PB/branches/${BRANCH}\"><b>Return Home</b></a>"
fi
