# cmi-pb-terminology

CMI-PB Controlled Terminology

This repository contains:

1. code and data for building the terminology into an OWL file
2. a Python library for browsing, searching, and updating the terminology
3. a basic Flask server for testing

## 1. Building the Terminology

Requirements: GNU Make, Java 8+.

We use [ROBOT](http://robot.obolibrary.org) to build the ontology,
following common OBO development patterns.
Just run `make` to rebuild the `cmi-pb.owl` file.

## 2. Using the Terminology

You can install the Python library via `pip`:

```
pip install git+https://github.com/jamesaoverton/cmi-pb-terminology.git
```

The package, called `terminology`, has two functions:

- `search`: accepts text and returns the JSON search results (based on labels)
- `term`: accepts a term ID and returns the HTML tree browser for `build/cmi-pb.db` at that term

## 3. Test Server

We provide a simple Flask server that will serve terminology pages without
the need to build the ontology.
