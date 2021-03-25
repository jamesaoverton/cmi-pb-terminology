import csv
import sqlite3

from argparse import ArgumentParser
from collections import defaultdict


def main():
    parser = ArgumentParser()
    parser.add_argument("db")
    parser.add_argument("proteins")
    parser.add_argument("output")
    args = parser.parse_args()

    proteins = []
    with open(args.proteins, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            proteins.append(row["uniprot_id"])
    proteins_str = ", ".join([f"'uniprot_protein:{x}'" for x in proteins])

    details = defaultdict(dict)
    with sqlite3.connect(args.db) as conn:
        cur = conn.cursor()

        # First get the labels, i.e. the recommended names
        print("Getting recommended names...")
        cur.execute(
            f"""SELECT DISTINCT s1.subject, s2.value
                FROM statements s1
                  JOIN statements s2 ON s1.object = s2.subject
                WHERE s1.subject IN ({proteins_str})
                  AND s1.predicate = 'uniprot_core:recommendedName'
                  AND s2.predicate = 'uniprot_core:fullName';"""
        )
        for res in cur.fetchall():
            uniprot = res[0].split(":")[1]
            details[uniprot] = {"label": res[1]}

        # Then get the short labels, i.e. the gene names
        print("Getting genes...")
        cur.execute(
            f"""SELECT DISTINCT s1.subject, s2.value
                FROM statements s1
                  JOIN statements s2 ON s1.object = s2.subject
                WHERE s1.subject IN ({proteins_str})
                  AND s1.predicate = 'uniprot_core:encodedBy'
                  AND s2.predicate = 'skos:prefLabel'"""
        )
        for res in cur.fetchall():
            uniprot = res[0].split(":")[1]
            if uniprot not in details:
                details[uniprot] = {}
            details[uniprot]["short_label"] = res[1]

        # Finally get the synonyms - there may be zero or more
        synonyms = defaultdict(list)
        print("Getting alternative names...")
        cur.execute(
            f"""SELECT DISTINCT s1.subject, s2.value
                FROM statements s1
                  JOIN statements s2 ON s1.object = s2.subject
                WHERE s1.subject IN ({proteins_str})
                  AND s1.predicate = 'uniprot_core:alternativeName'
                  AND s2.predicate = 'uniprot_core:fullName';"""
        )
        for res in cur.fetchall():
            uniprot = res[0].split(":")[1]
            if uniprot not in synonyms:
                synonyms[uniprot] = list()
            synonyms[uniprot].append(res[1])

        for uniprot, syns in synonyms.items():
            details[uniprot]["synonyms"] = "|".join(syns)

    missing = list(set(proteins) - set(details.keys()))
    if missing:
        print(f"WARNING: Missing {len(missing)} protein(s): " + ", ".join(missing))

    rows = []
    for uniprot, det in details.items():
        det["uniprot_id"] = "uniprot:" + uniprot
        det["parent"] = "PR:000000001"
        rows.append(det)

    # Build the ROBOT template
    with open(args.output, "w") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["uniprot_id", "parent", "label", "short_label", "synonyms"],
            delimiter="\t",
            lineterminator="\n",
        )
        writer.writeheader()
        # Write ROBOT template strings
        writer.writerow(
            {
                "uniprot_id": "ID",
                "parent": "SC %",
                "label": "LABEL",
                "short_label": "A CMI-PB:alternativeTerm",
                "synonyms": "A IAO:0000118 SPLIT=|",
            }
        )
        # Write all rows
        writer.writerows(rows)


if __name__ == "__main__":
    main()
