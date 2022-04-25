#!/usr/bin/env python3.9
import os

from cmi_pb_server.run import run

if __name__ == "__main__":
    os.chdir("..")
    run(
        "build/cmi-pb.db",
        "src/table.tsv",
        cgi_path="/CMI-PB/branches/next/views/src/run.py",
        log_file="app.log",
        title="CMI-PB"
    )
