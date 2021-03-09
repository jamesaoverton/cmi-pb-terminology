#!/usr/bin/env python3
#
# Update a file from a target URL,
# only if it is newer than the last update
# (according to its ETag).
# Keep the last X versions, with timestamps.
# Link to the latest version.

import os
import requests

from datetime import datetime

def update(url, path, keep_num=10):
    """Given a remote URL, a local file path,
    and an optional number of versions to keep,
    download a timestamped copy of the file from the URL
    only if the ETag differs from the last download,
    then link to that file.
    If there are more old files than the number to keep,
    delete the oldest ones."""
    directory, filename = os.path.split(path)
    directory = directory or "."
    basename, extension = os.path.splitext(filename)

    # Script configuration
    header = os.path.join(directory, "header.txt")
    etag = ""
    if os.path.exists(header) and os.path.islink(path):
        with open(header) as f:
            for line in f:
                if line.lower().startswith("etag:"):
                    etag = line[5:].strip()
                    break

    date = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    timestamped_path = os.path.join(directory, f"{basename}-{date}{extension}")

    response = requests.get(url, headers={
        "If-None-Match": etag
    })
    with open(header, "w") as f:
        for k, v in response.headers.items():
            f.write(f"{k}: {v}\n")

    if response.content:
        with open(timestamped_path, "wb") as f:
            f.write(response.content)
    elif response.text:
        with open(timestamped_path, "w") as f:
            f.write(response.text)

    if os.path.exists(timestamped_path):
        if os.path.islink(path):
            os.unlink(path)
        os.symlink(timestamped_path, path)

    names = []
    for name in os.listdir(directory):
        if os.path.isfile(name) and name.startswith(f"{basename}-"):
            names.append(name)
    names.sort()
    names.reverse()
    for name in names[keep_num:]:
        os.remove(name)


if __name__ == "__main__":
    # User configuration
    #URL="https://droid.ontodev.com/CMI-PB/branches/master/views/build/cmi-pb.db"
    url = "https://www.wikipedia.org/"
    path = "terminology.db"
    keep_num = 10
    update(url, path, keep_num)

