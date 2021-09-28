###############################################################################
# (c) Copyright 2019 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                       #
###############################################################################
from __future__ import print_function

import argparse
import os
import sys

try:
    import gitlab
except ImportError:
    print("gitlab api not available. pip install python-gitlab")
    sys.exit(1)


def removeTags(projectID, token, pattern, dry_run=False):

    gl = gitlab.Gitlab("https://gitlab.cern.ch", private_token=token)
    gl.auth()
    project = gl.projects.get(projectID)
    repo = project.repositories.list()[0]
    tags = repo.tags.list(all=True)
    toRemove = [tag for tag in tags if tag.attributes["name"].startswith(pattern)]

    if not toRemove:
        print("Nothing to remove")
        return
    if dry_run:
        print("DRY RUN: Would delete", [tag.attributes["name"] for tag in toRemove])
    else:
        print("DELETING")
        for tag in toRemove:
            print(tag.attributes["name"])
            tag.delete()


def main():
    parser = argparse.ArgumentParser(description="Clean Gitlab registry")
    parser.add_argument("-p", "--projectID", type=int, help="ID of the Gitlab project (LHCbDIRAC: 3588)", required=True)
    parser.add_argument("-t", "--token", help="Token for the Gitlab API", required=True)
    parser.add_argument("-m", "--match", help="Match tags starting with this string", required=True, dest="pattern")
    parser.add_argument("-d", "--dry-run", help="Do not perform the delete", action="store_true")
    args = parser.parse_args()

    removeTags(**vars(args))


if __name__ == "__main__":
    main()
