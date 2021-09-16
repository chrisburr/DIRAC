#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-admin-delete-user
# Author :  Adrian Casajus
########################################################################
"""
Remove User from Configuration

Usage:

  dirac-admin-delete-user [option|cfgfile] ... User ...

Arguments:

  User:     User name

Example:

  $ dirac-admin-delete-user vhamar
"""
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script

Script.setUsageMessage(__doc__)
Script.parseCommandLine(ignoreErrors=True)
args = Script.getPositionalArgs()

from DIRAC import exit as DIRACExit
from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin

diracAdmin = DiracAdmin()
exitCode = 0
errorList = []

if len(args) < 1:
    Script.showHelp()

choice = raw_input(
    "Are you sure you want to delete user/s %s? yes/no [no]: " % ", ".join(args)
)
choice = choice.lower()
if choice not in ("yes", "y"):
    print("Delete aborted")
    DIRACExit(0)

for user in args:
    if not diracAdmin.csDeleteUser(user):
        errorList.append(("delete user", "Cannot delete user %s" % user))
        exitCode = 255

if not exitCode:
    result = diracAdmin.csCommitChanges()
    if not result["OK"]:
        errorList.append(("commit", result["Message"]))
        exitCode = 255

for error in errorList:
    print("ERROR %s: %s" % error)

DIRACExit(exitCode)
