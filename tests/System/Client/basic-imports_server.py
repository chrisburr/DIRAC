#! /usr/bin/env python
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
""" Just importing stuff that should be present
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
# pylint: disable=unused-import,import-error

import pyparsing
import XRootD
import gfal2
import stomp
import requests
# import futures
import certifi
import pexpect
import fts3
import LbPlatformUtils
import LbEnv
import six

from distutils.spawn import find_executable


if six.PY3:
  cmds = ['voms-proxy-init', 'voms-proxy-info']
else:
  cmds = ['voms-proxy-init2', 'voms-proxy-info2']

for cmd in cmds:
  res = find_executable(cmd)
  if not res:
    raise RuntimeError()

if six.PY2:
  for cmd in ['glite-ce-job-submit', 'glite-ce-job-status', 'glite-ce-delegate-proxy', 'glite-ce-job-cancel']:
    res = find_executable(cmd)
    if not res:
      raise RuntimeError("No %s" % cmd)

for cmd in ['condor_submit', 'condor_history', 'condor_q', 'condor_rm', 'condor_transfer_data']:
  res = find_executable(cmd)
  if not res:
    raise RuntimeError("No %s" % cmd)

res = find_executable('ldapsearch')
if not res:
  raise RuntimeError()
