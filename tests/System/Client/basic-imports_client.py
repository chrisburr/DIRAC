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
import GSI
import XRootD
import gfal2
import stomp
import requests
# import futures
import certifi
import fts3
import LbPlatformUtils
import LbEnv


from distutils.spawn import find_executable

for cmd in ['voms-proxy-init2', 'voms-proxy-info2', ]:
  res = find_executable(cmd)
  if not res:
    raise RuntimeError()
