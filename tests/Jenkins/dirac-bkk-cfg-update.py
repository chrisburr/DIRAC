#!/usr/bin/env python
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
""" Update local configuration file for the bookkeeping database

The configuration is set as follows:
    System
    {
      Bookkeeping
      {
        Production
        {
          Databases
          {
            BookkeepingDB
            {
              LHCbDIRACBookkeepingUser=LHCB_DIRACBOOKKEEPING_INT_R
              LHCbDIRACBookkeepingServer=LHCB_DIRACBOOKKEEPING_INT_W
              LHCbDIRACBookkeepingPassword=password
              LHCbDIRACBookkeepingTNS=int12r
            }
          }
        }
      }
    }

    Operations
    {
      Defaults
      {
        Services
        {
          Catalogs
          {
            BookkeepingDB
            {
              AccessType=Write
              Status=Active
              Conditions
              {
                WRITE=Proxy=group.not_in(lhcb_user)
              }
            }
          }
        }
      }

    FileCatalogs
    {
      BookkeepingDB
      {
        AccessType = Read-Write
        Status = Active
        Master = True
      }
    }
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import sys

from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
from DIRAC.Core.Base import Script
from DIRAC import gLogger


def parse_args():
  parser = argparse.ArgumentParser()
  parser.add_argument('--read-username', default='LHCB_DIRACBOOKKEEPING_INT_R',
                      help='Read-only account username')
  parser.add_argument('--write-username', default='LHCB_DIRACBOOKKEEPING_INT_W',
                      help='Read-write account username')
  parser.add_argument('-p', '--password', required=True,
                      help='Password of the database accounts')
  parser.add_argument('--host', default='int12r',
                      help='Hostname of the bookkeeping database')
  parser.add_argument('-d', '--debug', action='count', default=0)
  args = parser.parse_args()

  sys.argv = ['-' + 'd' * args.debug] if args.debug else []
  Script.parseCommandLine()
  set_configuration(args.read_username, args.write_username, args.password, args.host)


def set_configuration(read_username, write_username, password, host):
  csAPI = CSAPI()

  for sct in ['Systems/Bookkeeping',
              'Systems/Bookkeeping/Production',
              'Systems/Bookkeeping/Production/Databases',
              'Systems/Bookkeeping/Production/Databases/BookkeepingDB',
              'Operations',
              'Operations/Defaults',
              'Operations/Defaults/Services',
              'Operations/Defaults/Services/Catalogs',
              'Operations/Defaults/Services/Catalogs/BookkeepingDB',
              'Operations/Defaults/Services/Catalogs/BookkeepingDB/Conditions']:
    res = csAPI.createSection(sct)
    if not res['OK']:
      gLogger.error(res['Message'])
      exit(1)

  csAPI.setOption('Systems/Bookkeeping/Production/Databases/BookkeepingDB/LHCbDIRACBookkeepingUser', read_username)
  csAPI.setOption('Systems/Bookkeeping/Production/Databases/BookkeepingDB/LHCbDIRACBookkeepingServer', write_username)
  csAPI.setOption('Systems/Bookkeeping/Production/Databases/BookkeepingDB/LHCbDIRACBookkeepingPassword', password)
  csAPI.setOption('Systems/Bookkeeping/Production/Databases/BookkeepingDB/LHCbDIRACBookkeepingTNS', host)

  csAPI.setOption('Operations/Defaults/Services/Catalogs/BookkeepingDB/AccessType', 'Write')
  csAPI.setOption('Operations/Defaults/Services/Catalogs/BookkeepingDB/Status', 'Active')
  csAPI.setOption('Operations/Defaults/Services/Catalogs/BookkeepingDB/Conditions/WRITE',
                  'Proxy=group.not_in(lhcb_user)')

  for sct in ['Resources/FileCatalogs',
              'Resources/FileCatalogs/BookkeepingDB']:
    res = csAPI.createSection(sct)
    if not res['OK']:
      print(res['Message'])
      exit(1)

  csAPI.setOption('Resources/FileCatalogs/BookkeepingDB/AccessType', 'Write')
  csAPI.setOption('Resources/FileCatalogs/BookkeepingDB/Status', 'Active')
  csAPI.setOption('Resources/FileCatalogs/BookkeepingDB/CatalogURL', 'Bookkeeping/BookkeepingManager')

  csAPI.commit()


if __name__ == '__main__':
  parse_args()
