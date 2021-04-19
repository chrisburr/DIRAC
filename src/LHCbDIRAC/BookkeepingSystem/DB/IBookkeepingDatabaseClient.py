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
"""Database interface."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"


class IBookkeepingDatabaseClient(object):
  """stores a Entity manager and expose its method."""
  #############################################################################

  def __init__(self, databaseManager):
    """initialize a manager."""
    self.databaseManager_ = databaseManager

  def __getattr__(self, name):
    # This allows the dir() method to work as well as tab completion in ipython
    if name == '__dir__':
      return self.getManager().__getattr__()  # pylint: disable=no-member
    return getattr(self.getManager(), name)

  #############################################################################
  def getManager(self):
    """current manager."""
    return self.databaseManager_
