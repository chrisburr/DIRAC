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
""" Module containing a front-end to the ElasticSearch-based ElasticMCGaussLogErrorsDB.
    This module interacts with one ES index: "ElasticMCLogErrors",

    Here we define a mapping which is taken from a list of log errors.
"""

from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

__RCSID__ = "$Id$"

from DIRAC import S_OK
from DIRAC.Core.Base.ElasticDB import ElasticDB


class ElasticMCStats(ElasticDB):

  def get(self, productionID):
    """ Get docs per productionID. Basically here only for tests, right now

    :param self: self reference
    :param int productionID: production ID

    :return: dict with all docs
    """

    self.log.debug('ElasticMCStats.get: Getting for production %s' % productionID)

    resultList = []

    """ the following should be equivalent to
    {
      "query": {
        "bool": {
          "filter": {  # no scoring
            "term": {"ProductionID": productionID}  # term level query, does not pass through the analyzer
          }
        }
      }
    }
    """

    s = self.dslSearch.query("bool", filter=self._Q("term", ProductionID=productionID))

    res = s.execute()

    for hit in res:
      hitDict = {}
      for name in hit:
        hitDict[name] = getattr(hit, name)
      resultList.append(hitDict)

    return S_OK(resultList)

  def set(self, data):
    """
    Inserts data into ElasticJobParametersDB index

    :param self: self reference
    :param str value: data to be inserted

    :returns: S_OK/S_ERROR as result of indexing
    """

    self.log.debug('Inserting data in %s:%s' % (self.indexName, data))

    result = self.index(self.indexName,
                        body=data,
                        docID=data['ProductionID'] + '_' + data['JobID'])
    if not result['OK']:
      self.log.error("ERROR: Couldn't insert data", result['Message'])
    return result
