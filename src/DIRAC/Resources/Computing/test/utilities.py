#!/bin/env python
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import time


jobScript = """#!/usr/bin/env python

from __future__ import print_function

import time
import os

jobNumber = %s
stopFile = 'stop_job_' + str(jobNumber)
start = time.time()

print("Start job", jobNumber, start)
while True:
  time.sleep(0.1)
  if os.path.isfile(stopFile):
    os.remove(stopFile)
    break
  if (time.time() - start) > 30:
    break
print("End job", jobNumber, time.time())
"""


def _stopJob(nJob):
  with open('stop_job_%s' % nJob, 'w') as stopFile:
    stopFile.write('Stop')
  time.sleep(0.2)
  if os.path.isfile('stop_job_%s' % nJob):
    os.remove('stop_job_%s' % nJob)
