#!/bin/python

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
import os
import time
import sys
try:
  from commands import getstatusoutput
except ImportError:
  from subprocess import getstatusoutput

print('**************************')
print('START myPythonScript.py')
print('**************************')
sys.stdout.flush()
time.sleep(30)
print('Hi this is a test')
print('hope it works...')
sys.stdout.flush()
root = os.getcwd()
print('we are here: ', root)
print('the files in this directory are:')
status,result = getstatusoutput('ls -al')
print(result)
#time.sleep(80)
sys.stdout.flush()
print('trying to see the local environment:')
status,result = getstatusoutput('env')
time.sleep(30)
print(result)
print('bye.')
print('**************************')
print('END myPythonScript.py')
print('**************************')


