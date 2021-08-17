=============
Sandbox Store
=============

Our Sandbox store is hosted on a VM (currently lbvobox112) and the files are stored on an external volume (currently mounted under `/opt/dirac/sandboxStore/`)


How To resize the sandbox store volume
======================================

Resize the sandbox volume since it is full. The normal procedure can be found here:
https://clouddocs.web.cern.ch/clouddocs/details/working_with_volumes.html

The volume has not been resized since the volume was moved to a 5TB volume with better IO performance.
