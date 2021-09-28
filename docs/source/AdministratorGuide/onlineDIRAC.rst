.. _online_dirac:

============
Online DIRAC
============

This page details how the HLT farm is used to run DIRAC

The linux account used is `lhcbprod`, and all the necessary files are under `/home/lhcbprod/production/`.

PVSS will start the script `/home/lhcbprod/production/launch_agent.sh`. This script mostly creates the working directory under `/localdisk1`, sets up a bit of the environment, and start the `/home/lhcbprod/production/dirac-pilot-3.sh` script.


The code for the Pilot itself is store in `/home/lhcbprod/production/Pilot3`. The content of this directory is updated every hour by a cron run on the machine cron02 as lhcbprod.

The script which updates it is `/home/lhcbprod/production/dirac-pilot3-cron.sh`.

===============================
Renewal of the host certificate
===============================

The certificate used to bootstrap the jobs is the same host certificate that is used to run the pit export installation, and is linked to the `lbdirac.cern.ch` alias.

Everyone in the `LHCB.PC-ADMINS` egroup should be able to renew it. Reminders will come many times before it expires.

Procedure:

  - go to the `cern ca website <https://ca.cern.ch/ca/>`_
  - use the tab `New Grid Host Certificate`, and click `Automatic certificate generation`. Do not use any passphrase. This will give you a p12 file
  - convert the p12 file in two PEM files: one for the host certificate, one for the private key. The details commands are given `here <https://ca.cern.ch/ca/Help/?kbid=024100>`_
  - put these two files (`hostcert.pem` and `hostkey.pem`) under `/home/lhcbprod/production/etc`  (note: this location is the one used in `launch_agent.sh`)

Since the same certificate is used for the pit export, you also may want to update these. Just copy the two files under `/sw/dirac/run2/etc/grid-security/`

===============
Troubleshooting
===============

The online PVSS panel can be accessed via `/group/online/ecs/Shortcuts315/OTHER/ONLDIRAC/ONLDIRAC_UI_FSM.sh`
