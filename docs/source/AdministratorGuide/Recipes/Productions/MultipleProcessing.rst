===========================================================================
What to do if some files have been processed multiply within a production ?
===========================================================================

Here the example is for stripping `<STRIPPING>` and merging `<MERGING>`

0. Quickly stop the merging productions as well as the stripping productions!
=============================================================================

Good to also stop the removal productions if any

1. Check whether their output was merged
========================================
::

    grep ProcMultDesc CheckDescendantsResults_<STRIPPING>.txt | dirac-bookkeeping-get-file-descendants —Prod <STRIPPING> —All | dirac-transformation-debug <MERGING>

Look at whether the multiple descendants have been merged, in which case they are set Processed in the final list. You can also see all files that are eventually already part of merging jobs (status Assigned).

If the number of RDST files is small and only 1 or 2 streams have been merged, one may afford just removing the merged files (i.e. the output of the merging jobs that ran): select the Processed files above and do::

    dirac-bookkeeping-get-file-descendants —Term
    <paste list of Processed files>

Else, one needs to remove the whole runs that are affected :(

2. Get the list of runs
=======================

::

    grep ProcMultDesc CheckDescendantsResults_<STRIPPING>.txt | dirac-bookkeeping-file-path —Summary —GroupBy RunNumber —List

This prints out how many files per run and at the end the list of affected runs… Now starts the fun!

3. Get the list of files to be removed
======================================

---------------------------------------------
3.0 Check whether some files are being merged
---------------------------------------------

::

    dirac-transformation-debug <MERGING> —Status Assigned —Run <list of runs>

As long as you have files Assigned, better do nothing.
if you are in a hurry, you may kill the corresponding jobs and proceed, but this may cause troubles…

----------------
3.1 Merged files
----------------

::

    dirac-bookkeeping-get-files —Prod <MERGING> —Run <list of runs> —Output toRemove<STRIPPING>-merged.txt

--------------------
3.2 Non-merged files
--------------------

::

    dirac-bookkeeping-get-files —Prod <STRIPPING> —Run <list of runs> —Output toRemove<STRIPPING>-nonMerged.txt

if this fails (there is a bug in the BK, but could be fixed ;-)

::

    dirac-loop —Item <list of runs> ‘dirac-bookkeeping-get-files —Prod <STRIPPING> —Run @arg@:@arg@ —Visi No’ > toRemove<STRIPPING>-nonMerged.txt

----------------------
3.3 Remove these files
----------------------

Firstly enable the xxx-ARCHIVE SEs for removal::

    dirac-dms-replica-stats —File toRemove<STRIPPING>-merged.txt

will give you the list of ARCHIVE SEs concerned... ::

    dirac-admin-enable-se —AllowRemove <list of ARCHIVE SEs >

    dirac-transformation-add-files —File toRemove<STRIPPING>-merged.txt,toRemove<STRIPPING>-nonMerged.txt Remove-all-replicas-CleanTrans

This is unfortunately not enough as you should set Removed the files that were Processed already, since the transformation above would not change status of Processed files...::

    dirac-bookkeeping-get-file-ancestors —File toRemove<STRIPPING>-merged.txt | dirac-transformation-reset-files —NewStatus Removed <MERGING>


4. Get the list of RDST files to reprocess and reset them
=========================================================

------------------------------------------------
4.0 Check whether some files are being processed
------------------------------------------------

::

    dirac-transformation-debug <STRIPPING> —Status Assigned —Run <list of runs>

As long as you have files Assigned, better do nothing
If you are in a hurry, you may kill the corresponding jobs and proceed, to your own risk!

--------------------------
4.1 Get list of RDST files
--------------------------

::

    dirac-bookkeeping-get-files —BK <BK path> —Run <list of runs> —Output rdstToReprocess-<STRIPPING>.txt

--------------------------------------------
4.2 Re-stage these files and their ancestors
--------------------------------------------

::

    dirac-transformation-add-files —File rdstToReprocess-<STRIPPING>.txt  PreStage-with-ancestors

or if this transformation doesn’t exist::

    dirac-dms-add-transformation —Plugin ReplicateWithAncestors —File rdstToReprocess-<STRIPPING>.txt  —Name PreStage-with-ancestors —Start

-------------------------------------------------------
4.3 Reset RDST files Unused in the stripping production
-------------------------------------------------------

Get the status of those files in the stripping production, in case they are already set Problematic or NotProcessed::

    dirac-transformation-debug —File rdstToReprocess-<STRIPPING>.txt —Status Problematic,NotProcessed <STRIPPING>

Save the list of files that are in either of these statuses in order to reset them later

When you are sure no file is Assigned::

    dirac-transformation-debug —File rdstToReprocess-<STRIPPING>.txt —Status Assigned <STRIPPING>

returns no files!

::

    dirac-transformation-reset-files —File rdstToReprocess-<STRIPPING>.txt <STRIPPING>

and then reset Problematic files you have saved as such

::

    dirac-transformation-reset-files —NewStatus Problematic —Term <STRIPPING>
    <paste the list of Problematic >

    dirac-transformation-reset-files —NewStatus NotProcessed —Term <STRIPPING>
    <paste the list of NotProcessed>


5. Make a few more checks
=========================

From time to time better do a further check that all is OK as you may have also other errors like files in FC without BK flag, in which case you should removed them::

    dirac-production-check-descendants —File rdstToReprocess-<STRIPPING>.txt <STRIPPING>

6. Restart productions
======================

Restart the stripping production first, do a few more checks as in 5. and when confident, restart the merging production

7. Not to be forgotten
======================

At the end of the productions, one should remove from Tier1-Buffer the RDST files that have been re-processed as well as their RAW ancestors. This is an additional duty for the DM cleaning that anyway must take place. If all is OK it is enough to just remove all files and ancestors from Buffer…
