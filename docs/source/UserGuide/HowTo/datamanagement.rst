.. _howto_user_lhcb_dms:

==============
DataManagement
==============


For an introduction about DataManagement concepts, please see the :ref:`introduction <dms-concepts>`

All the commands mentioned below can accept several StorageElements and LFNs as parameters. Please use `--help` for more details.

Basics
======

Check if a file is corrupted
----------------------------

This question normally arises when a job spits lines like::

    Error in <TBufferFile::CheckByteCount>: object of class LHCb::PackedRelation read too few bytes: 2 instead of 1061133141
    Error in <TBufferFile::CheckByteCount>: Byte count probably corrupted around buffer position 21698:
      1061133141 for a possible maximum of -6

    Error in <TBufferFile::ReadClassBuffer>: class: DataObject, attempting to access a wrong version: -25634, object skipped at offset 4044

Or something like::

    R__unzipLZMA: error 9 in lzma_code
    Error in <TBasket::ReadBasketBuffers>: fNbytes = 28617, fKeylen = 92, fObjlen = 103039, noutot = 0, nout=0, nin=28525, nbuf=103039

We know that there are bugs in some applications that produce files that it then can't read, but from the pure data management point of view, we consider the file good if the checksum stored in the catalog and the actual file checksum match. To check it, we need to download the file locally, compute the checksum, and compare it with the DFC.

For example::

    # Copy the file locally
    # (the URL can be obtained from the failing job, or from dirac-dms-lfn-accessURL, or one can even download the file with dirac-dms-get-file)

    bash-4.2$ gfal-copy root://xrootd.grid.surfsara.nl//pnfs/grid.sara.nl/data/lhcb/LHCb-Disk/lhcb/LHCb/Collision18/CHARMCHARGED.MDST/00077052/0000/00077052_00008134_1.charmcharged.mdst .       Copying root://xrootd.grid.surfsara.nl//pnfs/grid.sara.nl/data/lhcb/LHCb-Disk/lhcb/LHCb/Collision18/CHARMCHARGED.MDST/00077052/0000/00077052_00008134_1.charmcharged.mdst
    [...]
    Copying root://xrootd.grid.surfsara.nl//pnfs/grid.sara.nl/data/lhcb/LHCb-Disk/lhcb/LHCb/Collision18/CHARMCHARGED.MDST/00077052/0000/00077052_00008134_1.charmcharged.mdst...  69s 100% [====>]Copying root://xrootd.grid.surfsara.nl//pnfs/grid.sara.nl/data/lhcb/LHCb-Disk/lhcb/LHCb/Collision18/CHARMCHARGED.MDST/00077052/0000/00077052_00008134_1.charmcharged.mdst   [DONE]  after 69s

    # compute the checksum
    bash-4.2$ xrdadler32 00077052_00008134_1.charmcharged.mdst
    7f84828f 00077052_00008134_1.charmcharged.mdst

    # Compare it with the DFC
    bash-4.2$ dirac-dms-lfn-metadata /lhcb/LHCb/Collision18/CHARMCHARGED.MDST/00077052/0000/00077052_00008134_1.charmcharged.mdst
    Successful :
        /lhcb/LHCb/Collision18/CHARMCHARGED.MDST/00077052/0000/00077052_00008134_1.charmcharged.mdst :
                    Checksum : 7f84828f
                ChecksumType : Adler32
                CreationDate : 2018-08-16 02:53:48
                      FileID : 368851163
                        GID : 2749
                        GUID : 4606A403-E5A0-E811-ACFA-001B21B993CC
                        Mode : 775
            ModificationDate : 2018-08-16 02:53:48
                      Owner : fstagni
                  OwnerGroup : lhcb_data
                        Size : 5059895291
                      Status : AprioriGood
                        UID : 19727


If the checksums don't match, the file needs to be recovered. If it is your own user file, do as you please (remove, recreate, replicate, etc). If it is centrally managed, please contact ``lhcb-datamanagement`` mailing list