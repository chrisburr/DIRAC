/* ---------------------------------------------------------------------------#
# (c) Copyright 2019 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                      */

CREATE OR REPLACE package BOOKKEEPINGORACLEDB AS

  TYPE udt_RefCursor IS ref cursor;
  --TYPE ifileslist is VARRAY(30) of VARCHAR2(10);

  TYPE ifileslist IS TABLE OF VARCHAR2(30)
    INDEX BY PLS_INTEGER;

  TYPE numberarray  IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
  TYPE varchararray IS TABLE OF VARCHAR2(256) INDEX BY PLS_INTEGER;
  TYPE bigvarchararray IS TABLE OF VARCHAR2(2000) INDEX BY PLS_INTEGER;
  
PROCEDURE funny(a NUMBER);
FUNCTION  ext RETURN udt_RefCursor;
PROCEDURE getAvailableFileTypes(a_Cursor out udt_RefCursor );
FUNCTION insertFileTypes( v_name VARCHAR2, description VARCHAR2,filetype VARCHAR2) RETURN NUMBER;
PROCEDURE getAvailableConfigurations(a_Cursor out udt_RefCursor);
PROCEDURE getStepsForSpecificIfiles(iftypes ifileslist, a_Cursor out udt_RefCursor);
PROCEDURE getStepsForSpecificOfiles(oftypes ifileslist, a_Cursor out udt_RefCursor);
PROCEDURE getStepsForIfiles(iftypes ifileslist , a_Cursor out udt_RefCursor);
PROCEDURE getStepsForOfiles(oftypes ifileslist, a_Cursor out udt_RefCursor);
PROCEDURE getAvailebleSteps(iftypes ifileslist , a_Cursor out udt_RefCursor); --I can DELETE
PROCEDURE getAvailebleStepsRealAndMC(iftypes ifileslist , a_Cursor out udt_RefCursor); --I can DELETE
FUNCTION getStepsForFiletypes(iftypes lists, oftypes lists, match VARCHAR2) RETURN step_table PIPELINED;
FUNCTION getProductionProcessingPass(prod NUMBER) RETURN VARCHAR2;
FUNCTION getProductionPorcPassName(v_procid NUMBER) RETURN VARCHAR2;
FUNCTION getProductionProcessingPassId(prod NUMBER) RETURN NUMBER;
FUNCTION getProcessingPassId(root VARCHAR2, fullpath VARCHAR2) RETURN NUMBER;
PROCEDURE getAvailableEventTypes(a_Cursor out udt_RefCursor);
PROCEDURE getJobInfo(lfn VARCHAR2, a_Cursor out udt_RefCursor);
PROCEDURE insertTag(V_name VARCHAR2, V_tag VARCHAR2);
FUNCTION getDataQualityId(name VARCHAR2) RETURN NUMBER;
FUNCTION getQFlagByRunAndProcId(rnumber NUMBER, procid NUMBER) RETURN VARCHAR2;
PROCEDURE getRunByQflagAndProcId(procid NUMBER, flag NUMBER, a_Cursor out udt_RefCursor);
PROCEDURE getLFNsByProduction(prod NUMBER, a_Cursor out udt_RefCursor);
FUNCTION getFileID(v_FileName VARCHAR2) RETURN NUMBER;
PROCEDURE getJobIdFromInputFiles(v_FileId NUMBER, a_Cursor out udt_RefCursor);
PROCEDURE getFNameFiDRepWithJID(v_jobid NUMBER, a_Cursor out udt_RefCursor);
PROCEDURE getFileAndJobMetadata( v_jobid NUMBER, prod BOOLEAN, a_Cursor out udt_RefCursor);
PROCEDURE checkfile(name VARCHAR2, a_Cursor out udt_RefCursor);
FUNCTION checkFileTypeAndVersion (v_NAME  VARCHAR2,  v_VERSION VARCHAR2) RETURN NUMBER;
PROCEDURE checkEventType (v_EVENTTYPEID NUMBER, a_Cursor out udt_RefCursor);
FUNCTION insertJobsRow (
     v_ConfigName                  VARCHAR2,
     v_ConfigVersion               VARCHAR2,
     v_DiracJobId                  NUMBER,
     v_DiracVersion                VARCHAR2,
     v_EventInputStat              NUMBER,
     v_ExecTime                    FLOAT,
     v_FirstEventNumber            NUMBER,
     v_JobEnd                      TIMESTAMP,
     v_JobStart                    TIMESTAMP,
     v_Location                    VARCHAR2,
     v_Name                        VARCHAR2,
     v_NumberOfEvents              NUMBER,
     v_Production                  NUMBER,
     v_ProgramName                 VARCHAR2,
     v_ProgramVersion              VARCHAR2,
     v_StatisticsRequested         NUMBER,
     v_WNCPUPower                  VARCHAR2,
     v_CPUTime                   FLOAT,
     v_WNCache                     VARCHAR2,
     v_WNMemory                    VARCHAR2,
     v_WNModel                     VARCHAR2,
     v_WorkerNode                  VARCHAR2,
     v_runNumber                   NUMBER,
     v_fillNumber                  NUMBER,
     v_WNCPUHS06                   FLOAT,
     v_totalLuminosity             NUMBER,
     v_tck                         VARCHAR2,
     v_stepid                      NUMBER,
     v_WNMJFHS06                   FLOAT,
     v_hlt2tck                     VARCHAR2,
     v_numproc                     NUMBER
) RETURN NUMBER;

FUNCTION insertFilesRow (
    v_Adler32                         VARCHAR2,
    v_CreationDate                    TIMESTAMP,
    v_EventStat                       NUMBER,
    v_EventTypeId                     NUMBER,
    v_FileName                        VARCHAR2,
    v_FileTypeId                      NUMBER,
    v_GotReplica                      VARCHAR2,
    v_Guid                            VARCHAR2,
    v_JobId                           NUMBER,
    v_MD5Sum                          VARCHAR2,
    v_FileSize                        NUMBER,
    v_FullStat                        NUMBER,
    v_utc                             TIMESTAMP,
    dqflag                            VARCHAR2,
    v_luminosity                      NUMBER,
    v_instluminosity                  NUMBER,
    v_visibilityFlag                  VARCHAR2
) RETURN NUMBER;


PROCEDURE insertInputFilesRow (v_FileId NUMBER, v_JobId NUMBER);

PROCEDURE updateReplicaRow(v_fileID NUMBER,v_replica VARCHAR2);
PROCEDURE deleteInputFiles(v_jobid NUMBER);
PROCEDURE deletefile(v_fileid NUMBER);
PROCEDURE deleteSetpContiner( v_prod NUMBER);

FUNCTION insertSimConditions (
    v_Simdesc                VARCHAR2,
    v_BeamCond               VARCHAR2,
    v_BeamEnergy             VARCHAR2,
    v_Generator              VARCHAR2,
    v_MagneticField          VARCHAR2,
    v_DetectorCond           VARCHAR2,
    v_Luminosity             VARCHAR2,
    v_G4settings             VARCHAR2,
    v_visible                VARCHAR2
) RETURN NUMBER;

PROCEDURE getSimConditions(a_Cursor out udt_RefCursor);

FUNCTION insertDataTakingCond (
     v_DESCRIPTION                                        VARCHAR2,
     v_BEAMCOND                                           VARCHAR2,
     v_BEAMENERGY                                         VARCHAR2,
     v_MAGNETICFIELD                                      VARCHAR2,
     v_VELO                                               VARCHAR2,
     v_IT                                                 VARCHAR2,
     v_TT                                                 VARCHAR2,
     v_OT                                                 VARCHAR2,
     v_RICH1                                              VARCHAR2,
     v_RICH2                                              VARCHAR2,
     v_SPD_PRS                                            VARCHAR2,
     v_ECAL                                               VARCHAR2,
     v_HCAL                                               VARCHAR2,
     v_MUON                                               VARCHAR2,
     v_L0                                                 VARCHAR2,
     v_HLT                                                VARCHAR2,
     v_VeloPosition                                       VARCHAR2
) RETURN NUMBER;

PROCEDURE getFileMetaData(v_fileName VARCHAR2, a_Cursor out udt_RefCursor);
FUNCTION getFileMetaData2(iftypes lists) RETURN metadata_table PIPELINED;
PROCEDURE getFileMetaData3(iftypes varchararray, a_Cursor out udt_RefCursor);
FUNCTION fileExists(v_fileName VARCHAR2)RETURN NUMBER;
PROCEDURE inserteventTypes (v_Description VARCHAR2, v_EventTypeId NUMBER, v_Primary VARCHAR2);
PROCEDURE updateEventTypes(v_Description VARCHAR2, v_EventTypeId NUMBER, v_Primary VARCHAR2);
PROCEDURE setFileInvisible(lfn VARCHAR2);
PROCEDURE setFileVisible(lfn VARCHAR2);
PROCEDURE getConfigsAndEvtType(prodId NUMBER, a_Cursor out udt_RefCursor);
PROCEDURE getJobsbySites(prodId NUMBER,a_Cursor out udt_RefCursor);
PROCEDURE getSteps(prodId NUMBER, a_Cursor out udt_RefCursor);
PROCEDURE getProductionInformation(prodId NUMBER, a_Cursor out udt_RefCursor);
PROCEDURE getNbOfFiles(prodId NUMBER, a_Cursor out udt_RefCursor);
PROCEDURE getSizeOfFiles(prodId NUMBER, a_Cursor out udt_RefCursor);
PROCEDURE getNumberOfEvents(prodId NUMBER, a_Cursor out udt_RefCursor);
PROCEDURE getJobsNb(prodId NUMBER, a_Cursor out udt_RefCursor);
PROCEDURE insertStepsContainer(v_prod NUMBER, v_stepid NUMBER, v_step NUMBER);
PROCEDURE insertproductionscontainer_tmp(v_prod NUMBER, v_processingid NUMBER, v_simid NUMBER, v_daqperiodid NUMBER, cName VARCHAR2, cVersion VARCHAR2);
PROCEDURE insertproductionscontainer(v_prod NUMBER, v_processingid NUMBER, v_simid NUMBER, v_daqperiodid NUMBER, cName VARCHAR2, cVersion VARCHAR2);
PROCEDURE getEventTypes(cName VARCHAR2, cVersion VARCHAR2, a_Cursor out udt_RefCursor);
FUNCTION  getRunNumber(lfn VARCHAR2) RETURN NUMBER;
PROCEDURE insertRunquality(run NUMBER, qid NUMBER,procid NUMBER);
PROCEDURE getRunNbAndTck(lfn VARCHAR2, a_Cursor out udt_RefCursor);
PROCEDURE deleteProductionsCont(v_prod NUMBER);
PROCEDURE getRuns(c_name VARCHAR2, c_version VARCHAR2,  a_Cursor out udt_RefCursor);
FUNCTION getRunProcPass(v_runNumber NUMBER) RETURN run_proc_table;
PROCEDURE getRunQuality(runs numberarray , a_Cursor out udt_RefCursor);
PROCEDURE getTypeVesrsion(lfn VARCHAR2, a_Cursor out udt_RefCursor);
PROCEDURE getRunFiles(v_runNumber NUMBER, a_Cursor out udt_RefCursor);
FUNCTION getProcessedEvents(v_prodid NUMBER) RETURN NUMBER;
FUNCTION isVisible(v_stepid NUMBER) RETURN NUMBER;
FUNCTION isVisibleProd(v_prod NUMBER ) RETURN NUMBER;
/*FUNCTION getConfToBeUpdated RETURN conf_id_name_vers_table PIPELINED;*/
PROCEDURE insertRuntimeProject(pr_stepid NUMBER, run_pr_stepid NUMBER);
PROCEDURE updateRuntimeProject(pr_stepid NUMBER, run_pr_stepid NUMBER);
PROCEDURE removeRuntimeProject(pr_stepid NUMBER);
PROCEDURE getDirectoryMetadata(f_name VARCHAR2, a_Cursor out udt_RefCursor);
FUNCTION getFilesForGUID(v_guid VARCHAR2) RETURN VARCHAR2;
PROCEDURE updateDataQualityFlag(v_qualityid NUMBER, lfns varchararray);
PROCEDURE bulkcheckfiles(lfns varchararray,  a_Cursor out udt_RefCursor);
PROCEDURE bulkupdateReplicaRow(v_replica VARCHAR2, lfns varchararray);
PROCEDURE bulkgetTypeVesrsion(lfns varchararray, a_Cursor out udt_RefCursor);
PROCEDURE setObsolete;
PROCEDURE getDirectoryMetadata_new(lfns varchararray, a_Cursor out udt_RefCursor);
PROCEDURE bulkJobInfo(lfns varchararray, a_Cursor out udt_RefCursor);
PROCEDURE bulkJobInfoForJobName(jobNames varchararray, a_Cursor out udt_RefCursor);
PROCEDURE bulkJobInfoForJobId(jobids numberarray, a_Cursor out udt_RefCursor);
PROCEDURE insertRunStatus(v_runnumber NUMBER, v_JobId NUMBER, v_Finished VARCHAR2);
PROCEDURE setRunFinished(v_runnumber NUMBER, isFinished VARCHAR2);
PROCEDURE bulkupdateFileMetaData(files bigvarchararray);
PROCEDURE updateLuminosity(v_runnumber NUMBER);
PROCEDURE updateDesLuminosity(v_fileid NUMBER);
PROCEDURE getFileDesJobId(v_Filename VARCHAR2, a_Cursor out udt_RefCursor);
PROCEDURE getAllMetadata(v_jobid NUMBER, v_prod NUMBER, a_Cursor  out udt_RefCursor);
FUNCTION getProducedEvents(v_prodid NUMBER) RETURN NUMBER;
PROCEDURE bulkgetIdsFromFiles(lfns varchararray,  a_Cursor out udt_RefCursor);
PROCEDURE insertProdnOutputFtypes(v_production NUMBER, v_stepid NUMBER, v_filetypeid NUMBER, v_visible char, v_eventtype NUMBER);
END;
/


CREATE OR REPLACE package body BOOKKEEPINGORACLEDB AS
FUNCTION ext RETURN udt_RefCursor IS
cur udt_RefCursor;
BEGIN
  open cur FOR
    SELECT * FROM tab;
END;

-------------------------------------------------------------------------------------------------------------------------------
PROCEDURE getAvailableFileTypes(a_Cursor out udt_RefCursor )is
BEGIN
open a_Cursor FOR
  SELECT DISTINCT filetypes.name,filetypes.description FROM filetypes order by filetypes.name;
END;

---------------------------------------------------------------------------------------------------------------------------------
FUNCTION insertFileTypes(
  v_name VARCHAR2,
  description VARCHAR2,
  filetype VARCHAR2
) RETURN NUMBER is
id NUMBER;
found NUMBER;
ecode    Varchar2(256);
thisproc CONSTANT VARCHAR2(50) := 'trap_errmesg';
found_name EXCEPTION;
descr VARCHAR2(256);
BEGIN
  found := 0;
  id := -1;
  SELECT count(filetypeid) INTO found
  FROM filetypes
  WHERE filetypes.name=UPPER(v_name)
    AND filetypes.version=filetype;
  IF found>0 THEN
    RAISE found_name;
  ELSE
    SELECT DISTINCT description INTO descr
    FROM filetypes
    WHERE name=UPPER(v_name);
    SELECT COALESCE(max(filetypeid)+1, 1) INTO id
    FROM filetypes;
    INSERT INTO filetypes(filetypeid,
			  name,
			  description,
			  version)
    VALUES(id,
	  UPPER(v_name),
	  descr,
	  filetype);
    COMMIT;
    RETURN id;
  END IF;
  EXCEPTION
    WHEN found_name THEN
    raise_application_error(-20001,'The '||v_name || ' file type is already exist!!!');
    WHEN NO_DATA_FOUND THEN
      SELECT COALESCE(max(filetypeid)+1, 1) INTO id FROM filetypes;
      INSERT INTO filetypes(filetypeid,
			   name,
			   description,
			   version)
      VALUES(id,
	    UPPER(v_name),
	    description,
	    filetype);
      COMMIT;
    RETURN id;
    WHEN OTHERS THEN
      ecode := SQLERRM; --SQLCODE;
      dbms_output.put_line(thisproc || ' - ' || ecode);
      RETURN -1;
END;

-------------------------------------------------------------------------------------------------------------------------------
PROCEDURE getAvailableConfigurations (
    a_Cursor                    out udt_RefCursor
) IS
BEGIN
  open a_Cursor FOR
    SELECT ConfigName,ConfigVersion FROM configurations;
END;

---------------------------------------------------------------------------------------------------------------------------
PROCEDURE getStepsForSpecificIfiles(
  iftypes ifileslist,
  a_Cursor out udt_RefCursor
) IS
result BOOLEAN;
BEGIN
  IF iftypes.COUNT = 0 THEN
    INSERT INTO stepsTMP
      SELECT s.stepid,
	     s.stepname,
	     s.ApplicationName,
	     s.ApplicationVersion,
	     s.OptionFiles,
	     s.DDDb,
	     s.condDb,
	     s.extrapackages,
	     s.visible,
	     s.processingpass,
	     s.usable,
	     s.dqtag,
	     s.optionsformat,
	     s.isMulticore,
	     s.systemconfig,
	     s.mcTCK,
	     r.stepid,
	     r.stepname,
	     r.ApplicationName,
	     r.ApplicationVersion,
	     r.OptionFiles,
	     r.DDDb,
	     r.condDb,
	     r.extrapackages,
	     r.visible,
	     r.processingpass,
	     r.usable,
	     r.dqtag,
	     r.optionsformat,
	     r.isMulticore,
	     r.systemconfig,
	     r.mcTCK
      FROM steps s,
	   steps r,
	   runtimeprojects rr
      WHERE s.stepid=rr.stepid(+)
	AND r.stepid(+)=rr.runtimeprojectid
	AND s.inputfiletypes IS NULL;
  ELSE
    --iftypes:=inputfileslist('Charm.DST','SDST');
    FOR c IN (SELECT s.stepid,
		    s.inputfiletypes
	      FROM steps s,
		   table(s.inputfiletypes) i
	      WHERE i.name=iftypes(1))
      LOOP
      FOR i in c.inputfiletypes.FIRST .. c.inputfiletypes.LAST LOOP
      --   DBMS_OUTPUT.PUT_LINE('      Tag: '||c.inputfiletypes.FIRST(i));
      --  DBMS_OUTPUT.PUT_LINE('      Tag: '||c.inputfiletypes(i).NAME);
	result:=iftypes(i)=c.inputfiletypes(i).NAME;
	EXIT WHEN not result;
      END LOOP;
      IF result and iftypes.COUNT=c.inputfiletypes.LAST THEN
	INSERT INTO stepsTMP SELECT  s.stepid, s.stepname, s.ApplicationName, s.ApplicationVersion, s.OptionFiles, s.DDDb, s.condDb, s.extrapackages, s.visible, s.processingpass, s.usable, s.dqtag, s.optionsformat, s.isMulticore, s.systemconfig, s.mcTCK,
				     r.stepid, r.stepname, r.ApplicationName, r.ApplicationVersion, r.OptionFiles, r.DDDb, r.condDb, r.extrapackages, r.visible, r.processingpass, r.usable, r.dqtag, r.optionsformat, r.isMulticore, r.systemconfig, r.mcTCK
	FROM steps s,  steps r, runtimeprojects rr WHERE s.stepid=rr.stepid(+) and r.stepid(+)=rr.runtimeprojectid and s.stepid=c.stepid;
	DBMS_OUTPUT.PUT_LINE('      COOL: '||c.stepid);
      END IF;
    END LOOP;
    -- LOOP
	--inputf(i):=ftype(iftypes(i),'Y');
    --    DBMS_OUTPUT.PUT_LINE('      Tag: '||iftypes(i));
    -- END LOOP;
    --FOR inputfiletypes in c1
    --LOOP
       --result := inputf = iftypes;
    --   IF result THEN
    --      DBMS_OUTPUT.PUT_LINE('emp1 equal to emp2');
    --   END IF;
    --END LOOP;
  END IF;
  open  a_Cursor FOR
    SELECT * FROM stepsTMP;
END;
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
FUNCTION getStepsForFiletypes(iftypes lists, oftypes lists, match VARCHAR2) RETURN step_table PIPELINED IS
input BOOLEAN;
output BOOLEAN;
BEGIN
IF iftypes.COUNT = 0 and oftypes.COUNT = 0 THEN
  FOR cur in (SELECT s.stepid, s.stepname, s.ApplicationName, s.ApplicationVersion, s.OptionFiles, s.DDDb, s.condDb, s.extrapackages, s.visible, s.processingpass, s.usable, s.dqtag,s.optionsformat, s.isMulticore, s.systemconfig,s.mcTCK,
		     r.stepid AS rid, r.stepname AS rsname, r.ApplicationName AS rappname, r.ApplicationVersion AS rappver, r.OptionFiles AS roptsf, r.DDDb AS rdddb, r.condDb AS rcondb, r.extrapackages AS rextra,
		     r.visible AS rvisi, r.processingpass AS rproc, r.usable AS rusab, r.dqtag AS rdq,r.optionsformat AS ropff, r.isMulticore AS rmulticore, r.systemconfig AS rsystemconfig, r.mcTCK AS rmctck
  FROM steps s, steps r, runtimeprojects rr  WHERE s.stepid=rr.stepid(+) and r.stepid(+)=rr.runtimeprojectid and s.inputfiletypes is null and s.usable !='Obsolete') LOOP
  pipe row(stepobj(cur.stepid,cur.stepname,cur.ApplicationName, cur.ApplicationVersion, cur.OptionFiles, cur.DDDb, cur.condDb, cur.extrapackages, cur.visible, cur.processingpass, cur.usable, cur.dqtag, cur.optionsformat, cur.isMulticore, cur.systemconfig, cur.mcTCK,
            cur.rid, cur.rsname, cur.rappname, cur.rappver, cur.roptsf, cur.rdddb, cur.rcondb, cur.rextra, cur.rvisi, cur.rproc, cur.rusab,cur.rdq,cur.ropff, cur.rmulticore, cur.rsystemconfig, cur.rmctck));
  END LOOP;
ELSE
IF iftypes.COUNT>0 THEN
  FOR c IN (SELECT s.stepid, s.inputfiletypes, s.outputfiletypes FROM steps s  WHERE s.inputfiletypes is not null and s.usable!= 'Obsolete')
    LOOP
     --DBMS_OUTPUT.PUT_LINE('WHY!!? '||c.stepid);
     IF c.inputfiletypes is NOT NULL THEN
       IF match='YES' THEN
          IF c.inputfiletypes.COUNT != iftypes.COUNT THEN
             input:=FALSE;
          ELSE
          FOR i IN c.inputfiletypes.FIRST .. c.inputfiletypes.LAST LOOP
            IF i > iftypes.COUNT THEN
              input:= FALSE;
            ELSE
             input:=iftypes(i)=c.inputfiletypes(i).NAME;
            END IF;
            EXIT WHEN not input;
          END LOOP;
          END IF;
       ELSE
         input:=FALSE;
         FOR i in iftypes.FIRST .. iftypes.LAST LOOP
           FOR j in c.inputfiletypes.FIRST .. c.inputfiletypes.LAST LOOP
             IF iftypes(i)=c.inputfiletypes(j).NAME THEN
	       input:=TRUE;
               EXIT;
             END IF;
           END LOOP;
         EXIT WHEN input;
         END LOOP;
       END IF;
     END IF;
     IF input THEN
       IF oftypes.COUNT > 0 THEN
         output:=FALSE;
         IF c.outputfiletypes is NOT NULL THEN
           IF match='YES' THEN
             IF c.outputfiletypes.COUNT != oftypes.COUNT THEN
                 output:=FALSE;
             ELSE
             FOR i in c.outputfiletypes.FIRST .. c.outputfiletypes.LAST LOOP
	       IF i > iftypes.COUNT THEN
                  output:=FALSE;
               ELSE
                 output:=oftypes(i)=c.outputfiletypes(i).NAME;
               END IF;
               EXIT WHEN not output;
             END LOOP;
             END IF;
           ELSE
             output:=FALSE;
             FOR i in oftypes.FIRST .. oftypes.LAST LOOP
               FOR j in c.outputfiletypes.FIRST .. c.outputfiletypes.LAST LOOP
                 IF oftypes(i)=c.outputfiletypes(j).NAME THEN
	     output:=TRUE;
                   EXIT;
                 END IF;
               END LOOP;
               EXIT WHEN output;
             END LOOP;
           END IF;
         END IF;
       ELSE
        OUTPUT := TRUE;
       END IF;
    IF input and output THEN
      DBMS_OUTPUT.PUT_LINE('Insert1: '||c.stepid);
      FOR cur in (SELECT s.stepid, s.stepname, s.ApplicationName, s.ApplicationVersion, s.OptionFiles, s.DDDb, s.condDb, s.extrapackages, s.visible, s.processingpass, s.usable, s.dqtag,s.optionsformat, s.isMulticore, s.systemconfig,s.mcTCK,
		  r.stepid AS rid, r.stepname AS rsname, r.ApplicationName AS rappname, r.ApplicationVersion AS rappver, r.OptionFiles AS roptsf, r.DDDb AS rdddb, r.condDb AS rcondb, r.extrapackages AS rextra, r.visible AS rvisi,
		  r.processingpass AS rproc, r.usable AS rusab, r.dqtag AS rdq,r.optionsformat AS ropff, r.isMulticore AS rmulticore, r.systemconfig AS rsysconfig, r.mctck AS rmctck
	FROM steps s, steps r, runtimeprojects rr  WHERE s.stepid=rr.stepid(+) and r.stepid(+)=rr.runtimeprojectid and s.stepid=c.stepid and s.usable!='Obsolete' ) LOOP
      pipe row(stepobj(cur.stepid,cur.stepname,cur.ApplicationName, cur.ApplicationVersion, cur.OptionFiles, cur.DDDb, cur.condDb, cur.extrapackages, cur.visible, cur.processingpass, cur.usable, cur.dqtag, cur.optionsformat, cur.isMulticore, cur.systemconfig, cur.mctck,
               cur.rid, cur.rsname, cur.rappname, cur.rappver, cur.roptsf, cur.rdddb, cur.rcondb, cur.rextra, cur.rvisi, cur.rproc, cur.rusab,cur.rdq,cur.ropff, cur.rmulticore, cur.rsysconfig, cur.mctck));
      END LOOP;
    END IF;
  END IF;
 END LOOP;
ELSE
  FOR c IN (SELECT s.stepid, s.inputfiletypes, s.outputfiletypes FROM steps s WHERE s.outputfiletypes IS NOT NULL AND s.usable!= 'Obsolete')
    LOOP
     output:=FALSE;
     IF c.outputfiletypes IS NOT NULL THEN
       IF match='YES' THEN
	 IF c.outputfiletypes.COUNT!=oftypes.COUNT THEN
             output:=FALSE;
         ELSE
         FOR i IN c.outputfiletypes.FIRST .. c.outputfiletypes.LAST LOOP
           IF i > oftypes.COUNT THEN
             output:=FALSE;
           ELSE
             output:=oftypes(i)=c.outputfiletypes(i).NAME;
           END IF;
           EXIT WHEN not output;
         END LOOP;
         END IF;
       ELSE
        output:=FALSE;
        FOR i in oftypes.FIRST .. oftypes.LAST LOOP
          FOR j in c.outputfiletypes.FIRST .. c.outputfiletypes.LAST LOOP
            IF oftypes(i)=c.outputfiletypes(j).NAME THEN
	output:=TRUE;
              EXIT;
            END IF;
          END LOOP;
          EXIT WHEN output;
        END LOOP;
       END IF;
     END IF;
     IF output THEN
       IF iftypes.COUNT > 0 THEN
         input:=FALSE;
         IF match='YES' THEN
           IF c.inputfiletypes.COUNT!=iftypes.COUNT THEN
              input:=FALSE;
           ELSE
           FOR j in c.inputfiletypes.FIRST .. c.inputfiletypes.LAST LOOP
             IF j > iftypes.COUNT THEN
               input:=FALSE;
             ELSE
               input:=iftypes(j)=c.inputfiletypes(j).NAME;
             END IF;
             EXIT WHEN not output;
           END LOOP;
           END IF;
         ELSE
           input:=FALSE;
           FOR i in iftypes.FIRST .. iftypes.LAST LOOP
             FOR j in c.inputfiletypes.FIRST .. c.inputfiletypes.LAST LOOP
               IF iftypes(i)=c.inputfiletypes(j).NAME THEN
		 input:=TRUE;
                 EXIT;
               END IF;
             END LOOP;
             EXIT WHEN input;
           END LOOP;
         END IF;
       ELSE
        input := TRUE;
       END IF;
    IF input and output THEN
      DBMS_OUTPUT.PUT_LINE('Insert2: '||c.stepid);
      FOR cur2 in (SELECT s.stepid, s.stepname, s.ApplicationName, s.ApplicationVersion, s.OptionFiles, s.DDDb, s.condDb, s.extrapackages, s.visible, s.processingpass, s.usable, s.dqtag,s.optionsformat, s.isMulticore, s.systemconfig,s.mctck,
			  r.stepid AS rid, r.stepname AS rsname, r.ApplicationName AS rappname, r.ApplicationVersion AS rappver, r.OptionFiles AS roptsf, r.DDDb AS rdddb, r.condDb AS rcondb, r.extrapackages AS rextra, r.visible AS rvisi,
			  r.processingpass AS rproc, r.usable AS rusab, r.dqtag AS rdq,r.optionsformat AS ropff, r.isMulticore AS rmulticore, r.systemconfig AS rsysconfig, r.mctck AS rmctck
	FROM steps s, steps r, runtimeprojects rr  WHERE s.stepid=rr.stepid(+) and r.stepid(+)=rr.runtimeprojectid and s.stepid=c.stepid and s.usable!='Obsolete') LOOP
        pipe row(stepobj(cur2.stepid,cur2.stepname,cur2.ApplicationName, cur2.ApplicationVersion, cur2.OptionFiles, cur2.DDDb, cur2.condDb, cur2.extrapackages, cur2.visible, cur2.processingpass, cur2.usable, cur2.dqtag, cur2.optionsformat, cur2.isMulticore, cur2.systemconfig,cur2.mctck,
                          cur2.rid, cur2.rsname, cur2.rappname, cur2.rappver, cur2.roptsf, cur2.rdddb, cur2.rcondb, cur2.rextra, cur2.rvisi, cur2.rproc, cur2.rusab,cur2.rdq,cur2.ropff, cur2.rmulticore, cur2.rsysconfig, cur2.rmctck));
      END LOOP;
    END IF;
  END IF;
 END LOOP;
END IF;
END IF;
END;

---------------------------------------------------------------------------------------------------------------------------
PROCEDURE getStepsForSpecificOfiles(oftypes ifileslist, a_Cursor out udt_RefCursor)is
result BOOLEAN;
BEGIN
IF oftypes.COUNT = 0 THEN
INSERT INTO stepsTMP SELECT s.stepid, s.stepname, s.ApplicationName, s.ApplicationVersion,s.OptionFiles,s.DDDb, s.condDb,s.extrapackages,s.visible, s.processingpass, s.usable, s.dqtag, s.optionsformat, s.isMulticore, s.systemconfig,s.mctck,
     r.stepid, r.stepname, r.applicationname,r.applicationversion,r.optionfiles,r.DDDB,r.CONDDB, r.extrapackages,r.Visible, r.ProcessingPass, r.Usable, r.dqtag, r.optionsformat, r.isMulticore, r.systemconfig, r.mctck
    FROM steps s, steps r, runtimeprojects rr WHERE s.stepid=rr.stepid(+) AND r.stepid(+)=rr.runtimeprojectid AND s.outputfiletypes IS NULL;
ELSE
 FOR c IN (SELECT s.stepid, s.outputfiletypes FROM steps s, table(s.outputfiletypes) i WHERE i.name=oftypes(1)) LOOP
  FOR i IN c.outputfiletypes.FIRST .. c.outputfiletypes.LAST LOOP
    result:=oftypes(i)=c.outputfiletypes(i).NAME;
    EXIT WHEN not result;
  END LOOP;
  IF result AND oftypes.COUNT=c.outputfiletypes.LAST THEN
    INSERT INTO stepsTMP SELECT s.stepid, s.stepname, s.ApplicationName, s.ApplicationVersion,s.OptionFiles,s.DDDb, s.condDb,s.extrapackages,s.visible, s.processingpass, s.usable, s.dqtag, s.optionsformat, s.isMulticore, s.systemconfig, s.mctck,
     r.stepid, r.stepname, r.applicationname,r.applicationversion,r.optionfiles,r.DDDB,r.CONDDB, r.extrapackages,r.Visible, r.ProcessingPass, r.Usable, r.dqtag, r.optionsformat, r.isMulticore, r.systemconfig , r.mctck
    FROM steps s, steps r, runtimeprojects rr WHERE s.stepid=rr.stepid(+) AND r.stepid(+)=rr.runtimeprojectid AND s.stepid=c.stepid;
  END IF;
END LOOP;
END IF;
open  a_Cursor FOR
  SELECT * FROM stepsTMP;
END;

---------------------------------------------------------------------------------------------------------------------------
PROCEDURE getStepsForIfiles(iftypes ifileslist , a_Cursor out udt_RefCursor)is
result BOOLEAN;
BEGIN
IF iftypes.COUNT = 0 THEN
  INSERT INTO stepsTMP SELECT s.stepid, s.stepname, s.ApplicationName, s.ApplicationVersion,s.OptionFiles,s.DDDb, s.condDb,s.extrapackages,s.visible, s.processingpass, s.usable, s.dqtag, s.optionsformat, s.isMulticore, s.systemconfig, s.mctck,
     r.stepid, r.stepname, r.applicationname,r.applicationversion,r.optionfiles,r.DDDB,r.CONDDB, r.extrapackages,r.Visible, r.ProcessingPass, r.Usable, r.dqtag, r.optionsformat, r.isMulticore, r.systemconfig, r.mctck
     FROM steps s,steps r, runtimeprojects rr
     WHERE s.stepid=rr.stepid(+) AND r.stepid(+)=rr.runtimeprojectid AND s.inputfiletypes IS NULL;
ELSE
 FOR c IN (SELECT s.stepid, s.inputfiletypes FROM steps s, table(s.inputfiletypes)) LOOP
  FOR j in iftypes.FIRST .. iftypes.LAST LOOP
    result := False;
    FOR i in c.inputfiletypes.FIRST .. c.inputfiletypes.LAST LOOP
      result:=iftypes(j)=c.inputfiletypes(i).NAME;
      exit when result;
    END LOOP;
  END LOOP;
  IF result THEN
    INSERT INTO stepsTMP SELECT s.stepid, s.stepname, s.ApplicationName,s.ApplicationVersion,s.OptionFiles,s.DDDb,s.condDb,s.extrapackages,s.visible, s.processingpass, s.usable, s.dqtag, s.optionsformat, s.isMulticore, s.systemconfig, s.mctck,
    r.stepid, r.stepname, r.applicationname,r.applicationversion,r.optionfiles,r.DDDB,r.CONDDB, r.extrapackages,r.Visible, r.ProcessingPass, r.Usable, r.dqtag, r.optionsformat, r.isMulticore, r.systemconfig, r.mctck
       FROM steps s, steps r, runtimeprojects rr
     WHERE s.stepid=c.stepid AND s.stepid=rr.stepid(+) AND r.stepid(+)=rr.runtimeprojectid;
  END IF;
END LOOP;
END IF;
open  a_Cursor FOR
  SELECT DISTINCT * FROM stepsTMP;
END;
--------------------------------------------------------------------------------------
PROCEDURE getStepsForOfiles(oftypes ifileslist, a_Cursor out udt_RefCursor) is
result BOOLEAN;
BEGIN
IF oftypes.COUNT = 0 THEN
  INSERT INTO stepsTMP SELECT s.stepid, s.stepname, s.ApplicationName,s.ApplicationVersion,s.OptionFiles,s.DDDb,s.condDb,s.extrapackages,s.visible, s.processingpass, s.usable, s.dqtag, s.optionsformat, s.isMulticore, s.systemconfig, s.mctck,
    r.stepid, r.stepname, r.applicationname,r.applicationversion,r.optionfiles,r.DDDB,r.CONDDB, r.extrapackages,r.Visible, r.ProcessingPass, r.Usable, r.dqtag, r.optionsformat, r.isMulticore, r.systemconfig, r.mctck
     FROM steps s, steps r, runtimeprojects rr  WHERE
 s.stepid=rr.stepid(+) and r.stepid(+)=rr.runtimeprojectid and s.outputfiletypes is null;
ELSE
  FOR c IN (SELECT s.stepid, s.outputfiletypes FROM steps s, table(s.outputfiletypes)) LOOP
    FOR j IN oftypes.FIRST .. oftypes.LAST LOOP
      result := False;
      FOR i in c.outputfiletypes.FIRST .. c.outputfiletypes.LAST LOOP
        result:=oftypes(j)=c.outputfiletypes(i).NAME;
        exit when result;
      END LOOP;
    END LOOP;
    IF result THEN
      INSERT INTO stepsTMP SELECT s.stepid, s.stepname, s.ApplicationName,s.ApplicationVersion,s.OptionFiles,s.DDDb,s.condDb,s.extrapackages,s.visible, s.processingpass, s.usable, s.dqtag, s.optionsformat, s.isMulticore, s.systemconfig, s.mctck,
    r.stepid, r.stepname, r.applicationname,r.applicationversion,r.optionfiles,r.DDDB,r.CONDDB, r.extrapackages,r.Visible, r.ProcessingPass, r.Usable, r.dqtag, r.optionsformat, r.isMulticore, r.systemconfig, r.mctck
    FROM steps s, steps r, runtimeprojects rr WHERE s.stepid=rr.stepid(+) and r.stepid(+)=rr.runtimeprojectid and s.stepid=c.stepid;
    END IF;
  END LOOP;
END IF;
OPEN a_Cursor FOR
 SELECT DISTINCT * FROM stepsTMP;
END;

---------------------------------------------------------------------------------------------------------------------------
PROCEDURE getAvailebleSteps(iftypes ifileslist, a_Cursor out udt_RefCursor)is
result BOOLEAN;
BEGIN
IF iftypes.COUNT = 0 THEN
INSERT INTO stepsTMP SELECT s.stepid, s.stepname, s.ApplicationName,s.ApplicationVersion,s.OptionFiles,s.DDDb,s.condDb,s.extrapackages,s.visible, s.processingpass, s.usable,s.dqtag, s.optionsformat, s.isMulticore, s.systemconfig, s.mctck,
    r.stepid, r.stepname, r.applicationname,r.applicationversion,r.optionfiles,r.DDDB,r.CONDDB, r.extrapackages,r.Visible, r.ProcessingPass, r.Usable, r.dqtag, r.optionsformat, r.isMulticore, r.systemconfig, r.mctck
      FROM steps s, steps r, runtimeprojects rr WHERE s.stepid=rr.stepid(+) and r.stepid(+)=rr.runtimeprojectid and s.inputfiletypes is null;
ELSE
--iftypes:=inputfileslist('Charm.DST','SDST');
 FOR c IN (SELECT s.stepid, s.inputfiletypes FROM steps s, table(s.inputfiletypes) i WHERE i.name=iftypes(1)) LOOP
  FOR i in c.inputfiletypes.FIRST .. c.inputfiletypes.LAST LOOP
  --   DBMS_OUTPUT.PUT_LINE('      Tag: '||c.inputfiletypes.FIRST(i));
  --  DBMS_OUTPUT.PUT_LINE('      Tag: '||c.inputfiletypes(i).NAME);
    result:=iftypes(i)=c.inputfiletypes(i).NAME;
    EXIT WHEN not result;
  END LOOP;
  IF result and iftypes.COUNT=c.inputfiletypes.LAST THEN
    INSERT INTO stepsTMP SELECT s.stepid, s.stepname, s.ApplicationName,s.ApplicationVersion,s.OptionFiles,s.DDDb,s.condDb,s.extrapackages,s.visible, s.processingpass, s.usable, s.dqtag, s.optionsformat, s.isMulticore, s.systemconfig, s.mctck,
    r.stepid, r.stepname, r.applicationname,r.applicationversion,r.optionfiles,r.DDDB,r.CONDDB, r.extrapackages,r.Visible, r.ProcessingPass, r.Usable, r.dqtag, r.optionsformat, r.isMulticore, r.systemconfig, r.mctck
    FROM steps s, steps r, runtimeprojects rr WHERE s.stepid=rr.stepid(+) and r.stepid(+)=rr.runtimeprojectid and s.stepid=c.stepid;
    DBMS_OUTPUT.PUT_LINE('      COOL: '||c.stepid);
  END IF;
END LOOP;
-- LOOP
    --inputf(i):=ftype(iftypes(i),'Y');
--    DBMS_OUTPUT.PUT_LINE('      Tag: '||iftypes(i));
-- END LOOP;
--FOR inputfiletypes in c1
--LOOP
   --result := inputf = iftypes;
--   IF result THEN
--      DBMS_OUTPUT.PUT_LINE('emp1 equal to emp2');
--   END IF;
--END LOOP;
END IF;
open  a_Cursor FOR
  SELECT * FROM stepsTMP;
END;

--------------------------------------------------------------------------------------
PROCEDURE  getAvailebleStepsRealAndMC(iftypes ifileslist , a_Cursor out udt_RefCursor)is
result BOOLEAN;
BEGIN
IF iftypes.COUNT = 0 THEN
  INSERT INTO stepsTMP SELECT s.stepid, s.stepname, s.ApplicationName,s.ApplicationVersion,s.OptionFiles,s.DDDb,s.condDb,s.extrapackages,s.visible, s.processingpass, s.usable, s.dqtag, s.optionsformat, s.isMulticore, s.systemconfig, s.mctck,
  r.stepid, r.stepname, r.applicationname,r.applicationversion,r.optionfiles,r.DDDB,r.CONDDB, r.extrapackages,r.Visible, r.ProcessingPass, r.Usable, r.dqtag, r.optionsformat, r.isMulticore, r.systemconfig, r.mctck
  FROM steps s, steps r, runtimeprojects rr WHERE s.stepid=rr.stepid(+) and r.stepid(+)=rr.runtimeprojectid and s.inputfiletypes is null;
ELSE
  FOR c IN (SELECT s.stepid, s.inputfiletypes FROM steps s, table(s.inputfiletypes))
  LOOP
    FOR j in iftypes.FIRST .. iftypes.LAST
    LOOP
      result := False;
      FOR i in c.inputfiletypes.FIRST .. c.inputfiletypes.LAST
      LOOP
	result:=iftypes(j)=c.inputfiletypes(i).NAME;
	exit when result;
      END LOOP;
    END LOOP;
  IF result THEN
    INSERT INTO stepsTMP SELECT s.stepid, s.stepname, s.ApplicationName,s.ApplicationVersion,s.OptionFiles,s.DDDb,s.condDb,s.extrapackages,s.visible, s.processingpass, s.usable, s.dqtag, s.optionsformat, s.isMulticore, s.systemconfig, s.mctck,
    r.stepid, r.stepname, r.applicationname,r.applicationversion,r.optionfiles,r.DDDB,r.CONDDB, r.extrapackages,r.Visible, r.ProcessingPass, r.Usable, r.dqtag, r.optionsformat, r.isMulticore, r.systemconfig, r.mctck
    FROM steps s, steps r, runtimeprojects rr WHERE s.stepid=rr.stepid(+) and r.stepid(+)=rr.runtimeprojectid and s.stepid=c.stepid;
  END IF;
  END LOOP;
END IF;
open  a_Cursor FOR
    SELECT DISTINCT * FROM stepsTMP;
END;

--------------------------------------------------------------------------------------
FUNCTION getProductionProcessingPass(prod NUMBER) RETURN VARCHAR2 IS
retval VARCHAR2(256);
ecode    NUMBER(38);
thisproc CONSTANT VARCHAR2(50) := 'trap_errmesg';
BEGIN
SELECT v.path INTO retval FROM (SELECT DISTINCT  LEVEL-1 Pathlen, SYS_CONNECT_BY_PATH(name, '/') Path
FROM processing
WHERE LEVEL > 0 AND id = (SELECT DISTINCT processingid FROM productionscontainer prod WHERE prod.production=prod)
CONNECT BY NOCYCLE PRIOR id=parentid ORDER BY Pathlen desc) v WHERE rownum<=1;
RETURN retval;
EXCEPTION
  WHEN OTHERS THEN
    raise_application_error(-20004, 'error found! The processing pass does not exists!');
--ecode := SQLERRM; --SQLCODE;
--dbms_output.put_line(thisproc || ' - ' || ecode);
  RETURN NULL;
END;

------------------------------------------------------------------------------------------------------------------------------------------------------
FUNCTION getProductionProcessingPassId(prod NUMBER) RETURN NUMBER is
result Number;
ecode    NUMBER(38);
thisproc CONSTANT VARCHAR2(50) := 'trap_errmesg';
BEGIN
  SELECT DISTINCT processingid INTO result FROM productionscontainer prod WHERE prod.production=prod;
  RETURN result;
  EXCEPTION WHEN OTHERS THEN
  ecode := SQLERRM;
END;

----------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE getAvailableEventTypes(a_Cursor out udt_RefCursor) IS
BEGIN
  open a_Cursor FOR
    SELECT DISTINCT eventtypeid, description FROM eventtypes;
END;

------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE getJobInfo(
   lfn                             VARCHAR2,
   a_Cursor                        out udt_RefCursor
) IS
BEGIN
  open a_Cursor FOR
    SELECT jobs.DIRACJOBID,
	   jobs.DIRACVERSION,
	   jobs.EVENTINPUTSTAT,
	   jobs.EXECTIME,
	   jobs.FIRSTEVENTNUMBER,
	   jobs.LOCATION,
	   jobs.NAME,
	   jobs.NUMBEROFEVENTS,
	   jobs.STATISTICSREQUESTED,
	   jobs.WNCPUPOWER,
	   jobs.CPUTIME,
	   jobs.WNCACHE,
	   jobs.WNMEMORY,
	   jobs.WNMODEL,
	   jobs.WORKERNODE,
	   jobs.WNCPUHS06,
	   jobs.jobid,
	   jobs.totalluminosity,
	   jobs.production,
	   jobs.programName,
	   jobs.programVersion,
	   jobs.WNMJFHS06,
	   jobs.HLT2TCK,
	   jobs.NumberOfProcessors
    FROM jobs,files
    WHERE files.jobid=jobs.jobid
      AND files.filename=lfn;
END;

----------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE insertTag(
    V_name                            VARCHAR2,
    V_tag                             VARCHAR2
) IS
tid NUMBER;
BEGIN
  SELECT tags_index_seq.nextval INTO tid FROM dual;
  INSERT INTO tags(tagid,name, tag) VALUES(tid, V_name, V_tag);
  COMMIT;
END;

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
FUNCTION getProcessingPassId(root VARCHAR2, fullpath VARCHAR2) RETURN NUMBER IS
result NUMBER;
ecode NUMBER(38);
BEGIN
  result:=-1;
  SELECT DISTINCT v.id INTO result FROM (SELECT DISTINCT SYS_CONNECT_BY_PATH(name, '/') Path, id ID
  FROM processing v START WITH id IN (
    SELECT DISTINCT id
    FROM processing
    WHERE name=root
    )
  CONNECT BY NOCYCLE PRIOR  id=parentid) v
  WHERE v.path=fullpath;
  RETURN  result;
  EXCEPTION
    WHEN OTHERS THEN
      ecode := SQLERRM;
END;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
FUNCTION getDataQualityId(name VARCHAR2) RETURN NUMBER IS
result NUMBER;
ecode NUMBER(38);
BEGIN
  result:=1;
  SELECT DISTINCT qualityid INTO result FROM dataquality WHERE dataqualityflag=name;
  RETURN result;
  EXCEPTION WHEN OTHERS THEN
  ecode := SQLERRM;
END;

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
FUNCTION getQFlagByRunAndProcId(
  rnumber NUMBER,
  procid NUMBER)
RETURN VARCHAR2 IS result VARCHAR2(256);
ecode NUMBER(38);
BEGIN
  result:= -1;
  SELECT d.dataqualityflag INTO result  FROM dataquality d, newrunquality r WHERE r.runnumber=rnumber and r.processingid=procid and d.qualityid=r.qualityid;
  RETURN result;
  EXCEPTION
  WHEN NO_DATA_FOUND THEN
  raise_application_error(-20014, 'The data quality does not exists in the newrunquality table!');
  WHEN OTHERS THEN
  ecode := SQLERRM;
END;

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE getRunByQflagAndProcId(
  procid NUMBER,
  flag NUMBER,
  a_Cursor out udt_RefCursor ) IS
BEGIN
  IF flag is not null THEN
    open a_Cursor FOR
      SELECT runnumber
      FROM newrunquality
      WHERE processingid=procid AND qualityid=flag;
  ELSE
    open a_Cursor FOR SELECT runnumber   FROM newrunquality WHERE processingid=procid;
  END IF;
END;

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE getLFNsByProduction(
   prod                    NUMBER,
   a_Cursor                out udt_RefCursor
 ) IS
BEGIN
  open a_Cursor FOR
    SELECT filename
    FROM files, jobs
    WHERE jobs.jobid=files.jobid
      AND jobs.jobid=files.jobid
      AND jobs.production=prod;
/*   SELECT filename
     FROM files JOIN jobs ON jobs.jobid=files.jobid
       AND jobs.production=1622;
 */
END;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
FUNCTION getFileID(
  v_FileName VARCHAR2
)
RETURN NUMBER IS fid NUMBER;
BEGIN
  fid := 0;
  SELECT files.fileid INTO fid
  FROM files
  WHERE files.filename=v_FileName;
  RETURN fid;
  EXCEPTION WHEN OTHERS THEN
  RETURN NULL;
END;
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE getJobIdFromInputFiles(
   v_FileId                        NUMBER,
   a_Cursor                        out udt_RefCursor
 ) IS
BEGIN
  open a_Cursor FOR
    SELECT inputfiles.jobid
    FROM inputfiles
    WHERE inputfiles.fileid=v_FileId;
END;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE getFNameFiDRepWithJID(
  v_jobid NUMBER,
  a_Cursor out udt_RefCursor
) IS
BEGIN
  open a_Cursor FOR
    SELECT files.fileName,
	   files.fileid,
	   files.gotreplica
    FROM files
    WHERE files.jobid=v_jobid;
END;

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE getFileAndJobMetadata(
  v_jobid NUMBER,
  prod BOOLEAN,
  a_Cursor out udt_RefCursor
) IS
BEGIN
  IF NOT prod THEN
    open a_Cursor FOR
      SELECT files.fileName,
	     files.fileid,
	     files.gotreplica,
	     0,
	     files.eventstat,
	     files.eventtypeid,
	     files.luminosity,
	     files.instLuminosity,
	     filetypes.name
      FROM files, filetypes
      WHERE files.filetypeid=filetypes.filetypeid
	AND files.jobid=v_jobid;
  ELSE
    open a_Cursor FOR
      SELECT files.fileName,
	     files.fileid,
	     files.gotreplica,
	     jobs.production,
	     files.eventstat,
	     files.eventtypeid,
	     files.luminosity,
	     files.instLuminosity,
	     filetypes.name
      FROM files, jobs, filetypes
      WHERE files.filetypeid=filetypes.filetypeid
	AND jobs.jobid=files.jobid
	AND files.jobid=v_jobid;
  END IF;
END;

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE checkfile(
  name                            VARCHAR2,
  a_Cursor                        out udt_RefCursor
) IS
BEGIN
  open a_Cursor FOR
    SELECT fileId,
	   jobId,
	   filetypeid
    FROM files
    WHERE filename=name;
END;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
FUNCTION checkFileTypeAndVersion (
       v_NAME                          VARCHAR2,
       v_VERSION                       VARCHAR2
 ) RETURN NUMBER is
 id NUMBER :=0;
 descr VARCHAR2(256);
 BEGIN
   SELECT filetypeId INTO id FROM filetypes WHERE
           NAME=v_NAME and
           version=v_VERSION;
   RETURN id;
   EXCEPTION
    when TOO_MANY_ROWS THEN
     SELECT min(filetypeid) INTO id FROM filetypes WHERE NAME=v_NAME and version=v_VERSION; RETURN id;
    WHEN OTHERS THEN
   SELECT count(*) INTO id FROM filetypes WHERE
           NAME=v_NAME;
   IF id > 0 THEN
   SELECT DISTINCT DESCRIPTION INTO descr FROM filetypes WHERE
           NAME=v_NAME;
   SELECT COALESCE(max(filetypeid)+1, 1) INTO id FROM filetypes;
   INSERT INTO filetypes(filetypeid,name,description,version) VALUES(id,v_NAME,descr,v_VERSION);
   COMMIT;
   RETURN id;
   ELSE
     raise_application_error(-20013, 'File type does not exist!');
   END IF;
 END;
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE checkEventType (
    v_EVENTTYPEID                  NUMBER,
    a_Cursor                        out udt_RefCursor
 )is
 BEGIN
   open a_Cursor FOR
    SELECT DESCRIPTION,PRIMARY FROM eventtypes WHERE
      EVENTTYPEID=v_EVENTTYPEID;
 END;
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
FUNCTION insertJobsRow (
     v_ConfigName                  VARCHAR2,
     v_ConfigVersion               VARCHAR2,
     v_DiracJobId                  NUMBER,
     v_DiracVersion                VARCHAR2,
     v_EventInputStat              NUMBER,
     v_ExecTime                    FLOAT,
     v_FirstEventNumber            NUMBER,
     v_JobEnd                      TIMESTAMP,
     v_JobStart                    TIMESTAMP,
     v_Location                    VARCHAR2,
     v_Name                        VARCHAR2,
     v_NumberOfEvents              NUMBER,
     v_Production                  NUMBER,
     v_ProgramName                 VARCHAR2,
     v_ProgramVersion              VARCHAR2,
     v_StatisticsRequested         NUMBER,
     v_WNCPUPower                  VARCHAR2,
     v_CPUTime                     FLOAT,
     v_WNCache                     VARCHAR2,
     v_WNMemory                    VARCHAR2,
     v_WNModel                     VARCHAR2,
     v_WorkerNode                  VARCHAR2,
     v_runNumber                   NUMBER,
     v_fillNumber                  NUMBER,
     v_WNCPUHS06                   FLOAT,
     v_totalLuminosity             NUMBER,
     v_tck                         VARCHAR2,
     v_stepid                      NUMBER,
     v_WNMJFHS06                   FLOAT,
     v_hlt2tck                     VARCHAR2,
     v_numproc                     NUMBER
  )RETURN NUMBER is
  jid       NUMBER;
  configId  NUMBER;
  existInDB  NUMBER;
  ecode    Varchar2(256);
  BEGIN
    configId := 0;
    SELECT count(*) INTO existInDB FROM configurations WHERE ConfigName=v_ConfigName and ConfigVersion=v_ConfigVersion;
    IF existInDB=0 THEN
      SELECT configurationId_seq.nextval INTO configId FROM dual;
      INSERT INTO configurations(ConfigurationId,ConfigName,ConfigVersion)VALUES(configId, v_ConfigName, v_ConfigVersion);
      COMMIT;
    ELSE
     SELECT configurationid INTO configId FROM configurations WHERE ConfigName=v_ConfigName and ConfigVersion=v_ConfigVersion;
    END IF;

    SELECT jobId_seq.nextval INTO jid FROM dual;
     INSERT INTO jobs(
         JobId,
         ConfigurationId,
         DiracJobId,
         DiracVersion,
         EventInputStat,
         ExecTime,
         FirstEventNumber,
         JobEnd,
         JobStart,
         Location,
         Name,
         NumberOfEvents,
         Production,
         ProgramName,
         ProgramVersion,
         StatisticsRequested,
         WNCPUPower,
         CPUTime,
         WNCache,
         WNMemory,
         WNModel,
         WorkerNode,
         RunNumber,
         FillNumber,
         WNCPUHS06,
         TotalLuminosity,
         Tck,
         StepID,
         WNMJFHS06,
         HLT2Tck,
         NumberOfProcessors
         )
   VALUES(
          jid,
          configId,
          v_DiracJobId,
          v_DiracVersion,
          v_EventInputStat,
          v_ExecTime,
          v_FirstEventNumber,
          v_JobEnd,
          v_JobStart,
          v_Location,
          v_Name,
          v_NumberOfEvents,
          v_Production,
          v_ProgramName,
          v_ProgramVersion,
          v_StatisticsRequested,
          v_WNCPUPower,
          v_CPUTime,
          v_WNCache,
          v_WNMemory,
          v_WNModel,
          v_WorkerNode,
          v_runNumber,
          v_fillNumber,
          v_WNCPUHS06,
          v_totalLuminosity,
          v_tck,
          v_stepid,
          v_WNMJFHS06,
          v_hlt2tck,
          v_numproc);

  COMMIT;
  RETURN jid;
  EXCEPTION
  WHEN DUP_VAL_ON_INDEX THEN
    jid:=0;
    IF v_Production < 0 THEN
      SELECT j.jobid INTO jid FROM jobs j WHERE j.runnumber=v_runNumber and j.production<0;
    ELSE 
       SELECT j.jobid INTO jid FROM jobs j WHERE j.name=v_Name and j.production=v_Production;
    END IF;

    IF jid=0 THEN
      ecode:= SQLERRM;
      raise_application_error(ecode, 'It is not a run!');
    ELSE
       update jobs set ConfigurationId=configId,
         DiracJobId=v_DiracJobId,
         DiracVersion=v_DiracVersion,
         EventInputStat=v_EventInputStat,
         ExecTime=v_ExecTime,
         FirstEventNumber=v_FirstEventNumber,
         JobEnd=v_JobEnd,
         JobStart=v_JobStart,
         Location=v_Location,
         Name=v_Name,
         NumberOfEvents=v_NumberOfEvents,
         Production=v_Production,
         ProgramName=v_ProgramName,
         ProgramVersion=v_ProgramVersion,
         StatisticsRequested=v_StatisticsRequested,
         WNCPUPower=v_WNCPUPower,
         CPUTime=v_CPUTime,
         WNCache=v_WNCache,
         WNMemory=v_WNMemory,
         WNModel=v_WNModel,
         WorkerNode=v_WorkerNode,
         FillNumber=v_fillNumber,
         WNCPUHS06=v_WNCPUHS06,
         TotalLuminosity=v_totalLuminosity,
         StepID = v_stepid,
         Tck=v_tck,
         WNMJFHS06=v_WNMJFHS06,
         HLT2Tck=v_hlt2tck,
	 NumberOfProcessors=v_numproc WHERE jobid=jid;
      COMMIT;
    RETURN jid;
    END IF;
    RETURN -1;
  END;
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  FUNCTION insertFilesRow (
    v_Adler32                         VARCHAR2,
    v_CreationDate                    TIMESTAMP,
    v_EventStat                       NUMBER,
    v_EventTypeId                     NUMBER,
    v_FileName                        VARCHAR2,
    v_FileTypeId                      NUMBER,
    v_GotReplica                      VARCHAR2,
    v_Guid                            VARCHAR2,
    v_JobId                           NUMBER,
    v_MD5Sum                          VARCHAR2,
    v_FileSize                        NUMBER,
    v_FullStat                      NUMBER,
    v_utc                             TIMESTAMP,
    dqflag                            VARCHAR2,
    v_luminosity                      NUMBER,
    v_instluminosity                   Number,
    v_visibilityFlag                  VARCHAR2
  )RETURN NUMBER is
  fid NUMBER;
  dqid NUMBER;
  Begin
    dqid:=1;
    SELECT dataquality.qualityid INTO dqid FROM dataquality WHERE dataquality.dataqualityflag=dqflag;
    SELECT fileId_seq.nextval INTO fid FROM dual;
    INSERT INTO files (
                FileId,
                Adler32,
                CreationDate,
                EventStat,
                EventTypeId,
                FileName,
                FileTypeId,
                GotReplica,
                Guid,
                JobId,
                MD5Sum,
                FileSize,
                FullStat,
                Qualityid,
                inserttimestamp,
                Luminosity,
                InstLuminosity,
                VisibilityFlag
                )
           VALUES (
                fid,
                v_Adler32,
                v_CreationDate,
                v_EventStat,
                v_EventTypeId,
                v_FileName,
                v_FileTypeId,
                v_GotReplica,
                v_Guid,
                v_JobId,
                v_MD5Sum,
                v_FileSize,
                v_FullStat,
                dqid,
                v_utc,
                v_luminosity,
                v_instluminosity,
                v_visibilityFlag
                );
  COMMIT;
  RETURN fid;
  EXCEPTION
  WHEN DUP_VAL_ON_INDEX THEN
    SELECT fileid INTO fid FROM files WHERE FileName=v_FileName;
    update files set Adler32=v_Adler32,
                CreationDate=v_CreationDate,
                EventStat=v_EventStat,
                EventTypeId=v_EventTypeId,
                FileTypeId=v_FileTypeId,
                Guid=v_Guid,
                JobId=v_JobId,
                MD5Sum=v_MD5Sum,
                FileSize=v_FileSize,
                FullStat=v_FullStat,
                Qualityid=dqid,
                inserttimestamp=v_utc,
                Luminosity=v_luminosity,
                InstLuminosity=v_instluminosity,
		VisibilityFlag=v_visibilityFlag WHERE fileid=fid;
    COMMIT;
    RETURN fid;
  END;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE insertInputFilesRow (v_FileId NUMBER, v_JobId NUMBER)is
BEGIN
    INSERT INTO inputfiles(
         FileId,
         JobId
         ) VALUES(
                v_FileId,
                v_JobId);
  COMMIT;
  EXCEPTION
  WHEN DUP_VAL_ON_INDEX THEN
    DBMS_OUTPUT.PUT_LINE('The input file of the job is added: '|| v_JobId);
  END;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE updateReplicaRow(
   v_fileID NUMBER,
   v_replica VARCHAR2
  )is
  BEGIN
   update files set inserttimestamp = sys_extract_utc(systimestamp),gotreplica=v_replica WHERE fileid=v_fileID;
   COMMIT;
  END;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 PROCEDURE deleteInputFiles(
  v_jobid    NUMBER
 )is
  BEGIN
   DELETE inputfiles WHERE jobid=v_jobid;
   COMMIT;
  END;
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 PROCEDURE deletefile(
   v_fileid                NUMBER
 )is
  BEGIN
   DELETE files WHERE fileId=v_fileid;
   COMMIT;
 END;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE deleteSetpContiner(
  v_prod NUMBER
  )is
   BEGIN
   DELETE stepscontainer WHERE production=v_prod;
   COMMIT;
END;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE deleteProductionsCont(
 v_prod NUMBER
  )is
   BEGIN
   DELETE productionscontainer WHERE production=v_prod;
   COMMIT;
END;

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
FUNCTION insertSimConditions(
   v_Simdesc                VARCHAR2,
   v_BeamCond               VARCHAR2,
   v_BeamEnergy             VARCHAR2,
   v_Generator              VARCHAR2,
   v_MagneticField          VARCHAR2,
   v_DetectorCond           VARCHAR2,
   v_Luminosity             VARCHAR2,
   v_G4settings             VARCHAR2,
   v_visible                VARCHAR2
 )RETURN NUMBER
 is
  simulId NUMBER;
 BEGIN
  SELECT simulationCondID_seq.nextval INTO simulId FROM dual;
  INSERT INTO simulationconditions(
               SimId,
               SIMDESCRIPTION,
               BeamCond,
               BeamEnergy,
               Generator,
               MagneticField,
               DetectorCond,
               Luminosity,
               G4settings,
	       visible)VALUES(simulId,v_Simdesc,v_BeamCond,v_BeamEnergy,v_Generator,v_MagneticField,v_DetectorCond,v_Luminosity,v_G4settings, v_visible);
  COMMIT;
  RETURN simulId;
 END;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE getSimConditions (
    a_Cursor                        out udt_RefCursor
    )is
   BEGIN
     open a_Cursor FOR
       SELECT * FROM simulationconditions WHERE visible='Y' ORDER by simid desc;
   END;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
FUNCTION insertDataTakingCond(
     v_DESCRIPTION                                        VARCHAR2,
     v_BEAMCOND                                           VARCHAR2,
     v_BEAMENERGY                                         VARCHAR2,
     v_MAGNETICFIELD                                      VARCHAR2,
     v_VELO                                               VARCHAR2,
     v_IT                                                 VARCHAR2,
     v_TT                                                 VARCHAR2,
     v_OT                                                 VARCHAR2,
     v_RICH1                                              VARCHAR2,
     v_RICH2                                              VARCHAR2,
     v_SPD_PRS                                            VARCHAR2,
     v_ECAL                                               VARCHAR2,
     v_HCAL                                               VARCHAR2,
     v_MUON                                               VARCHAR2,
     v_L0                                                 VARCHAR2,
     v_HLT                                                VARCHAR2,
     v_VeloPosition                                       VARCHAR2
  ) RETURN NUMBER
  is
  daq       NUMBER;
  BEGIN

      daq := 0;
      SELECT simulationCondID_seq.nextval INTO daq FROM dual;
      IF v_DESCRIPTION is null THEN
      insert /* APPEND */ INTO data_taking_conditions(DAQPERIODID, DESCRIPTION, BEAMCOND, BEAMENERGY, MAGNETICFIELD,
                                       VELO, IT, TT, OT, RICH1, RICH2, SPD_PRS, ECAL, HCAL, MUON, L0, HLT,VELOPOSITION)
				      VALUES(
                                         daq,
                                         'DataTaking'||daq,
                                         v_BEAMCOND,
                                         v_BEAMENERGY,
                                         v_MAGNETICFIELD,
                                         v_VELO,
                                         v_IT,
                                         v_TT,
                                         v_OT,
                                         v_RICH1,
                                         v_RICH2,
                                         v_SPD_PRS,
                                         v_ECAL,
                                         v_HCAL,
                                         v_MUON,
                                         v_L0,
                                         v_HLT,
                                         v_VeloPosition);
    COMMIT;
    ELSE
       insert /* APPEND */ INTO data_taking_conditions(DAQPERIODID, DESCRIPTION, BEAMCOND, BEAMENERGY, MAGNETICFIELD,
                                       VELO, IT, TT, OT, RICH1, RICH2, SPD_PRS, ECAL, HCAL, MUON, L0, HLT,VELOPOSITION)
				      VALUES(
                                         daq,
                                         v_DESCRIPTION,
                                         v_BEAMCOND,
                                         v_BEAMENERGY,
                                         v_MAGNETICFIELD,
                                         v_VELO,
                                         v_IT,
                                         v_TT,
                                         v_OT,
                                         v_RICH1,
                                         v_RICH2,
                                         v_SPD_PRS,
                                         v_ECAL,
                                         v_HCAL,
                                         v_MUON,
                                         v_L0,
                                         v_HLT,
                                         v_VeloPosition);
    COMMIT;
    END IF;

    RETURN (daq);
    EXCEPTION WHEN OTHERS THEN
    RETURN 0;
  END;
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

PROCEDURE getFileMetaData(
   v_fileName              VARCHAR2,
   a_Cursor                out udt_RefCursor
  )is
  BEGIN
   open a_Cursor FOR
     SELECT files.FILENAME,files.ADLER32,files.CREATIONDATE,files.EVENTSTAT,files.EVENTTYPEID,filetypes.Name,files.GOTREPLICA,files.GUID,files.MD5SUM,files.FILESIZE, files.FullStat, dataquality.DATAQUALITYFLAG, files.jobid, jobs.runnumber, files.inserttimestamp,files.luminosity,files.instluminosity FROM files,filetypes,dataquality,jobs WHERE
         filename=v_fileName and
         jobs.jobid=files.jobid and
         files.filetypeid=filetypes.filetypeid and
         files.QUALITYID=DataQuality.qualityID;
  END;

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
FUNCTION getFileMetaData2(iftypes lists) RETURN metadata_table PIPELINED
is
BEGIN
FOR j in iftypes.FIRST .. iftypes.LAST LOOP
  DBMS_OUTPUT.PUT_LINE('FileName: '|| iftypes(j));
  FOR cur in (SELECT files.FILENAME,files.ADLER32,files.CREATIONDATE,files.EVENTSTAT,files.EVENTTYPEID,filetypes.Name,files.GOTREPLICA,files.GUID,files.MD5SUM,files.FILESIZE, files.FullStat, dataquality.DATAQUALITYFLAG, files.jobid, jobs.runnumber, files.inserttimestamp,files.luminosity,files.instluminosity, files.VISIBILITYFLAG FROM files,filetypes,dataquality,jobs WHERE
         filename=iftypes(j) and
         jobs.jobid=files.jobid and
         files.filetypeid=filetypes.filetypeid and
         files.QUALITYID=DataQuality.qualityID) LOOP
        pipe row(metadata0bj(cur.FILENAME, cur.ADLER32,cur.CREATIONDATE,cur.EVENTSTAT, cur.EVENTTYPEID, cur.Name, cur.GOTREPLICA, cur.GUID, cur.MD5SUM, cur.FILESIZE, cur.FullStat, cur.DATAQUALITYFLAG, cur.jobid, cur.runnumber, cur.inserttimestamp, cur.luminosity, cur.instluminosity, cur.VISIBILITYFLAG, NULL, NULL));
  END LOOP;
END LOOP;
END;

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE getFileMetaData3(iftypes varchararray, a_Cursor out udt_RefCursor)
is
lfnmeta metadata_table := metadata_table();
n integer := 0;
BEGIN
FOR j in iftypes.FIRST .. iftypes.LAST LOOP
  DBMS_OUTPUT.PUT_LINE('FileName: '|| iftypes(j));
  FOR cur in (SELECT files.FILENAME,files.ADLER32,files.CREATIONDATE,files.EVENTSTAT,files.EVENTTYPEID,filetypes.Name,files.GOTREPLICA,files.GUID,files.MD5SUM,files.FILESIZE, files.FullStat, dataquality.DATAQUALITYFLAG, files.jobid, jobs.runnumber, files.inserttimestamp,files.luminosity,files.instluminosity, files.VISIBILITYFLAG FROM files,filetypes,dataquality,jobs WHERE
         filename=iftypes(j) and
         jobs.jobid=files.jobid and
         files.filetypeid=filetypes.filetypeid and
         files.QUALITYID=DataQuality.qualityID) LOOP
 lfnmeta.extend;
 n:=n+1;
 lfnmeta (n):=metadata0bj(cur.FILENAME, cur.ADLER32,cur.CREATIONDATE,cur.EVENTSTAT, cur.EVENTTYPEID, cur.Name, cur.GOTREPLICA, cur.GUID, cur.MD5SUM, cur.FILESIZE, cur.FullStat, cur.DATAQUALITYFLAG, cur.jobid, cur.runnumber, cur.inserttimestamp, cur.luminosity, cur.instluminosity, cur.VISIBILITYFLAG, NULL, NULL);
  END LOOP;
END LOOP;
open a_Cursor FOR SELECT * FROM table(lfnmeta);
END;

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
FUNCTION fileExists(
    v_fileName            VARCHAR2
  )RETURN NUMBER is
  fid NUMBER;
  BEGIN
   SELECT fileid INTO fid FROM files WHERE filename=v_fileName;
  RETURN (fid);
    EXCEPTION WHEN OTHERS THEN
    RETURN 0;

END;
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE inserteventTypes (
        v_Description           VARCHAR2,
        v_EventTypeId           NUMBER,
        v_Primary               VARCHAR2
 )
 is
 BEGIN
   INSERT INTO eventtypes(Description,EventTypeId,Primary) VALUES (v_Description, v_EventTypeId, v_Primary);
   COMMIT;
 END;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE  updateEventTypes(
        v_Description           VARCHAR2,
        v_EventTypeId           NUMBER,
        v_Primary               VARCHAR2
 )
 is
 BEGIN
   update eventtypes set Description=v_Description, Primary=v_Primary WHERE EventTypeId=v_EventTypeId;
   COMMIT;
 END;
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE setFileInvisible(
  lfn VARCHAR2
 )is
 BEGIN
  update files set visibilityFlag='N',inserttimestamp = sys_extract_utc(systimestamp) WHERE files.filename=lfn;
  COMMIT;
 END;

PROCEDURE setFileVisible(
  lfn VARCHAR2
 )is
 BEGIN
  update files set visibilityFlag='Y',inserttimestamp = sys_extract_utc(systimestamp) WHERE files.filename=lfn;
  COMMIT;
 END;

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE getConfigsAndEvtType(
   prodId                  NUMBER,
   a_Cursor                out udt_RefCursor
  )is
  BEGIN
    open a_Cursor FOR
    SELECT c.configName,c.ConfigVersion,prod.eventtypeid FROM productionoutputfiles prod, configurations c,
    productionscontainer cont WHERE prod.production=prodID AND cont.production=prod.production AND cont.configurationid=c.configurationid
    GROUP BY c.configName,c.ConfigVersion,prod.eventtypeid;
  END;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE getJobsbySites(
   prodId                  NUMBER,
   a_Cursor                out udt_RefCursor
 )is
  BEGIN
   open a_Cursor FOR
    SELECT count(*), jobs.Location FROM jobs WHERE production=prodId Group By Location;
  END;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE getSteps(
   prodId                  NUMBER,
   a_Cursor                out udt_RefCursor
  )is
  BEGIN
   open a_Cursor FOR
    SELECT s.stepName, s.applicationname, s.applicationversion, s.optionfiles, s.dddb, s.conddb, s.extrapackages, s.stepid, s.visible
      FROM steps s, stepscontainer prod WHERE
      prod.stepid=s.stepid and
      prod.production=prodId order by prod.step;
  EXCEPTION
  WHEN OTHERS THEN
    raise_application_error(-20003, 'error found the production does not exists  in the productionscontainer table!');
  END;
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE getProductionInformation(
   prodId                  NUMBER,
   a_Cursor                out udt_RefCursor
  )is
  pid NUMBER;
  BEGIN
   open a_Cursor FOR
     SELECT DISTINCT c.configName,c.ConfigVersion,f.eventtypeid,s.stepName, s.applicationname, s.applicationversion, s.optionfiles, s.dddb, s.conddb, s.extrapackages, prod.step
	  FROM steps s, stepscontainer prod, jobs j, configurations c, files f WHERE
           j.jobid=f.jobid and
           f.eventtypeid>0 and
           j.production=prodId and
           c.configurationid=j.configurationid and
           prod.stepid=s.stepid and
           prod.production=j.production order by prod.step;
  END;
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE getNbOfFiles( prodId                  NUMBER,
    a_Cursor                out udt_RefCursor
  )is
  total NUMBER;
  BEGIN
   SELECT /*+ INDEX(files FILES_JOB_EVENT_FILETYPE) */ count(*) INTO total FROM files, jobs WHERE files.jobid=jobs.jobid and jobs.production=prodId;
   open a_Cursor FOR
     SELECT /*+ INDEX(files FILES_JOB_EVENT_FILETYPE) */ count(*), filetypes.Name,total AS TotalFiles FROM files, jobs,filetypes WHERE
        files.jobid=jobs.jobid and
        jobs.production=prodId and
        filetypes.filetypeid=files.filetypeid GROUP By filetypes.NAME;
  END;
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE getSizeOfFiles(
    prodId                  NUMBER,
    a_Cursor                out udt_RefCursor
  )is
  BEGIN
  open a_Cursor FOR
    SELECT /*+ INDEX(files FILES_JOB_EVENT_FILETYPE) */  sum(FILESIZE) FROM files,jobs WHERE files.jobid=jobs.jobid and jobs.production=prodId;
  END;
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE getNumberOfEvents(
    prodId                  NUMBER,
    a_Cursor                out udt_RefCursor
  )is
  BEGIN
  open a_Cursor FOR
   SELECT /*+ INDEX(files FILES_JOB_EVENT_FILETYPE) */ filetypes.name,sum(files.EVENTSTAT), files.eventtypeid, sum(jobs.eventinputstat) FROM files,jobs,filetypes WHERE
            files.jobid=jobs.jobid and
            files.gotreplica='Yes' and
            jobs.production=prodId and
            filetypes.filetypeid=files.filetypeid GROUP by filetypes.name, files.eventtypeid;
  END;
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE getJobsNb(
    prodId            NUMBER,
    a_Cursor                out udt_RefCursor
  )is
  BEGIN
  open a_Cursor FOR
    SELECT count(*) FROM jobs WHERE production=prodId;
END;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE insertStepsContainer(v_prod NUMBER, v_stepid NUMBER, v_step NUMBER)is
alreadyExists NUMBER;
BEGIN
INSERT INTO stepscontainer(production,stepid,step)VALUES(v_prod, v_stepid, v_step);
COMMIT;
EXCEPTION
  WHEN DUP_VAL_ON_INDEX THEN
   dbms_output.put_line(v_prod || 'already in the steps container table');
   SELECT count(*) INTO alreadyExists FROM stepscontainer WHERE production=v_prod AND stepid=v_stepid AND step=v_step;
   IF alreadyExists > 0 THEN
     raise_application_error(-20005, 'The production already exists in the steps container table!');
   END IF;
END;
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE insertproductionscontainer_tmp(v_prod NUMBER, v_processingid NUMBER, v_simid NUMBER, v_daqperiodid NUMBER, cName VARCHAR2, cVersion VARCHAR2) is
configId NUMBER;
existInDB NUMBER;
BEGIN
configId := 0;
SELECT count(*) INTO existInDB FROM configurations WHERE ConfigName=cName and ConfigVersion=cVersion;
IF existInDB=0 THEN
  SELECT configurationId_seq.nextval INTO configId FROM dual;
  INSERT INTO configurations(ConfigurationId,ConfigName,ConfigVersion)VALUES(configId, cName, cVersion);
  COMMIT;
ELSE
 SELECT configurationid INTO configId FROM configurations WHERE ConfigName=cName and ConfigVersion=cVersion;
END IF;
INSERT INTO productionscontainer(production,processingid,simid,daqperiodid, configurationid)VALUES(v_prod, v_processingid, v_simid, v_daqperiodid, configId);
COMMIT;
EXCEPTION
  WHEN DUP_VAL_ON_INDEX THEN
   existInDB := 0;
   dbms_output.put_line(v_prod || 'already in the steps container table');
   SELECT count(*) INTO existInDB FROM productionscontainer WHERE production=v_prod and processingid=v_processingid AND  simid=v_simid AND daqperiodid=v_daqperiodid and configurationid=configId;
   IF existInDB > 0 THEN
    raise_application_error(-20005, 'The production already exists in the productionscontainer table!');
   END IF;
END;
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE insertproductionscontainer(v_prod NUMBER, v_processingid NUMBER, v_simid NUMBER, v_daqperiodid NUMBER, cName VARCHAR2, cVersion VARCHAR2) is
configId NUMBER;
existInDB NUMBER;
BEGIN
configId := 0;
SELECT count(*) INTO existInDB FROM configurations WHERE ConfigName=cName and ConfigVersion=cVersion;
IF existInDB=0 THEN
  SELECT configurationId_seq.nextval INTO configId FROM dual;
  INSERT INTO configurations(ConfigurationId,ConfigName,ConfigVersion)VALUES(configId, cName, cVersion);
  COMMIT;
ELSE
 SELECT configurationid INTO configId FROM configurations WHERE ConfigName=cName and ConfigVersion=cVersion;
END IF;
INSERT INTO productionscontainer(production,processingid,simid,daqperiodid, configurationid)VALUES(v_prod, v_processingid, v_simid, v_daqperiodid, configId);
COMMIT;
EXCEPTION
  WHEN DUP_VAL_ON_INDEX THEN
   existInDB := 0;
   dbms_output.put_line(v_prod || 'already in the steps container table');
   SELECT count(*) INTO existInDB FROM productionscontainer WHERE production=v_prod and processingid=v_processingid AND  simid=v_simid AND daqperiodid=v_daqperiodid and configurationid=configId;
   IF existInDB > 0 THEN
    raise_application_error(-20005, 'The production already exists in the productionscontainer table!');
   END IF;
END;
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 PROCEDURE getEventTypes(
    cName                 VARCHAR2,
    cVersion              VARCHAR2,
    a_Cursor              out udt_RefCursor
   ) is
BEGIN
  open a_Cursor FOR
    SELECT DISTINCT e.EVENTTYPEID, e.DESCRIPTION FROM productionoutputfiles prod, eventtypes e, productionscontainer cont,
    configurations c WHERE
    c.CONFIGNAME=cName and c.CONFIGVERSION=cVersion AND
    c.configurationid=cont.configurationid AND cont.production=prod.production AND
    prod.eventtypeid=e.eventtypeid
    ORDER By e.EVENTTYPEID DESC;
   END;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
FUNCTION  getRunNumber(lfn VARCHAR2) RETURN NUMBER is
id NUMBER;
BEGIN
SELECT jobs.runnumber INTO id FROM jobs,files WHERE files.jobid=jobs.jobid and files.filename=lfn;
RETURN id;
EXCEPTION
  WHEN OTHERS THEN
  RETURN null;
END;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE insertRunquality(run NUMBER, qid NUMBER, procid NUMBER) is
BEGIN
INSERT INTO newrunquality(runnumber,qualityid, processingid) VALUES(run,qid,procid);
COMMIT;
EXCEPTION
  WHEN DUP_VAL_ON_INDEX THEN
    UPDATE newrunquality set qualityid=qid WHERE processingid=procid and runnumber=run;
COMMIT;
END;
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE getRunNbAndTck(lfn VARCHAR2, a_Cursor out udt_RefCursor) is
BEGIN
open a_Cursor FOR
  SELECT jobs.runnumber, jobs.Tck FROM jobs,files WHERE files.jobid=jobs.jobid and files.filename=lfn;
END;
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE getRuns(c_name VARCHAR2, c_version VARCHAR2,  a_Cursor out udt_RefCursor) is
BEGIN
open a_Cursor FOR
  SELECT DISTINCT run.runnumber FROM productionoutputfiles prod, prodrunview run, configurations c, productionscontainer cont WHERE
  prod.production=run.production and c.configname=c_name and c.configversion=c_version AND
  cont.configurationid=c.configurationid AND cont.production=prod.production;
END;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
FUNCTION getRunProcPass(v_runNumber NUMBER) RETURN run_proc_table
is
ret_tab run_proc_table := run_proc_table();
n integer := 0;
ret VARCHAR2(256);
BEGIN
  FOR r in (SELECT DISTINCT production FROM jobs WHERE runnumber=v_runNumber and production>0)
    LOOP
      ret_tab.extend;
      n := n + 1;
      ret:=getProductionProcessingPass(r.production);
      ret_tab(n) := runnb_proc(v_runNumber,ret);
      END LOOP;
RETURN ret_tab;
END;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE getRunQuality(runs numberarray , a_Cursor out udt_RefCursor)
is
ret_tab bulk_collect_run_quality_evt:= bulk_collect_run_quality_evt();
n integer := 0;
BEGIN
FOR i in 1 .. runs.COUNT LOOP
 FOR record in (SELECT DISTINCT jobs.runnumber,dataquality.dataqualityflag,files. eventtypeid FROM files, jobs,dataquality WHERE files.jobid=jobs.jobid and files.qualityid=dataquality.qualityid  and jobs.production<0 and jobs.runnumber=runs(i)) LOOP
  ret_tab.extend;
  n := n + 1;
  ret_tab(n):= runnb_quality_eventtype(record.runnumber,record.dataqualityflag,record.eventtypeid);
  END LOOP;
  END LOOP;
open a_Cursor FOR SELECT * FROM table(ret_tab);
END;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE getTypeVesrsion(lfn VARCHAR2, a_Cursor out udt_RefCursor)
is
BEGIN
open a_Cursor FOR SELECT ftype.version FROM files f, filetypes ftype WHERE f.filetypeid=ftype.filetypeid and f.filename=lfn;
END;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE getRunFiles(v_runNumber NUMBER, a_Cursor out udt_RefCursor)
is
BEGIN
open a_Cursor FOR
SELECT f.filename, f.gotreplica, f.filesize,f.guid, f.luminosity, f.INSTLUMINOSITY, f.eventstat, f.fullstat
FROM jobs j ,files f, filetypes ft
WHERE j.jobid=f.jobid and ft.filetypeid=f.filetypeid and ft.name='RAW' and  j.production<0 and j.runnumber=v_runNumber;
END;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
FUNCTION getProcessedEvents(v_prodid NUMBER) RETURN NUMBER
is
retVal NUMBER := 0;
BEGIN
SELECT sum(j.numberofevents) INTO retVal FROM jobs j, (SELECT scont.production, s.stepid
FROM stepscontainer scont, steps s
WHERE scont.stepid = s.stepid and
scont.production=v_prodid and
scont.step=(SELECT max(step) FROM stepscontainer WHERE stepscontainer.production=v_prodid)) firsts WHERE j.production=firsts.production and j.stepid=firsts.stepid;
RETURN retVal;
EXCEPTION
  WHEN OTHERS THEN
    raise_application_error(-20005, 'error found during the event NUMBER calculation');
END;
FUNCTION isVisible(v_stepid NUMBER) RETURN NUMBER
is
vis char;
c NUMBER;
BEGIN
SELECT count(*) INTO c FROM TABLE(SELECT s.outputfiletypes FROM steps s WHERE s.stepid=v_stepid);
IF c = 0 THEN
RETURN v_stepid;
ELSE
SELECT DISTINCT visible INTO vis FROM TABLE(SELECT s.outputfiletypes FROM steps s WHERE s.stepid=v_stepid)
    WHERE ViSible='Y';
IF vis='Y' THEN
RETURN v_stepid;
ELSE
RETURN 0;
END IF;
END IF;
EXCEPTION
   WHEN NO_DATA_FOUND THEN
    RETURN -1;
END;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
FUNCTION isVisibleProd(v_prod NUMBER) RETURN NUMBER
is
sid NUMBER := 0;
res NUMBER := 0;
BEGIN
SELECT st.stepid INTO sid FROM stepscontainer st WHERE st.production=v_prod and st.step=(SELECT max(step) FROM stepscontainer st2 WHERE st2.production=v_prod);
res := isVisible(sid);
IF res > 0 THEN
RETURN v_prod;
ELSE
RETURN -1;
END IF;
/*EXCEPTION
   WHEN NO_DATA_FOUND THEN
    RETURN v_prod;*/
END;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
/*FUNCTION getConfToBeUpdated RETURN conf_id_name_vers_table PIPELINED is
v_configurations_table conf_id_name_vers_table := conf_id_name_vers_table();
BEGIN
FOR cur in ( SELECT * FROM configurations WHERE configname='LHCb')
LOOP
pipe row(conf_id_name_vers(cur.configurationid,cur.configname,cur.configversion));
END LOOP;
--RETURN v_configurations_table;
END;*/
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE insertRuntimeProject(pr_stepid NUMBER, run_pr_stepid NUMBER)
is
BEGIN
INSERT INTO runtimeprojects(stepid, runtimeprojectid) VALUES (pr_stepid,run_pr_stepid);
COMMIT;
END;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE updateRuntimeProject(pr_stepid NUMBER, run_pr_stepid NUMBER)
is
counter Number;
BEGIN
 SELECT count(*) INTO counter FROM runtimeprojects WHERE stepid=pr_stepid;
 IF counter > 0 THEN
  update runtimeprojects set runtimeprojectid=run_pr_stepid WHERE stepid=pr_stepid;
  ELSE
    insertRuntimeProject (pr_stepid, run_pr_stepid);
  END IF;
COMMIT;
END;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE removeRuntimeProject(pr_stepid NUMBER)
is
BEGIN
DELETE runtimeprojects WHERE stepid=pr_stepid;
COMMIT;
END;
PROCEDURE funny(a NUMBER)is
b NUMBER;
BEGIN
IF a > 0 THEN
  b:=a-1;
  dbms_output.put_line(b || ' - ' || 'coool!');
  funny(b);
END IF;
END;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
FUNCTION getProductionPorcPassName(v_procid NUMBER) RETURN VARCHAR2 is
retval VARCHAR2(256);
ecode    NUMBER(38);
thisproc CONSTANT VARCHAR2(50) := 'trap_errmesg';
BEGIN
 SELECT v.path INTO retval FROM (SELECT DISTINCT  LEVEL-1 Pathlen, SYS_CONNECT_BY_PATH(name, '/') Path
   FROM processing
   WHERE LEVEL > 0 and id=v_procid
   CONNECT BY NOCYCLE PRIOR id=parentid order by Pathlen desc) v WHERE rownum<=1;
RETURN retval;
EXCEPTION WHEN OTHERS THEN
raise_application_error(-20004, 'error found! The processing pass does not exists!');
--ecode := SQLERRM; --SQLCODE;
--dbms_output.put_line(thisproc || ' - ' || ecode);
RETURN null;
END;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE getDirectoryMetadata(f_name VARCHAR2, a_Cursor out udt_RefCursor)
is
/*create or replace  type
directoryMetadata is object
(production NUMBER,
configname VARCHAR2(256),
configversion  VARCHAR2(256),
eventtypeid NUMBER,
filetype VARCHAR2(256),
processingpass VARCHAR2(256),
ConditionDescription VARCHAR2(256),
VISIBILITYFLAG CHAR(1));
create or replace
type bulk_collect_directoryMetadata is table of directoryMetadata;
*/
lfnmeta bulk_collect_directoryMetadata := bulk_collect_directoryMetadata();
n integer := 0;
procName VARCHAR2(256);
simdesc VARCHAR2(256);
daqdesc VARCHAR2(256);
BEGIN
FOR c in (SELECT /*+ INDEX(f FILES_FILENAME_UNIQUE) */ DISTINCT j.production, c.configname, c.configversion, ft.name, f.eventtypeid, f.VISIBILITYFLAG FROM files f, jobs j, filetypes ft, configurations c WHERE
c.configurationid=j.configurationid and ft.filetypeid = f.filetypeid and j.jobid=f.jobid and f.gotreplica='Yes' and f.filename like f_name)
LOOP
  SELECT getProductionPorcPassName(prod.processingid),sim.simdescription, daq.description INTO procName, simdesc, daqdesc FROM productionscontainer prod, simulationconditions sim, data_taking_conditions daq WHERE
   production=c.production and
   prod.simid=sim.simid(+) and
   prod.daqperiodid=daq.daqperiodid(+);
   lfnmeta.extend;
   n:=n+1;
   IF simdesc is NULL or simdesc='' THEN
     lfnmeta (n):= directoryMetadata(c.production,c.configname, c.configversion, c.eventtypeid, c.name, procname,daqdesc,c.VISIBILITYFLAG);
   ELSE
     lfnmeta (n):= directoryMetadata(c.production,c.configname, c.configversion, c.eventtypeid, c.name, procname,simdesc,c.VISIBILITYFLAG);
   END IF;
END LOOP;
open a_Cursor FOR SELECT * FROM table(lfnmeta);
EXCEPTION
   WHEN NO_DATA_FOUND THEN
    raise_application_error(-20088, 'The file '||f_name||' does not exists in the bookkeeping database!');
END;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE getDirectoryMetadata_new(lfns varchararray, a_Cursor out udt_RefCursor)
is
/*create or replace  type
directoryMetadata_new is object
(lfn VARCHAR2(256),
production NUMBER,
configname VARCHAR2(256),
configversion  VARCHAR2(256),
eventtypeid NUMBER,
filetype VARCHAR2(256),
processingpass VARCHAR2(256),
ConditionDescription VARCHAR2(256),
VISIBILITYFLAG CHAR(1));
create or replace
type bulk_collect_directoryMet_new is table of directoryMetadata_new;
*/
lfnmeta bulk_collect_directoryMet_new := bulk_collect_directoryMet_new();
n integer := 0;
procName VARCHAR2(256);
simdesc VARCHAR2(256);
daqdesc VARCHAR2(256);
allfiletypes VARCHAR2(256);
found NUMBER := 0;
BEGIN
FOR i in lfns.FIRST .. lfns.LAST LOOP
  FOR c in (SELECT DISTINCT j.production, c.configname, c.configversion, ft.name, f.eventtypeid, f.VISIBILITYFLAG FROM files f, jobs j, filetypes ft, configurations c WHERE
   c.configurationid=j.configurationid and ft.filetypeid = f.filetypeid and j.jobid=f.jobid and f.gotreplica='Yes' and f.filename like lfns(i)) LOOP
   SELECT count(*) INTO found FROM productionscontainer WHERE production=c.production;
   IF found>0then
     SELECT getProductionPorcPassName(prod.processingid),sim.simdescription, daq.description INTO procName, simdesc, daqdesc FROM productionscontainer prod, simulationconditions sim, data_taking_conditions daq WHERE
       production=c.production and
       prod.simid=sim.simid(+) and
       prod.daqperiodid=daq.daqperiodid(+);
     lfnmeta.extend;
     n:=n+1;
    allfiletypes := '';
    --we have to make the list of file types....
    FOR ff in (SELECT DISTINCT ft.name FROM files f, jobs j, filetypes ft, configurations c WHERE
      c.configurationid=j.configurationid and ft.filetypeid = f.filetypeid and j.jobid=f.jobid and f.gotreplica='Yes' and f.filename like lfns(i)) LOOP
       allfiletypes := CONCAT(allfiletypes, CONCAT(ff.name,','));
    END LOOP;
    --remove the coma
    allfiletypes := substr(allfiletypes, 0, length(allfiletypes)-1);
    IF simdesc is NULL or simdesc='' THEN
      lfnmeta (n):= directoryMetadata_new(lfns(i),c.production,c.configname, c.configversion, c.eventtypeid, allfiletypes, procname,daqdesc, c.VISIBILITYFLAG);
    ELSE
      lfnmeta (n):= directoryMetadata_new(lfns(i),c.production,c.configname, c.configversion, c.eventtypeid, allfiletypes, procname,simdesc, c.VISIBILITYFLAG);
    END IF;
 END IF;
  END LOOP;
END LOOP;
--do not RETURN the duplicated rows.
open a_Cursor FOR SELECT DISTINCT lfn, production, configname, configversion,eventtypeid, filetype, processingpass,ConditionDescription, VISIBILITYFLAG FROM table(lfnmeta);
END;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
FUNCTION getFilesForGUID(v_guid VARCHAR2) RETURN VARCHAR2 is
result VARCHAR2(256);
BEGIN
SELECT filename INTO result FROM files WHERE guid=v_guid;
RETURN result;
EXCEPTION
   WHEN NO_DATA_FOUND THEN
    raise_application_error(-20088, 'The file which corresponds to GUID: '||v_guid||' does not exists in the bookkeeping database!');
END;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE updateDataQualityFlag(v_qualityid NUMBER, lfns varchararray )
is
BEGIN
FOR i in lfns.FIRST .. lfns.LAST LOOP
  update files set inserttimestamp=sys_extract_utc(systimestamp), qualityid= v_qualityid WHERE filename=lfns(i);
END LOOP;
COMMIT;
END;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE bulkcheckfiles(lfns varchararray,  a_Cursor out udt_RefCursor)
is
lfnmeta metadata_table := metadata_table();
n integer := 0;
found NUMBER := 0;
BEGIN
FOR i in lfns.FIRST .. lfns.LAST LOOP
  SELECT count(filename) INTO found FROM files WHERE filename=lfns(i);
  IF found = 0 THEN
    lfnmeta.extend;
    n:=n+1;
    lfnmeta (n):=metadata0bj(lfns(i), NULL,NULL,NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
  END IF;
END LOOP;
open a_Cursor FOR SELECT filename FROM table(lfnmeta);
END;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE bulkupdateReplicaRow(v_replica VARCHAR2, lfns varchararray)
is
BEGIN
FOR i in lfns.FIRST .. lfns.LAST LOOP
 update files set inserttimestamp = sys_extract_utc(systimestamp),gotreplica=v_replica WHERE filename=lfns(i);
 COMMIT;
END LOOP;
END;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE bulkgetTypeVesrsion(lfns varchararray, a_Cursor out udt_RefCursor)
is
lfnmeta metadata_table := metadata_table();
n integer := 0;
found NUMBER := 0;
ftype VARCHAR2(256);

BEGIN
FOR i in lfns.FIRST .. lfns.LAST LOOP
  SELECT count(ftype.version) INTO found FROM files f, filetypes ftype WHERE f.filetypeid=ftype.filetypeid and f.filename=lfns(i);
  IF found > 0 THEN
    SELECT ftype.version INTO ftype FROM files f, filetypes ftype WHERE f.filetypeid=ftype.filetypeid and f.filename=lfns(i);
    lfnmeta.extend;
    n:=n+1;
    lfnmeta (n):=metadata0bj(lfns(i), ftype ,NULL,NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,NULL);
  END IF;
END LOOP;
open a_Cursor FOR SELECT * FROM table(lfnmeta);
END;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE setObsolete
is
BEGIN
update steps set usable='Obsolete' WHERE stepid in (SELECT stepid FROM steps WHERE trunc(INSERTTIMESTAMPS)<=add_months(sysdate+1,-12) and usable!='Obsolete');
COMMIT;
END;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE bulkJobInfo(lfns varchararray, a_Cursor out udt_RefCursor)
is
/*create or replace  type jobMetadata is object(lfn VARCHAR2(256),
  DiracJobId                  NUMBER,
  DiracVersion                VARCHAR2(256),
  EventInputStat              NUMBER,
  ExecTime                    FLOAT,
  FirstEventNumber            NUMBER,
  Location                    VARCHAR2(256),
  Name                        VARCHAR2(256),
  NumberOfEvents              NUMBER,
  StatisticsRequested         NUMBER,
  WNCPUPower                  VARCHAR2(256),
  CPUTime                     FLOAT,
  WNCache                     VARCHAR2(256),
  WNMemory                    VARCHAR2(256),
  WNModel                     VARCHAR2(256),
  WORKERNODE                  VARCHAR2(256),
  WNCPUHS06                   FLOAT,
  jobid                       NUMBER,
  totalLuminosity             NUMBER,
  production                  NUMBER,
  ProgramName                 VARCHAR2(256),
  ProgramVersion              VARCHAR2(256),
  WNMJFHS06                   FLOAT);
create or replace
type bulk_collect_jobMetadata is table of jobMetadata;
*/
n integer := 0;
jobmeta bulk_collect_jobMetadata := bulk_collect_jobMetadata();
BEGIN
FOR i in lfns.FIRST .. lfns.LAST LOOP
  FOR c in (SELECT  jobs.DIRACJOBID, jobs.DIRACVERSION, jobs.EVENTINPUTSTAT, jobs.EXECTIME, jobs.FIRSTEVENTNUMBER,jobs.LOCATION,  jobs.NAME, jobs.NUMBEROFEVENTS,
                 jobs.STATISTICSREQUESTED, jobs.WNCPUPOWER, jobs.CPUTIME, jobs.WNCACHE, jobs.WNMEMORY, jobs.WNMODEL, jobs.WORKERNODE, jobs.WNCPUHS06, jobs.jobid, jobs.totalluminosity, jobs.production, jobs.programName, jobs.programVersion, jobs.WNMJFHS06
   FROM jobs,files WHERE files.jobid=jobs.jobid and  files.filename=lfns(i)) LOOP
     jobmeta.extend;
     n:=n+1;
    jobmeta (n):= jobMetadata(lfns(i), c.DIRACJOBID, c.DIRACVERSION, c.EVENTINPUTSTAT, c.EXECTIME, c.FIRSTEVENTNUMBER,c.LOCATION,  c.NAME, c.NUMBEROFEVENTS,
                 c.STATISTICSREQUESTED, c.WNCPUPOWER, c.CPUTIME, c.WNCACHE, c.WNMEMORY, c.WNMODEL, c.WORKERNODE, c.WNCPUHS06, c.jobid, c.totalluminosity, c.production, c.programName, c.programVersion, c.WNMJFHS06);
  END LOOP;
END LOOP;
open a_Cursor FOR SELECT * FROM table(jobmeta);
END;

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE bulkJobInfoForJobName(jobNames varchararray, a_Cursor out udt_RefCursor)
is
n integer := 0;
jobmeta bulk_collect_jobMetadata := bulk_collect_jobMetadata();
BEGIN
FOR i in jobNames.FIRST .. jobNames.LAST LOOP
  FOR c in (SELECT  jobs.DIRACJOBID, jobs.DIRACVERSION, jobs.EVENTINPUTSTAT, jobs.EXECTIME, jobs.FIRSTEVENTNUMBER,jobs.LOCATION,  jobs.NAME, jobs.NUMBEROFEVENTS,
                 jobs.STATISTICSREQUESTED, jobs.WNCPUPOWER, jobs.CPUTIME, jobs.WNCACHE, jobs.WNMEMORY, jobs.WNMODEL, jobs.WORKERNODE, jobs.WNCPUHS06, jobs.jobid, jobs.totalluminosity, jobs.production, jobs.programName, jobs.programVersion,WNMJFHS06
   FROM jobs,files WHERE files.jobid=jobs.jobid and  jobs.name=jobNames(i)) LOOP
     jobmeta.extend;
     n:=n+1;
    jobmeta (n):= jobMetadata(jobNames(i), c.DIRACJOBID, c.DIRACVERSION, c.EVENTINPUTSTAT, c.EXECTIME, c.FIRSTEVENTNUMBER,c.LOCATION,  c.NAME, c.NUMBEROFEVENTS,
                 c.STATISTICSREQUESTED, c.WNCPUPOWER, c.CPUTIME, c.WNCACHE, c.WNMEMORY, c.WNMODEL, c.WORKERNODE, c.WNCPUHS06, c.jobid, c.totalluminosity, c.production, c.programName, c.programVersion, c.WNMJFHS06);
  END LOOP;
END LOOP;
open a_Cursor FOR SELECT * FROM table(jobmeta);
END;

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE bulkJobInfoForJobId(jobids numberarray, a_Cursor out udt_RefCursor)
is
n integer := 0;
jobmeta bulk_collect_jobMetadata := bulk_collect_jobMetadata();
BEGIN
FOR i in jobids.FIRST .. jobids.LAST LOOP
  FOR c in (SELECT  DISTINCT jobs.DIRACJOBID, jobs.DIRACVERSION, jobs.EVENTINPUTSTAT, jobs.EXECTIME, jobs.FIRSTEVENTNUMBER,jobs.LOCATION,  jobs.NAME, jobs.NUMBEROFEVENTS,
                 jobs.STATISTICSREQUESTED, jobs.WNCPUPOWER, jobs.CPUTIME, jobs.WNCACHE, jobs.WNMEMORY, jobs.WNMODEL, jobs.WORKERNODE, jobs.WNCPUHS06, jobs.jobid, jobs.totalluminosity, jobs.production, jobs.programName, jobs.programVersion, WNMJFHS06
   FROM jobs,files WHERE files.jobid=jobs.jobid and  jobs.diracjobid= jobids(i) Order by jobs.name) LOOP
     jobmeta.extend;
     n:=n+1;
    jobmeta (n):= jobMetadata(jobids(i), c.DIRACJOBID, c.DIRACVERSION, c.EVENTINPUTSTAT, c.EXECTIME, c.FIRSTEVENTNUMBER,c.LOCATION,  c.NAME, c.NUMBEROFEVENTS,
                 c.STATISTICSREQUESTED, c.WNCPUPOWER, c.CPUTIME, c.WNCACHE, c.WNMEMORY, c.WNMODEL, c.WORKERNODE, c.WNCPUHS06, c.jobid, c.totalluminosity, c.production, c.programName, c.programVersion, c.WNMJFHS06);
  END LOOP;
END LOOP;
open a_Cursor FOR SELECT * FROM table(jobmeta);
END;

PROCEDURE insertRunStatus(v_runnumber NUMBER, v_JobId NUMBER, v_Finished VARCHAR2)is
nbrows NUMBER;
BEGIN
    nbrows := 0;
    SELECT count(*) INTO nbrows FROM runstatus WHERE runnumber=v_runnumber;
    IF nbrows = 0 THEN
      INSERT INTO runstatus(
         runnumber,
         JobId,
         finished
         ) VALUES(
                v_runnumber,
                v_JobId,
                v_Finished);
   COMMIT;
   END IF;
  EXCEPTION
  WHEN DUP_VAL_ON_INDEX THEN
   update runstatus set Finished= v_Finished WHERE runnumber=v_runnumber and jobid=v_JobId;
   COMMIT;
  END;

PROCEDURE setRunFinished(
  v_runnumber NUMBER,
  isFinished VARCHAR2
 )is
 BEGIN
  update runstatus set Finished= isFinished WHERE runnumber=v_runnumber;
 IF SQL%ROWCOUNT = 0 THEN
  raise_application_error(-20088, 'The '|| v_runnumber ||' does not exists in the bookkeeping database!');
 ELSE
   COMMIT;
 END IF;
END;

PROCEDURE bulkupdateFileMetaData(files bigvarchararray) is
n NUMBER;
BEGIN
FOR i in files.FIRST .. files.LAST LOOP
   EXECUTE IMMEDIATE files(i);
END LOOP;
END;

PROCEDURE updateLuminosity(v_runnumber NUMBER)is
BEGIN
FOR c in (SELECT f.filename, f.luminosity, f.fileid FROM jobs j, files f WHERE j.jobid=f.jobid and j.runnumber=v_runnumber and j.production<0) LOOP
  updateDesLuminosity(c.fileid);
END LOOP;
END;

PROCEDURE updateDesLuminosity(v_fileid NUMBER)is
lumi NUMBER;
BEGIN
IF v_fileid = 0 THEN
  RETURN;
END IF;
FOR c in (SELECT f.filename, f.fileid, j.jobid FROM jobs j, files f, inputfiles i, filetypes ft WHERE ft.filetypeid=f.filetypeid and ft.name!='LOG' and j.jobid=f.jobid and  j.jobid=i.jobid and i.fileid=v_fileid) LOOP
  SELECT sum(f.luminosity) INTO lumi FROM inputfiles i, files f WHERE f.fileid=i.fileid and i.jobid=c.jobid;
  IF lumi > 0 THEN
    --dbms_output.put_line('update files set luminosity=' || lumi || ' WHERE filename='||c.filename);
    update files set luminosity=lumi WHERE fileid=c.fileid;
    updateDesLuminosity(c.fileid);
  END IF;
END LOOP;
END;

PROCEDURE getFileDesJobId(
   v_Filename                      VARCHAR2,
   a_Cursor                        out udt_RefCursor
 ) is
 BEGIN
    open a_Cursor FOR
      SELECT i.jobid FROM inputfiles i, files f WHERE i.fileid=f.fileid and f.filename=v_Filename;
 END;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE getAllMetadata(
   v_jobid NUMBER,
   v_prod   NUMBER,
   a_Cursor                        out udt_RefCursor
 ) is
 BEGIN
  IF v_prod > 0  THEN
    open a_Cursor FOR
    SELECT files.fileName,files.fileid,files.gotreplica, jobs.production, files.eventstat,
	   files.eventtypeid, files.luminosity, files.instLuminosity, filetypes.name FROM files, jobs, filetypes WHERE files.filetypeid=filetypes.filetypeid and jobs.jobid=files.jobid and files.jobid=v_jobid and jobs.production=v_prod;
    ELSE
    open a_Cursor FOR
      SELECT files.fileName,files.fileid,files.gotreplica, 0, files.eventstat,
	   files.eventtypeid, files.luminosity, files.instLuminosity, filetypes.name FROM files, filetypes WHERE files.filetypeid=filetypes.filetypeid and files.jobid=v_jobid;
  END IF;
 END;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
FUNCTION getProducedEvents(v_prodid NUMBER) RETURN NUMBER
is
retVal NUMBER := 0;
BEGIN
SELECT sum(f.eventstat) INTO retVal
  FROM files f,
       jobs j, 
       (SELECT scont.production, s.stepid
	  FROM stepscontainer scont,
               steps s
	  WHERE
            scont.stepid = s.stepid and
            scont.production=v_prodid and
	    scont.step=(SELECT max(step) FROM stepscontainer WHERE stepscontainer.production=v_prodid)) firsts
  WHERE j.jobid=f.jobid and
        j.production=firsts.production and 
        j.stepid=firsts.stepid;
RETURN retVal;
EXCEPTION
  WHEN OTHERS THEN
    raise_application_error(-20005, 'error found during the event NUMBER calculation');
END;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE bulkgetIdsFromFiles(lfns varchararray,  a_Cursor out udt_RefCursor)
is
lfnmeta metadata_table := metadata_table();
n integer := 0;
fileid NUMBER := 0;
filetypeid NUMBER := 0;
jobid NUMBER := 0;
BEGIN
FOR i in lfns.FIRST .. lfns.LAST LOOP
  BEGIN 
    SELECT fileid, jobid, filetypeid INTO fileid, jobid, filetypeid FROM files WHERE filename=lfns(i);
    lfnmeta.extend;
    n:=n+1;
    lfnmeta(n):=metadata0bj(lfns(i), NULL,NULL,NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, jobid,NULL, NULL, NULL, NULL, NULL, fileid,filetypeid);
  EXCEPTION WHEN NO_DATA_FOUND THEN
         NULL;
  END;
 END LOOP;
open a_Cursor FOR SELECT FILENAME, jobid, fileid, filetypeid FROM table(lfnmeta);
END;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE insertProdnOutputFtypes(v_production NUMBER, v_stepid NUMBER, v_filetypeid NUMBER, v_visible char, v_eventtype NUMBER)IS
BEGIN
  INSERT INTO productionoutputfiles(production, stepid, filetypeid, visible, eventtypeid)VALUES(v_production,v_stepid, v_filetypeid, v_visible,v_eventtype);
  COMMIT;
EXCEPTION
  WHEN DUP_VAL_ON_INDEX THEN
    DBMS_OUTPUT.put_line ('EXISTS:'||v_production||'->'||v_stepid||'->'||v_filetypeid||'->'||v_visible||'->'||v_eventtype);
    --NOT: If the production is already in the table, we only change the step!!!
    UPDATE productionoutputfiles SET stepid=v_stepid WHERE production=v_production and filetypeid=v_filetypeid and visible =v_visible and eventtypeid=v_eventtype;
    COMMIT;
END;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
function getJobIdWithoutReplicaCheck(
  v_FileName             varchar2
 )return number
 is
 jId number;
 begin
  select jobs.jobid into jId from files,jobs where
       files.jobid=jobs.jobid and
       files.FileName=v_FileName;

   return (jId);
   EXCEPTION WHEN OTHERS THEN
  return 0;
end;
END; 
/
