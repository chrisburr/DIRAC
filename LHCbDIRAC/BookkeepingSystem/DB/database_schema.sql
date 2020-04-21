/* ---------------------------------------------------------------------------#
# (c) Copyright 2019 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                      */

-- Bookkeeping DB schema (Oracle)


CREATE OR replace TYPE stepobj IS object (
  stepid              NUMBER,
  stepname            VARCHAR2(256),
  applicationname     VARCHAR2(128),
  applicationversion  VARCHAR2(128),
  optionfiles         VARCHAR2(1000),
  dddb                VARCHAR2(256),
  conddb              VARCHAR2(256),
  extrapackages       VARCHAR2(256),
  visible             CHAR(1),
  processingpass      VARCHAR2(256),
  usable              VARCHAR2(10),
  dqtag               VARCHAR2(256),
  optionsformat       VARCHAR2(30),
  ismulticore         CHAR(1),
  systemconfig        VARCHAR2(256),
  mctck               VARCHAR2(256),
  rstepid             NUMBER,
  rstepname           VARCHAR2(256),
  rapplicationname    VARCHAR2(128),
  rapplicationversion VARCHAR2(128),
  roptionfiles        VARCHAR2(1000),
  rdddb               VARCHAR2(256),
  rconddb             VARCHAR2(256),
  rextrapackages      VARCHAR2(256),
  rvisible            CHAR(1),
  rprocessingpass     VARCHAR2(256),
  rusable             VARCHAR2(10),
  rdqtag              VARCHAR2(256),
  roptionsformat      VARCHAR2(30),
  rismulticore        CHAR(1),
  rsystemconfig       VARCHAR2(256),
  rmctck              VARCHAR2(256)
);
/

CREATE OR REPLACE TYPE step_table is table of stepobj;
/

CREATE OR REPLACE TYPE runnb_quality_eventtype is object (runnumber number, dataqualityflag VARCHAR2(256), eventtypeid number);
/

CREATE OR REPLACE TYPE runnb_proc is object (runnumber number, processingpass VARCHAR2(256));
/

CREATE OR REPLACE TYPE run_proc_table is table of runnb_proc;
/

CREATE OR replace TYPE metadata0bj IS object(
  filename        VARCHAR2(256),
  adler32         VARCHAR2(256),
  creationdate    TIMESTAMP(6),
  eventstat       NUMBER,
  eventtypeid     NUMBER,
  name            VARCHAR2(256),
  gotreplica      VARCHAR2(3),
  guid            VARCHAR2(256),
  md5sum          VARCHAR2(256),
  filesize        NUMBER,
  fullstat        NUMBER,
  dataqualityflag VARCHAR2(256),
  jobid           NUMBER(38,0),
  runnumber       NUMBER,
  inserttimestamp TIMESTAMP(6),
  luminosity      NUMBER,
  instluminosity  NUMBER,
  visibilityflag  CHAR(1),
  fileid          NUMBER,
  filetypeid      NUMBER
);
 /

CREATE OR REPLACE TYPE metadata_table is table of metadata0bj;
/

CREATE OR REPLACE TYPE lists IS TABLE OF VARCHAR2(256);
/

CREATE OR REPLACE TYPE jobMetadata is object(lfn VARCHAR2(256),
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
    jobid                       number,
    totalLuminosity             NUMBER,
    production                  NUMBER,
    ProgramName                 VARCHAR2(256),
    ProgramVersion              VARCHAR2(256),
    WNMJFHS06                   FLOAT
);
/

CREATE OR REPLACE TYPE ftype AS object(
  name VARCHAR2(256),
  visible char(1)
);
/

CREATE OR REPLACE TYPE filetypesARRAY is VARRAY(30) OF ftype;
/

CREATE OR REPLACE TYPE directoryMetadata_new is object(
  lfn                   VARCHAR2(256),
  production            NUMBER,
  configname            VARCHAR2(256),
  configversion         VARCHAR2(256),
  eventtypeid           NUMBER,
  filetype              VARCHAR2(256),
  processingpass        VARCHAR2(256),
  ConditionDescription  VARCHAR2(256),
  VISIBILITYFLAG        CHAR(1));
/

CREATE OR REPLACE TYPE directoryMetadata is object(
  production            NUMBER,
  configname            VARCHAR2(256),
  configversion         VARCHAR2(256),
  eventtypeid           NUMBER,
  filetype              VARCHAR2(256),
  processingpass        VARCHAR2(256),
  ConditionDescription  VARCHAR2(256),
  VISIBILITYFLAG        CHAR(1));
/

CREATE OR REPLACE TYPE bulk_collect_run_quality_evt is table of runnb_quality_eventtype;
/

CREATE OR REPLACE TYPE bulk_collect_jobMetadata is table of jobMetadata;
/

CREATE OR REPLACE TYPE bulk_collect_directoryMetadata is table of directoryMetadata;
/

CREATE OR REPLACE TYPE bulk_collect_directoryMet_new is table of directoryMetadata_new;
/

CREATE SEQUENCE  APPLICATIONS_INDEX_SEQ MINVALUE 1 MAXVALUE 999999999999999999999999999 INCREMENT BY 1 START WITH 1;

CREATE SEQUENCE  CONFIGURATIONID_SEQ MINVALUE 1 MAXVALUE 999999999999999999999999999 INCREMENT BY 1 START WITH 1;

CREATE SEQUENCE  FILEID_SEQ  MINVALUE 1 MAXVALUE 999999999999999999999999999 INCREMENT BY 1 START WITH 1;

CREATE SEQUENCE  GROUPID_SEQ  MINVALUE 1 MAXVALUE 999999999999999999999999999 INCREMENT BY 1 START WITH 1;

CREATE SEQUENCE  JOBID_SEQ  MINVALUE 1 MAXVALUE 999999999999999999999999999 INCREMENT BY 1 START WITH 1;

CREATE SEQUENCE  PASS_INDEX_SEQ  MINVALUE 1 MAXVALUE 999999999999999999999999999 INCREMENT BY 1 START WITH 1;

CREATE SEQUENCE  PRODUCTION_SEQ  MINVALUE -99999999999999999999999999 MAXVALUE -1 INCREMENT BY -1 START WITH -1;

CREATE SEQUENCE  SIMULATIONCONDID_SEQ  MINVALUE 1 MAXVALUE 999999999999999999999999999 INCREMENT BY 1 START WITH 1;

CREATE SEQUENCE  TAGS_INDEX_SEQ  MINVALUE 1 MAXVALUE 999999999999999999999999999 INCREMENT BY 1 START WITH 1;

CREATE GLOBAL TEMPORARY TABLE Stepstmp(
    stepid              NUMBER,
    stepname            VARCHAR2(256 BYTE),
    applicationname     VARCHAR2(128 BYTE),
    applicationversion  VARCHAR2(128 BYTE),
    optionfiles         VARCHAR2(1000 BYTE),
    dddb                VARCHAR2(256 BYTE),
    conddb              VARCHAR2(256 BYTE),
    extrapackages       VARCHAR2(256 BYTE),
    visible             CHAR(1 BYTE) DEFAULT 'Y',
    processingpass      VARCHAR2(256 BYTE),
    usable              VARCHAR2(10 BYTE) DEFAULT 'Not ready',
    dqtag               VARCHAR2(256 BYTE),
    optionsformat       VARCHAR2(30 BYTE),
    ismulticore         CHAR(1 BYTE) DEFAULT 'N',
    systemconfig        VARCHAR2(256 BYTE),
    mctck               VARCHAR2(256 BYTE),
    rstepid             NUMBER,
    rstepname           VARCHAR2(256 BYTE),
    rapplicationname    VARCHAR2(128 BYTE),
    rapplicationversion VARCHAR2(128 BYTE),
    roptionfiles        VARCHAR2(1000 BYTE),
    rdddb               VARCHAR2(256 BYTE),
    rconddb             VARCHAR2(256 BYTE),
    rextrapackages      VARCHAR2(256 BYTE),
    rvisible            CHAR(1 BYTE),
    rprocessingpass     VARCHAR2(256 BYTE),
    rusable             VARCHAR2(10 BYTE),
    rdqtag              VARCHAR2(256 BYTE),
    roptionsformat      VARCHAR2(30 BYTE),
    rismulticore        CHAR(1 BYTE) DEFAULT 'N',
    rsystemconfig       VARCHAR2(256 BYTE),
    rmctck              VARCHAR2(256 BYTE)
) ON COMMIT DELETE ROWS;

---------------------------------------------------------------------------------------
CREATE TABLE Tags(
    tagid           NUMBER,
    name            VARCHAR2(256 BYTE),
    tag             VARCHAR2(256 BYTE),
    inserttimestamp TIMESTAMP (6) DEFAULT SYSTIMESTAMP
);

---------------------------------------------------------------------------------------
CREATE TABLE Processing(
    id       NUMBER,
    parentid NUMBER,
    name     VARCHAR2(256 BYTE),
    CONSTRAINT PROCESSING_PK PRIMARY KEY (id),
    CONSTRAINT PROCESSING_FK FOREIGN KEY (parentid) REFERENCES Processing (id)
);

CREATE INDEX PROCESSING_PID
  ON Processing (parentid);

CREATE INDEX PROCESSING_PID_NAME
  ON Processing (parentid, name);

CREATE OR REPLACE EDITIONABLE TRIGGER PROCESSING_BEFORE_INSERT
BEFORE INSERT
  ON Processing
    FOR EACH ROW
  DECLARE
  BEGIN
  if INSTR(:new.name,'/') > 0 then
    RAISE_APPLICATION_ERROR(-20001,'The processing pass name can not contain / characther!!!');
  END if;
END;

---------------------------------------------------------------------------------------
CREATE TABLE Filetypes(
    filetypeid  NUMBER,
    description VARCHAR2(256 BYTE),
    name        VARCHAR2(256 BYTE),
    version     VARCHAR2(256 BYTE),
    PRIMARY KEY (filetypeid),
    CONSTRAINT FILETYPES_NAME_VERSION UNIQUE (name, version),
    CONSTRAINT FILETYPES_ID_NAME_UK UNIQUE (filetypeid, name)
);

---------------------------------------------------------------------------------------
CREATE TABLE Applications(
    applicationid      NUMBER,
    applicationname    VARCHAR2(128 BYTE) NOT NULL,
    applicationversion VARCHAR2(128 BYTE) NOT NULL,
    optionfiles        VARCHAR2(1000 BYTE),
    dddb               VARCHAR2(256 BYTE),
    conddb             VARCHAR2(256 BYTE),
    extrapackages      VARCHAR2(256 BYTE),
    PRIMARY KEY (applicationid)
);

---------------------------------------------------------------------------------------
CREATE TABLE Configurations(
    configurationid NUMBER,
    configname      VARCHAR2(128 BYTE) NOT NULL,
    configversion   VARCHAR2(128 BYTE) NOT NULL,
    PRIMARY KEY (configurationid),
    CONSTRAINT CONFIGURATION_UK UNIQUE (configname, configversion)
);

---------------------------------------------------------------------------------------
CREATE TABLE Data_taking_conditions(
    daqperiodid   NUMBER,
    description   VARCHAR2(256 BYTE),
    beamcond      VARCHAR2(256 BYTE),
    beamenergy    VARCHAR2(256 BYTE),
    magneticfield VARCHAR2(256 BYTE),
    velo          VARCHAR2(256 BYTE),
    it            VARCHAR2(256 BYTE),
    tt            VARCHAR2(256 BYTE),
    ot            VARCHAR2(256 BYTE),
    rich1         VARCHAR2(256 BYTE),
    rich2         VARCHAR2(256 BYTE),
    spd_prs       VARCHAR2(256 BYTE),
    ecal          VARCHAR2(256 BYTE),
    hcal          VARCHAR2(256 BYTE),
    muon          VARCHAR2(256 BYTE),
    l0            VARCHAR2(256 BYTE),
    hlt           VARCHAR2(256 BYTE),
    veloposition  VARCHAR2(255 BYTE),
    PRIMARY KEY (daqperiodid)
);

CREATE INDEX DATA_TAKING_CONDITION_ID_DESC
  ON DATA_TAKING_CONDITIONS (daqperiodid, description);

---------------------------------------------------------------------------------------
CREATE TABLE Dataquality(
    qualityid       NUMBER,
    dataqualityflag VARCHAR2(256 BYTE),
    PRIMARY KEY (qualityid)
);

INSERT INTO Dataquality (qualityid,dataqualityflag) SELECT 1,'UNCHECKED' FROM DUAL WHERE NOT EXISTS (SELECT * FROM dataquality WHERE (qualityid=1 AND dataqualityflag='UNCHECKED'));
INSERT INTO Dataquality (qualityid,dataqualityflag) SELECT 2,'OK' FROM DUAL WHERE NOT EXISTS (SELECT * FROM dataquality WHERE (qualityid=2 AND dataqualityflag='OK'));
INSERT INTO Dataquality (qualityid,dataqualityflag) SELECT 3,'BAD' FROM DUAL WHERE NOT EXISTS (SELECT * FROM dataquality WHERE (qualityid=3 AND dataqualityflag='BAD'));
COMMIT;

---------------------------------------------------------------------------------------
CREATE TABLE EVENTTYPES(
    DESCRIPTION VARCHAR2(256 BYTE),
    EVENTTYPEID NUMBER,
    PRIMARY VARCHAR2(256 BYTE),
    PRIMARY KEY (EVENTTYPEID)
);

---------------------------------------------------------------------------------------
CREATE TABLE SIMULATIONCONDITIONS(
    SIMID NUMBER,
    SIMDESCRIPTION VARCHAR2(256 BYTE),
    BEAMCOND VARCHAR2(256 BYTE),
    BEAMENERGY VARCHAR2(256 BYTE),
    GENERATOR VARCHAR2(256 BYTE),
    MAGNETICFIELD VARCHAR2(256 BYTE),
    DETECTORCOND VARCHAR2(256 BYTE),
    LUMINOSITY VARCHAR2(256 BYTE),
    G4SETTINGS VARCHAR2(256 BYTE) DEFAULT ' ',
    VISIBLE CHAR(1 BYTE) DEFAULT 'Y',
    INSERTTIMESTAMPS TIMESTAMP (6) DEFAULT sys_extract_utc(systimestamp),
    CONSTRAINT SIMCOND_PK PRIMARY KEY (SIMID),
    CONSTRAINT SIMDESC UNIQUE (SIMDESCRIPTION),
    CHECK (visible in ('N','Y'))
);

---------------------------------------------------------------------------------------
CREATE TABLE PRODUCTIONSCONTAINER(
    PRODUCTION NUMBER,
    PROCESSINGID NUMBER,
    SIMID NUMBER,
    DAQPERIODID NUMBER,
    TOTALPROCESSING VARCHAR2(256 BYTE),
    CONFIGURATIONID NUMBER,
    CONSTRAINT PK_PRODUCTIONSCONTAINER PRIMARY KEY (PRODUCTION),
    CONSTRAINT FK1_PRODUCTIONSCONTAINER FOREIGN KEY (SIMID) REFERENCES SIMULATIONCONDITIONS (SIMID),
    CONSTRAINT FK2_PRODUCTIONSCONTAINER FOREIGN KEY (DAQPERIODID) REFERENCES DATA_TAKING_CONDITIONS (DAQPERIODID),
    CONSTRAINT FK_PRODUCTIONSCONTAINER_PROC FOREIGN KEY (PROCESSINGID) REFERENCES PROCESSING (ID),
    FOREIGN KEY (CONFIGURATIONID) REFERENCES CONFIGURATIONS (CONFIGURATIONID)
);

CREATE INDEX PRODCONTDAQ ON PRODUCTIONSCONTAINER (DAQPERIODID, PRODUCTION);
CREATE INDEX PRODCONTPSIM ON PRODUCTIONSCONTAINER (SIMID, PRODUCTION);
CREATE INDEX PRODCONT_PROC ON PRODUCTIONSCONTAINER (PROCESSINGID);
CREATE INDEX PRODCONT_PROC_PROD ON PRODUCTIONSCONTAINER (PROCESSINGID, PRODUCTION);

---------------------------------------------------------------------------------------
CREATE TABLE STEPS(
    STEPID NUMBER,
    STEPNAME VARCHAR2(256 BYTE),
    APPLICATIONNAME VARCHAR2(128 BYTE) NOT NULL DISABLE,
    APPLICATIONVERSION VARCHAR2(128 BYTE) NOT NULL DISABLE,
    OPTIONFILES VARCHAR2(1000 BYTE),
    DDDB VARCHAR2(256 BYTE),
    CONDDB VARCHAR2(256 BYTE),
    EXTRAPACKAGES VARCHAR2(256 BYTE),
    INSERTTIMESTAMPS TIMESTAMP (6) DEFAULT sys_extract_utc(systimestamp),
    VISIBLE CHAR(1 BYTE) DEFAULT 'Y',
    INPUTFILETYPES FILETYPESARRAY ,
    OUTPUTFILETYPES FILETYPESARRAY,
    PROCESSINGPASS VARCHAR2(256 BYTE),
    USABLE VARCHAR2(10 BYTE) DEFAULT 'Not ready',
    DQTAG VARCHAR2(256 BYTE),
    OPTIONSFORMAT VARCHAR2(30 BYTE),
    ISMULTICORE CHAR(1 BYTE) DEFAULT 'N',
    SYSTEMCONFIG VARCHAR2(256 BYTE),
    MCTCK VARCHAR2(256 BYTE),
    CHECK (visible in ('N','Y')),
    PRIMARY KEY (STEPID),
    CONSTRAINT S_PROCESSINGPASS CHECK (processingpass is not null),
    CHECK (USABLE='Yes' OR USABLE='Not ready' OR USABLE='Obsolete'),
    CHECK (isMulticore in ('N','Y'))
);

CREATE OR REPLACE EDITIONABLE TRIGGER STEP_INSERT
before insert on steps
referencing new as new old as old
for each row
declare
begin
if :new.DDDB ='NULL' or :new.DDDB = 'None' or :new.DDDB = '' then
   :new.DDDB:=null;
end if;
if :new.conddb='NULL' or :new.Conddb = 'None' or :new.Conddb = '' then
  :new.conddb := null;
end if;
end;
/

CREATE OR REPLACE EDITIONABLE TRIGGER STEPS_BEFORE_INSERT
BEFORE INSERT
   ON steps
   FOR EACH ROW
DECLARE
BEGIN

  if INSTR(:new.processingpass,'/') > 0 then
    RAISE_APPLICATION_ERROR(-20001,'The processing pass name can not contain / characther!!!');
  END if;

END;
/

CREATE OR REPLACE EDITIONABLE TRIGGER STEP_UPDATE
before update on steps
referencing new as new old as old
for each row
declare
rowcnt number;
begin
SELECT COUNT(*) INTO rowcnt from stepscontainer s where s.stepid=:new.stepid;
if rowcnt > 0 then
   DBMS_OUTPUT.PUT_LINE('      Tag: '||:new.Visible||:old.stepname);
   :new.stepname:=:old.stepname;
   :new.applicationname:=:old.applicationname;
   :new.applicationversion:=:old.applicationversion;
   :new.optionfiles:=:old.optionfiles;
   :new.DDDB:=:old.DDDB;
   :new.conddb:=:old.conddb;
   :new.extrapackages:=:old.extrapackages;
   :new.visible:=:old.visible;
   :new.inputfiletypes:=:old.inputfiletypes;
   :new.outputfiletypes:=:old.outputfiletypes;
   :new.processingpass:=:old.processingpass;
   --raise_application_error (-20999,'You are not allowed to modify already used steps!');

end if;
end;
/

---------------------------------------------------------------------------------------
CREATE TABLE JOBS(
    JOBID               NUMBER,
    CONFIGURATIONID     NUMBER,
    DIRACJOBID          NUMBER,
    DIRACVERSION        VARCHAR2(256 BYTE),
    EVENTINPUTSTAT      NUMBER,
    EXECTIME            FLOAT(126),
    FIRSTEVENTNUMBER    NUMBER,
    GEOMETRYVERSION     VARCHAR2(256 BYTE),
    GRIDJOBID           VARCHAR2(256 BYTE),
    JOBEND              TIMESTAMP (6),
    JOBSTART            TIMESTAMP (6),
    LOCALJOBID          VARCHAR2(256 BYTE),
    LOCATION            VARCHAR2(256 BYTE),
    NAME                VARCHAR2(256 BYTE),
    NUMBEROFEVENTS      NUMBER,
    PRODUCTION          NUMBER,
    PROGRAMNAME         VARCHAR2(256 BYTE),
    PROGRAMVERSION      VARCHAR2(256 BYTE),
    STATISTICSREQUESTED NUMBER,
    WNCPUPOWER          VARCHAR2(256 BYTE),
    CPUTIME             FLOAT(126),
    WNCACHE             VARCHAR2(256 BYTE),
    WNMEMORY            VARCHAR2(256 BYTE),
    WNMODEL             VARCHAR2(256 BYTE),
    WORKERNODE          VARCHAR2(256 BYTE),
    GENERATOR           VARCHAR2(256 BYTE),
    RUNNUMBER           NUMBER,
    FILLNUMBER          NUMBER,
    WNCPUHS06           FLOAT(126) DEFAULT 0.0,
    TOTALLUMINOSITY     NUMBER DEFAULT 0,
    TCK                 VARCHAR2(20 BYTE) DEFAULT 'None',
    STEPID              NUMBER,
    WNMJFHS06           FLOAT(126),
    HLT2TCK             VARCHAR2(20 BYTE),
    NUMBEROFPROCESSORS  NUMBER DEFAULT 1,
    PRIMARY KEY (JOBID),
    CONSTRAINT JOB_NAME_UNIQUE UNIQUE (NAME),
    CONSTRAINT FK_PRODCONT_PROD FOREIGN KEY (PRODUCTION) REFERENCES PRODUCTIONSCONTAINER (PRODUCTION),
    CONSTRAINT JOBS_FK1 FOREIGN KEY (CONFIGURATIONID) REFERENCES CONFIGURATIONS (CONFIGURATIONID),
    CONSTRAINT FK_JOBS_STEPID FOREIGN KEY (STEPID) REFERENCES STEPS (STEPID)
) PARTITION BY RANGE (PRODUCTION)
  SUBPARTITION BY HASH (CONFIGURATIONID)
  SUBPARTITION TEMPLATE (
    SUBPARTITION CONFIG1,
    SUBPARTITION CONFIG2,
    SUBPARTITION CONFIG3,
    SUBPARTITION CONFIG4,
    SUBPARTITION CONFIG5,
    SUBPARTITION CONFIG6,
    SUBPARTITION CONFIG7,
    SUBPARTITION CONFIG8 )
  (PARTITION RUNLAST  VALUES LESS THAN (-187450),
   PARTITION RUN2     VALUES LESS THAN (-90000),
   PARTITION RUN1     VALUES LESS THAN (0),
   PARTITION PROD1    VALUES LESS THAN (33612),
   PARTITION PROD2    VALUES LESS THAN (42466),
   PARTITION PROD3    VALUES LESS THAN (49181),
   PARTITION PRODLAST VALUES LESS THAN (MAXVALUE));

CREATE INDEX CONF_JOB_RUN
  ON JOBS (CONFIGURATIONID, JOBID, RUNNUMBER);

CREATE INDEX JOBSPROGNAMEANDVERSION
  ON JOBS (PROGRAMNAME, PROGRAMVERSION);

CREATE INDEX JOBS_DIRACJOBID_JOBID
  ON JOBS (DIRACJOBID, JOBID) LOCAL;

CREATE INDEX JOBS_FILL_RUNNUMBER
  ON JOBS (FILLNUMBER, RUNNUMBER);

CREATE INDEX JOBS_PRODUCTIONID
  ON JOBS (PRODUCTION);

CREATE INDEX JOBS_PROD_CONFIG_JOBID
  ON JOBS (PRODUCTION, CONFIGURATIONID, JOBID) LOCAL;

CREATE INDEX PROD_START_END
  ON JOBS (PRODUCTION, JOBSTART, JOBEND);

CREATE INDEX RUNNUMBER
  ON JOBS (RUNNUMBER);


---------------------------------------------------------------------------------------
CREATE TABLE FILES(
    FILEID          NUMBER,
    ADLER32         VARCHAR2(256 BYTE),
    CREATIONDATE    TIMESTAMP (6),
    EVENTSTAT       NUMBER,
    EVENTTYPEID     NUMBER,
    FILENAME        VARCHAR2(256 BYTE) NOT NULL,
    FILETYPEID      NUMBER,
    GOTREPLICA      VARCHAR2(3 BYTE) DEFAULT 'No',
    GUID            VARCHAR2(256 BYTE) NOT NULL,
    JOBID           NUMBER(38, 0),
    MD5SUM          VARCHAR2(256 BYTE) NOT NULL,
    FILESIZE        NUMBER DEFAULT 0,
    QUALITYID       NUMBER DEFAULT 1,
    INSERTTIMESTAMP TIMESTAMP (6) DEFAULT current_timestamp NOT NULL,
    FULLSTAT        NUMBER,
    PHYSICSTAT      NUMBER,
    LUMINOSITY      NUMBER DEFAULT 0,
    VISIBILITYFLAG  CHAR(1 BYTE) DEFAULT 'Y',
    INSTLUMINOSITY  NUMBER DEFAULT 0,
    CONSTRAINT FILES_PK11 PRIMARY KEY (FILEID),
    CONSTRAINT FILES_FILENAME_UNIQUE UNIQUE (FILENAME),
    CONSTRAINT CHECK_PHYSICSTAT CHECK (physicstat < 0),
    CHECK (visibilityFlag IN ('N', 'Y')),
    CONSTRAINT FILES_FK11 FOREIGN KEY (EVENTTYPEID) REFERENCES EVENTTYPES (EVENTTYPEID),
    CONSTRAINT FILES_FK21 FOREIGN KEY (FILETYPEID) REFERENCES FILETYPES (FILETYPEID),
    CONSTRAINT FK_QUALITYID FOREIGN KEY (QUALITYID) REFERENCES DATAQUALITY (QUALITYID),
    CONSTRAINT FILES_FK31 FOREIGN KEY (JOBID) REFERENCES JOBS (JOBID) ON DELETE CASCADE
) PARTITION BY RANGE (JOBID) (
    PARTITION SECT_0020M VALUES LESS THAN (20000000),
    PARTITION SECT_0040M VALUES LESS THAN (40000000),
    PARTITION SECT_0060M VALUES LESS THAN (60000000),
    PARTITION SECT_0080M VALUES LESS THAN (80000000),
    PARTITION SECT_0100M VALUES LESS THAN (100000000),
    PARTITION SECT_0120M VALUES LESS THAN (120000000),
    PARTITION SECT_0140M VALUES LESS THAN (140000000),
    PARTITION SECT_0160M VALUES LESS THAN (160000000),
    PARTITION SECT_0180M VALUES LESS THAN (180000000),
    PARTITION SECT_0200M VALUES LESS THAN (200000000),
    PARTITION SECT_0220M VALUES LESS THAN (220000000),
    PARTITION SECT_0240M VALUES LESS THAN (240000000),
    PARTITION SECT_0260M VALUES LESS THAN (260000000),
    PARTITION SECT_0280M VALUES LESS THAN (280000000),
    PARTITION SECT_0300M VALUES LESS THAN (300000000),
    PARTITION SECT_0320M VALUES LESS THAN (320000000),
    PARTITION SECT_0340M VALUES LESS THAN (340000000),
    PARTITION SECT_0360M VALUES LESS THAN (360000000),
    PARTITION SECT_0380M VALUES LESS THAN (380000000),
    PARTITION SECT_0400M VALUES LESS THAN (400000000),
    PARTITION SECT_0420M VALUES LESS THAN (420000000),
    PARTITION SECT_0440M VALUES LESS THAN (440000000),
    PARTITION SECT_0460M VALUES LESS THAN (460000000),
    PARTITION SECT_0480M VALUES LESS THAN (480000000),
    PARTITION SECT_0500M VALUES LESS THAN (500000000),
    PARTITION SECT_0520M VALUES LESS THAN (520000000)
) NOLOGGING;

CREATE INDEX FILES_FILETYPEID
  ON FILES (FILETYPEID);

ALTER INDEX FILES_FILETYPEID UNUSABLE;

CREATE INDEX FILES_GUID
  ON FILES (GUID);

CREATE INDEX FILES_JOB_EVENT_FILETYPE
  ON FILES (JOBID, EVENTTYPEID, FILETYPEID) local;

CREATE INDEX FILES_TIME_GOTREPLICA
  ON FILES (INSERTTIMESTAMP, GOTREPLICA);

ALTER INDEX FILES_TIME_GOTREPLICA invisible;

CREATE INDEX F_GOTREPLICA
  ON FILES (GOTREPLICA, VISIBILITYFLAG, JOBID) local;


---------------------------------------------------------------------------------------
CREATE TABLE INPUTFILES(
    FILEID NUMBER,
    JOBID  NUMBER,
    CONSTRAINT PK_INPUTFILES_ PRIMARY KEY (FILEID, JOBID),
    CONSTRAINT FILES_FK1 FOREIGN KEY (FILEID) REFERENCES FILES (FILEID),
    CONSTRAINT INPUTFILES_FK31 FOREIGN KEY (JOBID) REFERENCES JOBS (JOBID) ON
    DELETE CASCADE
);

CREATE INDEX inputfiles_jobid_test ON inputfiles (jobid, fileid)
  GLOBAL PARTITION BY RANGE(jobid)
(PARTITION SECT_0020M  VALUES LESS THAN (20000000),
 PARTITION SECT_0040M  VALUES LESS THAN (40000000),
 PARTITION SECT_0060M  VALUES LESS THAN (60000000),
 PARTITION SECT_0080M  VALUES LESS THAN (80000000),
 PARTITION SECT_0100M  VALUES LESS THAN (100000000),
 PARTITION SECT_0120M  VALUES LESS THAN (120000000),
 PARTITION SECT_0140M  VALUES LESS THAN (140000000),
 PARTITION SECT_0160M  VALUES LESS THAN (160000000),
 PARTITION SECT_0180M  VALUES LESS THAN (180000000),
 PARTITION SECT_0200M  VALUES LESS THAN (200000000),
 PARTITION SECT_0220M  VALUES LESS THAN (220000000),
 PARTITION SECT_0240M  VALUES LESS THAN (240000000),
 PARTITION SECT_0260M  VALUES LESS THAN (260000000),
 PARTITION SECT_0280M  VALUES LESS THAN (280000000),
 PARTITION SECT_0300M  VALUES LESS THAN (300000000),
 PARTITION SECT_0320M  VALUES LESS THAN (320000000),
 PARTITION SECT_0340M  VALUES LESS THAN (340000000),
 PARTITION SECT_0360M  VALUES LESS THAN (360000000),
 PARTITION SECT_0380M  VALUES LESS THAN (380000000),
 PARTITION SECT_0400M  VALUES LESS THAN (400000000),
 PARTITION SECT_0420M  VALUES LESS THAN (420000000),
 PARTITION SECT_0440M  VALUES LESS THAN (440000000),
 PARTITION SECT_0460M  VALUES LESS THAN (460000000),
 PARTITION SECT_0480M  VALUES LESS THAN (480000000),
 PARTITION SECT_0500M  VALUES LESS THAN (500000000),
 PARTITION SECT_0520M  VALUES LESS THAN (520000000),
 PARTITION p_greater_than_520000000 VALUES LESS THAN (maxvalue));

---------------------------------------------------------------------------------------
CREATE TABLE NEWRUNQUALITY(
    RUNNUMBER    NUMBER,
    QUALITYID    NUMBER,
    PROCESSINGID NUMBER,
    CONSTRAINT PK_RUN_QUALITY PRIMARY KEY (RUNNUMBER, PROCESSINGID),
    CONSTRAINT FK_QUALITYID_RUN FOREIGN KEY (QUALITYID) REFERENCES DATAQUALITY (QUALITYID),
    CONSTRAINT PROCESSING_ID FOREIGN KEY (PROCESSINGID) REFERENCES PROCESSING (ID)
  );

CREATE INDEX NEWRUNQUALITY_PROC
  ON NEWRUNQUALITY (PROCESSINGID);

CREATE OR REPLACE EDITIONABLE TRIGGER RUNQUALITY
BEFORE UPDATE OR INSERT ON newrunquality REFERENCING NEW AS NEW OLD AS OLD
FOR EACH ROW
  BEGIN
    UPDATE
      files
    SET
      insertTimestamp = sys_extract_utc(systimestamp + INTERVAL '5' MINUTE), files.qualityid = :NEW.qualityid
    WHERE
      jobid IN
      (
	SELECT
	  j.jobid
	FROM
	  jobs j
	WHERE
	  j.runnumber = :NEW.runnumber
	  AND j.production < 0
      );
    UPDATE
      files
    SET
      insertTimestamp = sys_extract_utc(systimestamp + INTERVAL '5' MINUTE),
      files.qualityid = :NEW.qualityid
    WHERE
      files.fileid IN
      (
	SELECT
	  f.fileid
	FROM
	  files f,
	  jobs j
	WHERE
	  j.jobid = f.jobid
	  AND j.runnumber = :NEW.runnumber
	  AND f.gotreplica = 'Yes'
	  AND j.production IN
	  (
	    SELECT
	      prod.production
	    FROM
	      productionscontainer prod
	    WHERE
	      prod.processingid = :NEW.processingid
	  )
      );
  END;


---------------------------------------------------------------------------------------
CREATE TABLE PRODUCTIONOUTPUTFILES(
    PRODUCTION  NUMBER,
    STEPID      NUMBER,
    EVENTTYPEID NUMBER,
    FILETYPEID  NUMBER,
    VISIBLE     CHAR(1 BYTE) DEFAULT 'Y',
    GOTREPLICA  VARCHAR2(3 BYTE) DEFAULT 'No',
    CONSTRAINT PK_PRODUCTIONOUTPUTFILES_P PRIMARY KEY (PRODUCTION, STEPID, FILETYPEID, EVENTTYPEID, VISIBLE),
    CONSTRAINT FK_PRODUCTIONOUTPUTFILES_STEPS FOREIGN KEY (STEPID) REFERENCES STEPS (STEPID),
    CONSTRAINT FK_PRODUCTIONOUTPUTFILES_EVT FOREIGN KEY (EVENTTYPEID) REFERENCES EVENTTYPES (EVENTTYPEID),
    CONSTRAINT FK_PRODUCTIONOUTPUTFILES_FT FOREIGN KEY (FILETYPEID) REFERENCES FILETYPES (FILETYPEID),
    CONSTRAINT FK_PRODUCTIONOUTPUTFILES_PROD FOREIGN KEY (PRODUCTION)
    REFERENCES PRODUCTIONSCONTAINER (PRODUCTION) ON DELETE CASCADE
);


---------------------------------------------------------------------------------------
CREATE TABLE RUNSTATUS(
    RUNNUMBER NUMBER,
    JOBID NUMBER,
    FINISHED CHAR(1 BYTE) DEFAULT 'N',
    CONSTRAINT PK_RUNSTATUS PRIMARY KEY (RUNNUMBER, JOBID),
    CONSTRAINT FK_RUNSTATUS FOREIGN KEY (JOBID)
    REFERENCES JOBS (JOBID)
);

CREATE OR REPLACE EDITIONABLE TRIGGER RUNSTATUS
BEFORE UPDATE ON runstatus
referencing new as new old as old
FOR EACH ROW
  BEGIN
     BOOKKEEPINGORACLEDB.updateLuminosity(:new.runnumber);
  END;

---------------------------------------------------------------------------------------
CREATE TABLE RUNTIMEPROJECTS(
    STEPID           NUMBER,
    RUNTIMEPROJECTID NUMBER,
    CONSTRAINT RUNTIMEPROJECT_PK PRIMARY KEY (RUNTIMEPROJECTID, STEPID),
    CONSTRAINT RUNTIMEPROJECT_FK1 FOREIGN KEY (STEPID) REFERENCES STEPS (STEPID),
    CONSTRAINT RUNTIMEPROJECT_FK2 FOREIGN KEY (RUNTIMEPROJECTID) REFERENCES STEPS (STEPID)
);


CREATE TABLE prodrunview_table (
  production number NOT NULL,
  runnumber number NOT NULL,
  CONSTRAINT prod_run_const UNIQUE (production, runnumber)
);

---------------------------------------------------------------------------------------
BEGIN
  DBMS_SCHEDULER.CREATE_JOB (
     job_name             => 'produpdatejob',
     job_type             => 'PLSQL_BLOCK',
     job_action           => 'BEGIN BKUTILITIES.updateProdOutputFiles(); END;',
     repeat_interval      => 'FREQ=MINUTELY; interval=10',
     start_date           => systimestamp,
     enabled              =>  TRUE
     );
END;
/

BEGIN
  DBMS_SCHEDULER.CREATE_JOB (
     job_name             => 'prodrunupdatejob',
     job_type             => 'PLSQL_BLOCK',
     job_action           => 'BEGIN BKUTILITIES.updateprodrunview(); END;',
     repeat_interval      => 'FREQ=MINUTELY; interval=20',
     start_date           => systimestamp,
     enabled              =>  TRUE
     );
END;
/
