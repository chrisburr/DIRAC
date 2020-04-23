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


CREATE OR REPLACE TYPE stepobj IS OBJECT(
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

CREATE OR REPLACE TYPE step_table IS TABLE OF stepobj;
/

CREATE OR REPLACE TYPE runnb_quality_eventtype IS OBJECT(
    runnumber NUMBER,
    dataqualityflag VARCHAR2(256),
    eventtypeid NUMBER
);
/

CREATE OR REPLACE TYPE runnb_proc IS OBJECT(
    runnumber NUMBER,
    processingpass VARCHAR2(256)
);
/

CREATE OR REPLACE TYPE run_proc_table IS TABLE OF runnb_proc;
/


CREATE OR REPLACE TYPE metadata0bj IS OBJECT(
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

CREATE OR REPLACE TYPE metadata_table IS TABLE OF metadata0bj;
/

CREATE OR REPLACE TYPE lists IS TABLE OF VARCHAR2(256);
/

CREATE OR REPLACE TYPE jobMetadata IS OBJECT(
    lfn                         VARCHAR2(256),
    diracjobid                  NUMBER,
    diracversion                VARCHAR2(256),
    eventinputstat              NUMBER,
    exectime                    FLOAT,
    firsteventnumber            NUMBER,
    location                    VARCHAR2(256),
    name                        VARCHAR2(256),
    numberofevents              NUMBER,
    statisticsrequested         NUMBER,
    wncpupower                  VARCHAR2(256),
    cputime                     FLOAT,
    wncache                     VARCHAR2(256),
    wnmemory                    VARCHAR2(256),
    wnmodel                     VARCHAR2(256),
    workernode                  VARCHAR2(256),
    wncpuhs06                   FLOAT,
    jobid                       number,
    totalluminosity             NUMBER,
    production                  NUMBER,
    programname                 VARCHAR2(256),
    programversion              VARCHAR2(256),
    wnmjfhs06                   FLOAT
);
/

CREATE OR REPLACE TYPE ftype AS OBJECT(
    name VARCHAR2(256),
    visible CHAR(1)
);
/

CREATE OR REPLACE TYPE filetypesARRAY IS VARRAY(30) OF ftype;
/

CREATE OR REPLACE TYPE directoryMetadata_new IS OBJECT(
    lfn                   VARCHAR2(256),
    production            NUMBER,
    configname            VARCHAR2(256),
    configversion         VARCHAR2(256),
    eventtypeid           NUMBER,
    filetype              VARCHAR2(256),
    processingpass        VARCHAR2(256),
    ConditionDescription  VARCHAR2(256),
    VISIBILITYFLAG        CHAR(1)
);
/

CREATE OR REPLACE TYPE directoryMetadata IS object(
    production            NUMBER,
    configname            VARCHAR2(256),
    configversion         VARCHAR2(256),
    eventtypeid           NUMBER,
    filetype              VARCHAR2(256),
    processingpass        VARCHAR2(256),
    ConditionDescription  VARCHAR2(256),
    VISIBILITYFLAG        CHAR(1)
);
/

CREATE OR REPLACE TYPE bulk_collect_run_quality_evt IS TABLE of runnb_quality_eventtype;
/

CREATE OR REPLACE TYPE bulk_collect_jobMetadata IS TABLE of jobMetadata;
/

CREATE OR REPLACE TYPE bulk_collect_directoryMetadata IS TABLE of directoryMetadata;
/

CREATE OR REPLACE TYPE bulk_collect_directoryMet_new IS TABLE of directoryMetadata_new;
/

CREATE SEQUENCE applications_index_seq MINVALUE 1 MAXVALUE 999999999999999999999999999 INCREMENT BY 1 START WITH 1;

CREATE SEQUENCE configurationid_seq MINVALUE 1 MAXVALUE 999999999999999999999999999 INCREMENT BY 1 START WITH 1;

CREATE SEQUENCE fileid_seq MINVALUE 1 MAXVALUE 999999999999999999999999999 INCREMENT BY 1 START WITH 1;

CREATE SEQUENCE groupid_seq MINVALUE 1 MAXVALUE 999999999999999999999999999 INCREMENT BY 1 START WITH 1;

CREATE SEQUENCE jobid_seq MINVALUE 1 MAXVALUE 999999999999999999999999999 INCREMENT BY 1 START WITH 1;

CREATE SEQUENCE pass_index_seq MINVALUE 1 MAXVALUE 999999999999999999999999999 INCREMENT BY 1 START WITH 1;

CREATE SEQUENCE production_seq MINVALUE -99999999999999999999999999 MAXVALUE -1 INCREMENT BY -1 START WITH -1;

CREATE SEQUENCE simulationcondid_seq MINVALUE 1 MAXVALUE 999999999999999999999999999 INCREMENT BY 1 START WITH 1;

CREATE SEQUENCE tags_index_seq MINVALUE 1 MAXVALUE 999999999999999999999999999 INCREMENT BY 1 START WITH 1;

CREATE GLOBAL TEMPORARY TABLE Stepstmp(
    stepid              NUMBER,
    stepname            VARCHAR2(256),
    applicationname     VARCHAR2(128),
    applicationversion  VARCHAR2(128),
    optionfiles         VARCHAR2(1000),
    dddb                VARCHAR2(256),
    conddb              VARCHAR2(256),
    extrapackages       VARCHAR2(256),
    visible             CHAR(1) DEFAULT 'Y',
    processingpass      VARCHAR2(256),
    usable              VARCHAR2(10) DEFAULT 'Not ready',
    dqtag               VARCHAR2(256),
    optionsformat       VARCHAR2(30),
    ismulticore         CHAR(1) DEFAULT 'N',
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
    rismulticore        CHAR(1) DEFAULT 'N',
    rsystemconfig       VARCHAR2(256),
    rmctck              VARCHAR2(256)
) ON COMMIT DELETE ROWS;

---------------------------------------------------------------------------------------
CREATE TABLE tags(
    tagid           NUMBER,
    name            VARCHAR2(256),
    tag             VARCHAR2(256),
    inserttimestamp TIMESTAMP (6) DEFAULT SYSTIMESTAMP
);

---------------------------------------------------------------------------------------
CREATE TABLE processing(
    id       NUMBER,
    parentid NUMBER,
    name     VARCHAR2(256),
    CONSTRAINT processing_pk PRIMARY KEY (id),
    CONSTRAINT processing_fk FOREIGN KEY (parentid) REFERENCES processing (id)
);

CREATE INDEX processing_pid ON processing (parentid);
CREATE INDEX processing_pid_name ON processing (parentid, name);

CREATE OR REPLACE EDITIONABLE TRIGGER processing_before_insert
BEFORE INSERT
  ON processing
    FOR EACH ROW
  DECLARE
  BEGIN
  IF INSTR(:new.name,'/') > 0 THEN
    RAISE_APPLICATION_ERROR(-20001,'The processing pass name can not contain / characther!!!');
  END IF;
END;
/
---------------------------------------------------------------------------------------
CREATE TABLE filetypes(
    filetypeid  NUMBER,
    description VARCHAR2(256),
    name        VARCHAR2(64),
    version     VARCHAR2(256),
    PRIMARY KEY (filetypeid),
    CONSTRAINT filetypes_name_version UNIQUE (name, version),
    CONSTRAINT filetypes_id_name_uk UNIQUE (filetypeid, name)
);

---------------------------------------------------------------------------------------
CREATE TABLE applications(
    applicationid      NUMBER,
    applicationname    VARCHAR2(128) NOT NULL,
    applicationversion VARCHAR2(128) NOT NULL,
    optionfiles        VARCHAR2(1000),
    dddb               VARCHAR2(256),
    conddb             VARCHAR2(256),
    extrapackages      VARCHAR2(256),
    PRIMARY KEY (applicationid)
);

---------------------------------------------------------------------------------------
CREATE TABLE configurations(
    configurationid NUMBER,
    configname      VARCHAR2(128) NOT NULL,
    configversion   VARCHAR2(128) NOT NULL,
    PRIMARY KEY (configurationid),
    CONSTRAINT configuration_uk UNIQUE (configname, configversion)
);

---------------------------------------------------------------------------------------
CREATE TABLE data_taking_conditions(
    daqperiodid   NUMBER,
    description   VARCHAR2(256),
    beamcond      VARCHAR2(256),
    beamenergy    VARCHAR2(256),
    magneticfield VARCHAR2(256),
    velo          VARCHAR2(256),
    it            VARCHAR2(256),
    tt            VARCHAR2(256),
    ot            VARCHAR2(256),
    rich1         VARCHAR2(256),
    rich2         VARCHAR2(256),
    spd_prs       VARCHAR2(256),
    ecal          VARCHAR2(256),
    hcal          VARCHAR2(256),
    muon          VARCHAR2(256),
    l0            VARCHAR2(256),
    hlt           VARCHAR2(256),
    veloposition  VARCHAR2(255),
    PRIMARY KEY (daqperiodid)
);

CREATE INDEX data_taking_condition_id_desc ON data_taking_conditions (daqperiodid, description);

---------------------------------------------------------------------------------------
CREATE TABLE dataquality(
    qualityid       NUMBER,
    dataqualityflag VARCHAR2(256),
    PRIMARY KEY (qualityid)
);

INSERT INTO dataquality (qualityid,dataqualityflag) SELECT 1,'UNCHECKED' FROM DUAL WHERE NOT EXISTS (SELECT * FROM dataquality WHERE (qualityid=1 AND dataqualityflag='UNCHECKED'));
INSERT INTO dataquality (qualityid,dataqualityflag) SELECT 2,'OK' FROM DUAL WHERE NOT EXISTS (SELECT * FROM dataquality WHERE (qualityid=2 AND dataqualityflag='OK'));
INSERT INTO dataquality (qualityid,dataqualityflag) SELECT 3,'BAD' FROM DUAL WHERE NOT EXISTS (SELECT * FROM dataquality WHERE (qualityid=3 AND dataqualityflag='BAD'));
COMMIT;

---------------------------------------------------------------------------------------
CREATE TABLE eventtypes(
    description   VARCHAR2(256),
    eventtypeid   NUMBER,
    PRIMARY       VARCHAR2(256),
    PRIMARY KEY (eventtypeid)
);

---------------------------------------------------------------------------------------
CREATE TABLE simulationconditions(
    simid NUMBER,
    simdescription VARCHAR2(256),
    beamcond VARCHAR2(256),
    beamenergy VARCHAR2(256),
    generator VARCHAR2(256),
    magneticfield VARCHAR2(256),
    detectorcond VARCHAR2(256),
    luminosity VARCHAR2(256),
    g4settings VARCHAR2(256) DEFAULT ' ',
    visible CHAR(1) DEFAULT 'Y',
    inserttimestamps TIMESTAMP (6) DEFAULT SYS_EXTRACT_UTC(SYSTIMESTAMP),
    CONSTRAINT simcond_pk PRIMARY KEY (simid),
    CONSTRAINT simdesc UNIQUE (simdescription),
    CHECK (visible in ('N','Y'))
);

---------------------------------------------------------------------------------------
CREATE TABLE productionscontainer(
    production NUMBER,
    processingid NUMBER,
    simid NUMBER,
    daqperiodid NUMBER,
    totalprocessing VARCHAR2(256),
    configurationid NUMBER,
    CONSTRAINT pk_productionscontainer PRIMARY KEY (production),
    CONSTRAINT fk1_productionscontainer FOREIGN KEY (simid) REFERENCES simulationconditions (SIMID),
    CONSTRAINT fk2_productionscontainer FOREIGN KEY (daqperiodid) REFERENCES data_taking_conditions (DAQPERIODID),
    CONSTRAINT fk_productionscontainer_proc FOREIGN KEY (processingid) REFERENCES processing (ID),
    FOREIGN KEY (configurationid) REFERENCES configurations (configurationid)
);

CREATE INDEX prodcontdaq ON productionscontainer (daqperiodid, production);
CREATE INDEX prodcontpsim ON productionscontainer (simid, production);
CREATE INDEX prodcont_proc ON productionscontainer (processingid);
CREATE INDEX prodcont_proc_prod ON productionscontainer (processingid, production);

---------------------------------------------------------------------------------------
CREATE TABLE steps(
     stepid             NUMBER,
     stepname           VARCHAR2(256),
     applicationname    VARCHAR2(128) NOT NULL DISABLE,
     applicationversion VARCHAR2(128) NOT NULL DISABLE,
     optionfiles        VARCHAR2(1000),
     dddb               VARCHAR2(256),
     conddb             VARCHAR2(256),
     extrapackages      VARCHAR2(256),
     inserttimestamps   TIMESTAMP (6) DEFAULT Sys_extract_utc(systimestamp),
     visible            CHAR(1) DEFAULT 'Y',
     inputfiletypes     FILETYPESARRAY,
     outputfiletypes    FILETYPESARRAY,
     processingpass     VARCHAR2(256),
     usable             VARCHAR2(10) DEFAULT 'Not ready',
     dqtag              VARCHAR2(256),
     optionsformat      VARCHAR2(30),
     ismulticore        CHAR(1) DEFAULT 'N',
     systemconfig       VARCHAR2(256),
     mctck              VARCHAR2(256),
     CHECK (visible IN ('N', 'Y')),
     PRIMARY KEY (stepid),
     CONSTRAINT s_processingpass CHECK (processingpass IS NOT NULL),
     CHECK (usable='Yes' OR usable='Not ready' OR usable='Obsolete'),
     CHECK (ismulticore IN ('N', 'Y'))
);

---------------------------------------------------------------------------------------
CREATE TABLE stepscontainer(
     production  NUMBER,
     stepid      NUMBER,
     step        NUMBER,
     eventtypeid NUMBER,
     CONSTRAINT pk_stepcontainer PRIMARY KEY (production, stepid),
     CONSTRAINT fk_stepcontainer FOREIGN KEY (stepid) REFERENCES steps (stepid),
     CONSTRAINT fk_stepscontainer_eventtypeid FOREIGN KEY (eventtypeid) REFERENCES eventtypes (eventtypeid)
);

CREATE INDEX steps_id ON stepscontainer (stepid);


CREATE OR REPLACE EDITIONABLE TRIGGER step_insert
BEFORE INSERT ON steps
REFERENCING new AS new old AS old
FOR EACH ROW
DECLARE
BEGIN
  IF :new.DDDB ='NULL' OR :new.DDDB = 'None' OR :new.DDDB = '' THEN
     :new.DDDB:=null;
  END IF;
  IF :new.conddb='NULL' OR :new.Conddb = 'None' OR :new.Conddb = '' THEN
    :new.conddb := null;
  END IF;
END;
/

CREATE OR REPLACE EDITIONABLE TRIGGER steps_before_insert
BEFORE INSERT ON steps
FOR EACH ROW
DECLARE
BEGIN
  IF INSTR(:new.processingpass,'/') > 0 then
    RAISE_APPLICATION_ERROR(-20001,'The processing pass name can not contain / characther!!!');
  END IF;
END;
/

CREATE OR REPLACE EDITIONABLE TRIGGER step_update
BEFORE UPDATE ON steps
referencing new AS new old AS old
FOR EACH ROW DECLARE rowcnt NUMBER;
BEGIN
  SELECT COUNT(*) INTO rowcnt FROM stepscontainer s WHERE s.stepid=:new.stepid;
    IF rowcnt > 0 THEN
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
    END IF;
END;
/

---------------------------------------------------------------------------------------
CREATE TABLE jobs(
    jobid               NUMBER,
    configurationid     NUMBER,
    diracjobid          NUMBER,
    diracversion        VARCHAR2(256),
    eventinputstat      NUMBER,
    exectime            FLOAT(126),
    firsteventnumber    NUMBER,
    geometryversion     VARCHAR2(256),
    gridjobid           VARCHAR2(256),
    jobend              TIMESTAMP (6),
    jobstart            TIMESTAMP (6),
    localjobid          VARCHAR2(256),
    location            VARCHAR2(256),
    name                VARCHAR2(256),
    numberofevents      NUMBER,
    production          NUMBER,
    programname         VARCHAR2(256),
    programversion      VARCHAR2(256),
    statisticsrequested NUMBER,
    wncpupower          VARCHAR2(256),
    cputime             FLOAT(126),
    wncache             VARCHAR2(256),
    wnmemory            VARCHAR2(256),
    wnmodel             VARCHAR2(256),
    workernode          VARCHAR2(256),
    generator           VARCHAR2(256),
    runnumber           NUMBER,
    fillnumber          NUMBER,
    wncpuhs06           FLOAT(126) DEFAULT 0.0,
    totalluminosity     NUMBER DEFAULT 0,
    tck                 VARCHAR2(20) DEFAULT 'None',
    stepid              NUMBER,
    wnmjfhs06           FLOAT(126),
    hlt2tck             VARCHAR2(20),
    numberofprocessors  NUMBER DEFAULT 1,
    PRIMARY KEY (jobid),
    CONSTRAINT job_name_unique UNIQUE (name),
    CONSTRAINT fk_prodcont_prod FOREIGN KEY (production) REFERENCES productionscontainer (production),
    CONSTRAINT jobs_fk1 FOREIGN KEY (configurationid) REFERENCES configurations (configurationid),
    CONSTRAINT fk_jobs_stepid FOREIGN KEY (stepid) REFERENCES steps (stepid)
) PARTITION BY RANGE (production)
  SUBPARTITION BY HASH (configurationid)
  SUBPARTITION TEMPLATE (
    SUBPARTITION CONFIG1,
    SUBPARTITION CONFIG2,
    SUBPARTITION CONFIG3,
    SUBPARTITION CONFIG4,
    SUBPARTITION CONFIG5,
    SUBPARTITION CONFIG6,
    SUBPARTITION CONFIG7,
    SUBPARTITION CONFIG8
  )
  (PARTITION RUNLAST  VALUES LESS THAN (-187450),
   PARTITION RUN2     VALUES LESS THAN (-90000),
   PARTITION RUN1     VALUES LESS THAN (0),
   PARTITION PROD1    VALUES LESS THAN (33612),
   PARTITION PROD2    VALUES LESS THAN (42466),
   PARTITION PROD3    VALUES LESS THAN (49181),
   PARTITION PRODLAST VALUES LESS THAN (MAXVALUE));

CREATE INDEX conf_job_run ON jobs (configurationid, jobid, runnumber);
CREATE INDEX jobsprognameandversion ON jobs (programname, programversion);
CREATE INDEX jobs_diracjobid_jobid ON jobs (diracjobid, jobid) LOCAL;
CREATE INDEX jobs_fill_runnumber ON jobs (fillnumber, runnumber);
CREATE INDEX jobs_productionid ON jobs (production);
CREATE INDEX jobs_prod_config_jobid ON jobs (production, configurationid, jobid) LOCAL;
CREATE INDEX prod_start_end ON jobs (production, jobstart, jobend);
CREATE INDEX runnumber ON jobs (runnumber);


---------------------------------------------------------------------------------------
CREATE TABLE files(
    fileid          NUMBER,
    adler32         VARCHAR2(256),
    creationdate    TIMESTAMP (6),
    eventstat       NUMBER,
    eventtypeid     NUMBER,
    filename        VARCHAR2(256) NOT NULL,
    filetypeid      NUMBER,
    gotreplica      VARCHAR2(3) DEFAULT 'No',
    guid            VARCHAR2(256) NOT NULL,
    jobid           NUMBER(38, 0),
    md5sum          VARCHAR2(256) NOT NULL,
    filesize        NUMBER DEFAULT 0,
    qualityid       NUMBER DEFAULT 1,
    inserttimestamp TIMESTAMP (6) DEFAULT current_timestamp NOT NULL,
    fullstat        NUMBER,
    physicstat      NUMBER,
    luminosity      NUMBER DEFAULT 0,
    visibilityflag  CHAR(1) DEFAULT 'Y',
    instluminosity  NUMBER DEFAULT 0,
    CONSTRAINT FILES_PK11 PRIMARY KEY (fileid),
    CONSTRAINT FILES_FILENAME_UNIQUE UNIQUE (filename),
    CONSTRAINT CHECK_PHYSICSTAT CHECK (physicstat < 0),
    CHECK (visibilityFlag IN ('N', 'Y')),
    CONSTRAINT FILES_FK11 FOREIGN KEY (eventtypeid) REFERENCES eventtypes (eventtypeid),
    CONSTRAINT FILES_FK21 FOREIGN KEY (filetypeid) REFERENCES filetypes (filetypeid),
    CONSTRAINT FK_QUALITYID FOREIGN KEY (qualityid) REFERENCES dataquality (qualityid),
    CONSTRAINT FILES_FK31 FOREIGN KEY (jobid) REFERENCES jobs (jobid) ON DELETE CASCADE
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

CREATE INDEX files_filetypeid ON files (filetypeid);
ALTER INDEX files_filetypeid UNUSABLE;
CREATE INDEX files_guid ON files (guid);
CREATE INDEX files_job_event_filetype ON files (jobid, eventtypeid, filetypeid) LOCAL;
CREATE INDEX files_time_gotreplica ON files (inserttimestamp, gotreplica);
ALTER INDEX files_time_gotreplica INVISIBLE;
CREATE INDEX f_gotreplica ON files (gotreplica, visibilityflag, jobid) LOCAL;


---------------------------------------------------------------------------------------
CREATE TABLE inputfiles(
    fileid NUMBER,
    jobid  NUMBER,
    CONSTRAINT pk_inputfiles_ PRIMARY KEY (fileid, jobid),
    CONSTRAINT files_fk1 FOREIGN KEY (fileid) REFERENCES files (fileid),
    CONSTRAINT inputfiles_fk31 FOREIGN KEY (jobid) REFERENCES jobs (jobid) ON
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
CREATE TABLE newrunquality(
    runnumber    NUMBER,
    qualityid    NUMBER,
    processingid NUMBER,
    CONSTRAINT pk_run_quality PRIMARY KEY (runnumber, processingid),
    CONSTRAINT fk_qualityid_run FOREIGN KEY (qualityid) REFERENCES dataquality (qualityid),
    CONSTRAINT processing_id FOREIGN KEY (processingid) REFERENCES processing (id)
  );

CREATE INDEX newrunquality_proc ON newrunquality (processingid);

CREATE OR REPLACE EDITIONABLE TRIGGER runquality
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
/

---------------------------------------------------------------------------------------
CREATE TABLE productionoutputfiles(
    production  NUMBER,
    stepid      NUMBER,
    eventtypeid NUMBER,
    filetypeid  NUMBER,
    visible     CHAR(1) DEFAULT 'Y',
    gotreplica  VARCHAR2(3) DEFAULT 'No',
    CONSTRAINT pk_productionoutputfiles_p PRIMARY KEY (production, stepid, filetypeid, eventtypeid, visible),
    CONSTRAINT fk_productionoutputfiles_steps FOREIGN KEY (stepid) REFERENCES STEPS (stepid),
    CONSTRAINT fk_productionoutputfiles_evt FOREIGN KEY (eventtypeid) REFERENCES eventtypes (eventtypeid),
    CONSTRAINT fk_productionoutputfiles_ft FOREIGN KEY (filetypeid) REFERENCES filetypes (filetypeid),
    CONSTRAINT fk_productionoutputfiles_prod FOREIGN KEY (production)
    REFERENCES productionscontainer (production) ON DELETE CASCADE
);


---------------------------------------------------------------------------------------
CREATE TABLE runstatus(
    runnumber NUMBER,
    jobid NUMBER,
    finished CHAR(1) DEFAULT 'N',
    CONSTRAINT PK_RUNSTATUS PRIMARY KEY (runnumber, jobid),
    CONSTRAINT FK_RUNSTATUS FOREIGN KEY (jobid)
    REFERENCES JOBS (jobid)
);

CREATE OR REPLACE EDITIONABLE TRIGGER runstatus
BEFORE UPDATE ON runstatus
referencing new AS new old AS old
FOR EACH ROW
  BEGIN
     BOOKKEEPINGORACLEDB.updateLuminosity(:new.runnumber);
  END;

---------------------------------------------------------------------------------------
CREATE TABLE runtimeprojects(
    stepid           NUMBER,
    runtimeprojectid NUMBER,
    CONSTRAINT runtimeproject_pk PRIMARY KEY (runtimeprojectid, stepid),
    CONSTRAINT runtimeproject_fk1 FOREIGN KEY (stepid) REFERENCES steps (stepid),
    CONSTRAINT runtimeproject_fk2 FOREIGN KEY (runtimeprojectid) REFERENCES steps (stepid)
);


---------------------------------------------------------------------------------------
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
     enabled              => TRUE
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
