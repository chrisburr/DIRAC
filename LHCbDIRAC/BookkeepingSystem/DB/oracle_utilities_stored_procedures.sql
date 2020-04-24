/* ---------------------------------------------------------------------------#
# (c) Copyright 2019 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges AND immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                      */

CREATE OR REPLACE package BKUTILITIES AS
  TYPE numberarray IS TABLE OF NUMBER INDEX BY PLS_INTEGER;

  PROCEDURE updateNbevt(v_production NUMBER);
  PROCEDURE updateJobNbofevt(v_jobid NUMBER);
  PROCEDURE updateEventInputStat(v_production NUMBER, fixstripping BOOLEAN);
  PROCEDURE updateJobEvtinpStat(v_jobid NUMBER, fixstripping BOOLEAN);
  PROCEDURE destroyDatasets;
  PROCEDURE insertProtoPordoutput(v_production NUMBER);
  PROCEDURE updateProtoPordoutput(v_production NUMBER);
  PROCEDURE updateProdOutputFiles;
  PROCEDURE updateprodrunview;
END;
/

CREATE OR REPLACE package body BKUTILITIES AS
PROCEDURE updateNbevt(
  v_production NUMBER
) IS
BEGIN
/* It updates the NUMBER of events for a given production*/
  FOR c IN (SELECT j.jobid
	    FROM jobs j
	    WHERE j.production=v_production)
   LOOP
    updateJobNbofevt(c.jobid);
   END LOOP;
END;

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE updateJobNbofevt(v_jobid NUMBER)is
sumevt NUMBER;
BEGIN
/* UPDATE the NUMBER of event for a given job.
The NUMBER of events is the sum of the eventstat of the input files */
SELECT SUM(f.eventstat) INTO sumevt
  FROM jobs j,
       files f,
       inputfiles i
   WHERE
      i.jobid=v_jobid AND
      i.fileid=f.fileid AND
      f.jobid=j.jobid  AND
      f.eventstat IS NOT NULL AND
      f.filetypeid NOT IN (SELECT filetypeid
			   FROM filetypes
			   WHERE name='RAW');
  IF sumevt > 0 THEN
    UPDATE jobs SET numberofevents=sumevt WHERE jobid=v_jobid;
  --for c in (SELECT j.jobid
  --            FROM jobs j, files f, inputfiles i
  --              WHERE
  --                i.jobid=v_jobid AND
  --                i.fileid=f.fileid AND
  --                f.jobid=j.jobid AND
  --                f.eventstat IS NOT NULL AND f.filetypeid not in (SELECT filetypeid FROM filetypes WHERE name='RAW'))
  --LOOP
 --   updateJobNbofevt(c.jobid);
  --END LOOP;
  END IF;
END;

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE updateEventInputStat(
  v_production NUMBER,
  fixstripping BOOLEAN
) IS
BEGIN
/* It updates the eventinputstat for a given production.
If the fixstripping is true, the value of the eventinputstat is calculated using the
eventinputstat for the input jobs, othetwise we use the eventstat for the input files.
for example: If we want to fix the eventinputstat of reconstructed files (FULL.DST), fixstripping equal False*/
  IF fixstripping = TRUE THEN
    FOR c IN (SELECT j.jobid
	      FROM jobs j,
		   files f
	      WHERE j.jobid=f.jobid
		AND j.production=v_production
	      )
      LOOP
	updateJobEvtinpStat(c.jobid, fixstripping);
      END LOOP;
  ELSE
    for c IN (SELECT j.jobid
	      FROM jobs j,
		   files f
	      WHERE j.jobid=f.jobid AND
		    j.production=v_production AND
		    f.gotreplica='Yes' AND
		    f.visibilityflag='Y')
      LOOP
	updateJobEvtinpStat(c.jobid, fixstripping);
      END LOOP;
  END IF;
END;

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE updateJobEvtinpStat(
  v_jobid NUMBER,
  fixstripping BOOLEAN
) IS
/*It updates the eventinputstat for a given job */
sumevtinp NUMBER;
BEGIN
  IF fixstripping=TRUE THEN
    SELECT sum(j.eventinputstat) into sumevtinp
    FROM jobs j,
	 files f,
	 inputfiles i
    WHERE i.jobid=v_jobid AND
	  i.fileid=f.fileid AND
	  f.jobid=j.jobid;
  ELSE
    SELECT sum(f.eventstat) into sumevtinp
    FROM jobs j,
	 files f,
	 inputfiles i
    WHERE i.jobid=v_jobid AND
	  i.fileid=f.fileid AND
	  f.jobid=j.jobid;
  END IF;
  IF sumevtinp > 0 THEN
    UPDATE jobs SET eventinputstat=sumevtinp WHERE jobid=v_jobid;
  END IF;
END;
PROCEDURE destroyDatasets IS
runsteps numberarray;
productionsteps numberarray;
i NUMBER;
v_production NUMBER;
BEGIN
    v_production:=2; /*this must be same as in the integration test: LHCbDIRAC/tests/Integration/BookkeepingSystem/Test_Bookkeeping.py*/
    /*DELETE run data*/
    DELETE productionscontainer WHERE production=3;
    DELETE stepscontainer WHERE production=3;
    DELETE productionscontainer WHERE production=-1122;
    i:=1;/*before we DELETE the steps FROM the stepcontainer table, the steps must be saved*/
    FOR step IN (SELECT stepid
		 FROM stepscontainer
		 WHERE production=-1122) LOOP
      runsteps(i):=step.stepid;
      i:=i+1;
      dbms_output.put_line('run Step:' || step.stepid);
    END LOOP;
    DELETE stepscontainer WHERE production=-1122;
    DELETE runstatus WHERE runnumber=1122;
    DELETE files WHERE jobid in (SELECT jobid FROM jobs WHERE runnumber=1122);
    DELETE jobs WHERE runnumber=1122;
    FOR i in 1 .. runsteps.COUNT LOOP
      dbms_output.put_line('run step DELETE:' || runsteps(i));
      DELETE steps WHERE stepid=runsteps(i);
    END LOOP;
    /* DELETE production data */
    i:=1;
    /*before we DELETE the steps FROM the stepcontainer table, the steps must be saved*/
    FOR step IN (SELECT stepid FROM stepscontainer WHERE production=v_production) LOOP
      productionsteps(i):=step.stepid;
      i:=i+1;
      dbms_output.put_line('production step:' || step.stepid);
    END LOOP;
    DELETE productionscontainer WHERE production=v_production;
    DELETE stepscontainer WHERE production=v_production;
    DELETE files WHERE jobid in (SELECT jobid FROM jobs WHERE production=v_production);
    DELETE jobs WHERE production=v_production;
    FOR i in 1 .. productionsteps.COUNT LOOP
      dbms_output.put_line('production step DELETE:' || productionsteps(i));
      DELETE steps WHERE stepid=productionsteps(i);
    END LOOP;
    COMMIT;
END;


---------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE insertProtoPordoutput(
  v_production NUMBER
) IS
BEGIN
  FOR prod IN(SELECT j.production,
		     j.stepid,
		     f.eventtypeid,
		     f.filetypeid,
		     f.gotreplica,
		     f.visibilityflag
	      FROM jobs j,
		   files f
	      WHERE j.jobid = f.jobid AND
		    j.production=v_production AND
		    f.gotreplica IS NOT NULL AND
		    f.filetypeid NOT IN(9,17)
	      GROUP BY j.production,
		       j.stepid,
		       f.eventtypeid,
		       f.filetypeid,
		       f.gotreplica,
		       f.visibilityflag
	      ORDER BY f.gotreplica,
		       f.visibilityflag
	      ASC) LOOP
    dbms_output.put_line('Inserting -> Production:' || prod.production || '->step:' || prod.stepid || '->file type:' || prod.filetypeid || '->visible:'||prod.visibilityflag||'->event type:'||prod.eventtypeid||'->replica flag:'||prod.gotreplica);
    INSERT INTO productionoutputfiles(production,
				      stepid,
				      filetypeid,
				      visible,
				      eventtypeid,
				      gotreplica)
	   VALUES(prod.production,
		  prod.stepid,
		  prod.filetypeid,
		  prod.visibilityflag,
		  prod.eventtypeid,
		  prod.gotreplica);
  END LOOP;
END;

---------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE updateProtoPordoutput(
  v_production NUMBER
) is
nb NUMBER;
BEGIN
  FOR prod IN(SELECT j.production,
		     j.stepid,
		     f.eventtypeid,
		     f.filetypeid,
		     f.gotreplica,
		     f.visibilityflag
	      FROM jobs j,
		   files f
	      WHERE j.jobid = f.jobid AND
		    j.production=v_production AND
		    f.gotreplica IS NOT NULL AND
		    f.filetypeid NOT IN(9,17)
	      GROUP BY j.production,
		       j.stepid,
		       f.eventtypeid,
		       f.filetypeid,
		       f.gotreplica,
		       f.visibilityflag
	      ORDER BY f.gotreplica,
		       f.visibilityflag
	      ASC) LOOP
    SELECT count(*) into nb FROM productionoutputfiles WHERE production=prod.production AND eventtypeid=prod.eventtypeid AND filetypeid=prod.filetypeid AND stepid=prod.stepid AND visible=prod.visibilityflag AND gotreplica=prod.gotreplica;
    dbms_output.put_line('Try UPDATE -> Production:' || prod.production || '->step:' || prod.stepid || '->file type:' || prod.filetypeid || '->visible:'||prod.visibilityflag||'->event type:'||prod.eventtypeid||'->replica flag:'||prod.gotreplica);
    IF nb = 0 THEN -- we want to UPDATE only the row, which has modified...
      -- we have to see which rows can be updated
      FOR toupdate IN (SELECT * FROM (SELECT production,
					     stepid,
					     eventtypeid,
					     filetypeid,
					     gotreplica,
					     visible AS visibilityflag
				      FROM productionoutputfiles
				      WHERE production=v_production)
			 minus
		       SELECT j.production,
			      j.stepid,
			      f.eventtypeid,
			      f.filetypeid,
			      f.gotreplica,
			      f.visibilityflag
		       FROM jobs j,
			    files f
		       WHERE j.jobid = f.jobid AND
			     j.production= v_production AND
			     f.gotreplica IS NOT NULL AND
			     f.filetypeid NOT IN(9,17)
		       GROUP BY j.production,
				j.stepid,
				f.eventtypeid,
				f.filetypeid,
				f.gotreplica,
				f.visibilityflag) LOOP
	dbms_output.put_line('Update -> Production:' || prod.production || '->step:' || prod.stepid || '->file type:' || prod.filetypeid || '->visible:'||prod.visibilityflag||'->event type:'||prod.eventtypeid||'->replica flag:'||prod.gotreplica);
	UPDATE productionoutputfiles SET visible=prod.visibilityflag, gotreplica=prod.gotreplica WHERE production=prod.production AND eventtypeid=prod.eventtypeid AND filetypeid=prod.filetypeid AND stepid=prod.stepid AND visible=toupdate.visibilityflag AND gotreplica=toupdate.gotreplica;
      END LOOP;
    END IF;
  END LOOP;
END;


---------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE updateProdOutputFiles IS
nbrows NUMBER;
nbrowstobeprocessed NUMBER;
nb NUMBER;
err_num NUMBER;
err_msg VARCHAR2(1000);
BEGIN
--FOR toprod in (SELECT distinct j.production FROM jobs j, files f WHERE f.jobid=j.jobid AND j.production>0 AND f.gotreplica='Yes') LOOP
  FOR c IN (SELECT j.production
	    FROM jobs j,
		 files f
	    WHERE f.inserttimestamp >= SYSTIMESTAMP - 1 AND
		  j.jobid = f.jobid AND
		  --j.production=toprod.production AND
		  f.gotreplica IS NOT NULL AND
		  f.filetypeid NOT IN(9,17) group by j.production) LOOP
    SELECT COUNT(*) INTO nbrows
    FROM  productionoutputfiles
    WHERE production=c.production;
    SELECT COUNT(*) INTO nbrowstobeprocessed
    FROM (SELECT j.production,
		 j.stepid,
		 f.eventtypeid,
		 f.filetypeid,
		 f.gotreplica,
		 f.visibilityflag
	  FROM jobs j, files f
	  WHERE j.jobid = f.jobid AND
		j.production= c.production AND
		f.gotreplica IS NOT NULL AND
		f.filetypeid NOT IN(9,17)
	  GROUP BY j.production,
		   j.stepid,
		   f.eventtypeid,
		   f.filetypeid,
		   f.gotreplica,
		   f.visibilityflag
	  ORDER BY f.gotreplica,
		   f.visibilityflag
	 );
    IF nbrows > 0 THEN
      IF nbrows = nbrowstobeprocessed THEN
	updateProtoPordoutput(c.production);
      ELSIF nbrows>nbrowstobeprocessed THEN
	updateProtoPordoutput(c.production);
	FOR toDelete IN (SELECT * FROM (SELECT production,
					       stepid,
					       eventtypeid,
					       filetypeid,
					       gotreplica,
					       visible AS visibilityflag
					FROM productionoutputfiles
					WHERE production=c.production)
			   minus
			 SELECT j.production,
				j.stepid,
				f.eventtypeid,
				f.filetypeid,
				f.gotreplica,
				f.visibilityflag
			 FROM jobs j,
			      files f
			 WHERE j.jobid = f.jobid AND
			       j.production= c.production AND
			       f.gotreplica IS NOT NULL AND
			       f.filetypeid NOT IN(9,17)
			 GROUP BY j.production,
				  j.stepid,
				  f.eventtypeid,
				  f.filetypeid,
				  f.gotreplica,
				  f.visibilityflag) LOOP
	  dbms_output.put_line('Delete -> Production:' || toDelete.production || '->step:' || toDelete.stepid || '->file type:' || toDelete.filetypeid || '->visible:'||toDelete.visibilityflag||'->event type:'||toDelete.eventtypeid||'->replica flag:'||toDelete.gotreplica);
	  DELETE productionoutputfiles WHERE production=toDelete.production AND eventtypeid=toDelete.eventtypeid AND filetypeid=toDelete.filetypeid AND stepid=toDelete.stepid AND visible=toDelete.visibilityflag AND gotreplica=toDelete.gotreplica;
      END LOOP;
      ELSIF nbrows < nbrowstobeprocessed THEN
	updateProtoPordoutput(c.production);
	FOR toInsert IN(SELECT * FROM (SELECT j.production,j.stepid, f.eventtypeid, f.filetypeid, f.gotreplica, f.visibilityflag FROM jobs j, files f WHERE
            j.jobid = f.jobid AND
	    j.production= c.production AND
	    f.gotreplica IS NOT NULL AND
	    f.filetypeid NOT IN(9,17) GROUP BY j.production, j.stepid, f.eventtypeid, f.filetypeid, f.gotreplica, f.visibilityflag ORDER BY f.gotreplica,f.visibilityflag)  minus
		   SELECT production, stepid, eventtypeid, filetypeid, gotreplica, visible as visibilityflag FROM productionoutputfiles WHERE production=c.production) LOOP
		dbms_output.put_line('Inserting -> Production:' || toInsert.production || '->step:' || toInsert.stepid || '->file type:' || toInsert.filetypeid || '->visible:'||toInsert.visibilityflag||'->event type:'||toInsert.eventtypeid||'->replica flag:'||toInsert.gotreplica);
		INSERT INTO productionoutputfiles(production, stepid, filetypeid, visible, eventtypeid,gotreplica)VALUES(toInsert.production,toInsert.stepid, toInsert.filetypeid, toInsert.visibilityflag,toInsert.eventtypeid, toInsert.gotreplica);
	END LOOP;
      END if;
    ELSE
	insertProtoPordoutput(c.production);
    END if;
    COMMIT;
  END LOOP;
--END LOOP;
  EXCEPTION
  WHEN OTHERS THEN
    err_num := SQLCODE;
    err_msg := SUBSTR(SQLERRM, 1, 1000);
    utl_mail.send(sender => 'lhcb-geoc@cern.ch',
	    recipients => 'lhcb-bookkeeping@cern.ch',
	    subject    => 'Failed to UPDATE productionoutputfiles',
	    message    => 'ERROR NUMBER:'||err_num||' error message:'||err_msg||' More info: https://lhcb-dirac.readthedocs.io/en/latest/AdministratorGuide/Bookkeeping/administrate_oracle.html#automatic-updating-of-the-productionoutputfiles');
END;
procedure updateprodrunview is
err_num NUMBER;
err_msg VARCHAR2(1000);
begin
-- get the modified production list
 for prod in (select j.production from jobs j, files f WHERE 
                f.inserttimestamp >= SYSTIMESTAMP - 1 AND
                j.jobid = f.jobid AND
                f.gotreplica IS NOT NULL and
                f.filetypeid NOT IN(9,17) group by j.production)
  LOOP
    delete prodrunview where production=prod.production;
    for insertProd in (select j.production, j.runnumber from jobs j, files f where j.jobid=f.jobid and j.production=prod.production and f.gotreplica='Yes' 
                                 and f.visibilityflag='Y' and j.runnumber is not null group by j.production,j.runnumber)
    LOOP
      insert into prodrunview(production,runnumber)values(insertProd.production,insertProd.runnumber);
    END LOOP;
    commit;
  END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
        err_num := SQLCODE;
        err_msg := SUBSTR(SQLERRM, 1, 1000);
        utl_mail.send(sender => 'lhcb-geoc@cern.ch',
                recipients => 'lhcb-bookkeeping@cern.ch',
                subject    => 'Failed to update prodrunview',
                message    => 'ERROR number:'||err_num||' error message:'||err_msg||' More info: https://lhcb-dirac.readthedocs.io/en/latest/AdministratorGuide/Bookkeeping/administrate_oracle.html#automatic-updating-of-the-prodrunview');
end;
END;
/
