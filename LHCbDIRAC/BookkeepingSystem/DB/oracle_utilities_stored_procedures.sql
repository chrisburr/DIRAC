###############################################################################
# (c) Copyright 2019 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                       #
###############################################################################

create or replace package BKUTILITIES as
  TYPE numberarray  IS TABLE OF NUMBER INDEX BY PLS_INTEGER;

  procedure updateNbevt(v_production number);
  procedure updateJobNbofevt(v_jobid number);
  procedure updateEventInputStat(v_production number, fixstripping BOOLEAN);
  procedure updateJobEvtinpStat(v_jobid number, fixstripping BOOLEAN);
  PROCEDURE destroyDatasets;
  procedure insertProtoPordoutput(v_production number);
  procedure updateProtoPordoutput(v_production number);
  PROCEDURE updateProdOutputFiles;
  
end;
/

create or replace package body BKUTILITIES as
procedure updateNbevt(v_production number)is
begin
/* It updates the number of events for a given production*/
for c in (select j.jobid
            from
              jobs j
                where
                  j.production=v_production)
 LOOP
  updateJobNbofevt(c.jobid);
 END LOOP;
end;
procedure updateJobNbofevt(v_jobid number)is
sumevt number;
begin
/* update the number of event for a given job.
The number of events is the sum of the eventstat of the input files */
select sum(f.eventstat) into sumevt
  from
   jobs j, files f, inputfiles i
   where
      i.jobid=v_jobid and
      i.fileid=f.fileid and
      f.jobid=j.jobid  and
      f.eventstat is not null and
      f.filetypeid not in (select filetypeid from filetypes where name='RAW');
if sumevt > 0 then
  update jobs set numberofevents=sumevt where jobid=v_jobid;
  --for c in (select j.jobid
  --            from jobs j, files f, inputfiles i
  --              where
  --                i.jobid=v_jobid and
  --                i.fileid=f.fileid and
  --                f.jobid=j.jobid and
  --                f.eventstat is not null and f.filetypeid not in (select filetypeid from filetypes where name='RAW'))
  --LOOP
 --   updateJobNbofevt(c.jobid);
  --END LOOP;
 End if;
end;
procedure updateEventInputStat(v_production number, fixstripping BOOLEAN) is
begin
/* It updates the eventinputstat for a given production.
If the fixstripping is true, the value of the eventinputstat is calculated using the
eventinputstat for the input jobs, othetwise we use the eventstat for the input files.
for example: If we want to fix the eventinputstat of reconstructed files (FULL.DST), fixstripping equal False*/
if fixstripping = TRUE THEN
  FOR c in (select j.jobid from jobs j, files f where j.jobid=f.jobid and j.production=v_production)
    LOOP
      updateJobEvtinpStat(c.jobid, fixstripping);
    END LOOP;
ELSE
for c in (select j.jobid
            from
              jobs j, files f
                where
                  j.jobid=f.jobid and
                  j.production=v_production and
                  f.gotreplica='Yes' and
                  f.visibilityflag='Y')
  LOOP
    updateJobEvtinpStat(c.jobid, fixstripping);
  END LOOP;
END IF;
end;
procedure updateJobEvtinpStat(v_jobid number, fixstripping BOOLEAN) is
/*It updates the eventinputstat for a given job */
sumevtinp number;
BEGIN
 if fixstripping=TRUE THEN
  select sum(j.eventinputstat) into sumevtinp from jobs j, files f, inputfiles i where i.jobid=v_jobid and i.fileid=f.fileid and f.jobid=j.jobid;
 ELSE
  select sum(f.eventstat) into sumevtinp from jobs j, files f, inputfiles i where i.jobid=v_jobid and i.fileid=f.fileid and f.jobid=j.jobid;
 END IF;
 IF sumevtinp > 0 THEN
    update jobs set eventinputstat=sumevtinp where jobid=v_jobid;
 END IF;
END;
PROCEDURE destroyDatasets IS
runsteps numberarray;
productionsteps numberarray;
i number;
v_production number;
BEGIN
    v_production:=2; /*this must be same as in the integration test: LHCbDIRAC/tests/Integration/BookkeepingSystem/Test_Bookkeeping.py*/
    /*delete run data*/
    delete productionscontainer where production=3;
    delete stepscontainer where production=3;
    DELETE productionscontainer WHERE production=-1122;
    i:=1;/*before we delete the steps from the stepcontainer table, the steps must be saved*/
    FOR step IN (SELECT stepid FROM stepscontainer WHERE production=-1122) LOOP
        runsteps(i):=step.stepid;
        i:=i+1;
        dbms_output.put_line('run Step:' || step.stepid);
    END LOOP;
    DELETE stepscontainer WHERE production=-1122;
    DELETE runstatus WHERE runnumber=1122;
    DELETE files WHERE jobid in (SELECT jobid FROM jobs WHERE runnumber=1122);
    DELETE jobs WHERE runnumber=1122;
    FOR i in 1 .. runsteps.COUNT LOOP
        dbms_output.put_line('run step delete:' || runsteps(i));
        DELETE steps WHERE stepid=runsteps(i);
    END LOOP;
    /* delete production data */
    i:=1;
    /*before we delete the steps from the stepcontainer table, the steps must be saved*/
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
        dbms_output.put_line('production step delete:' || productionsteps(i));
        DELETE steps WHERE stepid=productionsteps(i);
    END LOOP;
    COMMIT;
END;
procedure insertProtoPordoutput(v_production number) is
begin
FOR prod IN(SELECT j.production,J.STEPID, f.eventtypeid, f.filetypeid, f.gotreplica, f.visibilityflag
        FROM jobs j, files f WHERE
            j.jobid = f.jobid AND
            j.production=v_production and
            f.gotreplica IS NOT NULL and
            f.filetypeid NOT IN(9,17) GROUP BY j.production, J.STEPID, f.eventtypeid, f.filetypeid, f.gotreplica, f.visibilityflag Order by f.gotreplica,f.visibilityflag asc) LOOP
    dbms_output.put_line('Inserting -> Production:' || prod.production || '->step:' || prod.stepid || '->file type:' || prod.filetypeid || '->visible:'||prod.visibilityflag||'->event type:'||prod.eventtypeid||'->replica flag:'||prod.gotreplica);
    INSERT INTO productionoutputfiles(production, stepid, filetypeid, visible, eventtypeid,gotreplica)VALUES(prod.production,prod.stepid, prod.filetypeid, prod.visibilityflag,prod.eventtypeid, prod.gotreplica);
end loop;
end;
procedure updateProtoPordoutput(v_production number) is
nb number;
begin
FOR prod IN(SELECT j.production,J.STEPID, f.eventtypeid, f.filetypeid, f.gotreplica, f.visibilityflag
        FROM jobs j, files f WHERE
            j.jobid = f.jobid AND
            j.production=v_production and
            f.gotreplica IS NOT NULL and
            f.filetypeid NOT IN(9,17) GROUP BY j.production, J.STEPID, f.eventtypeid, f.filetypeid, f.gotreplica, f.visibilityflag Order by f.gotreplica,f.visibilityflag asc) LOOP
    select count(*) into nb from productionoutputfiles where production=prod.production AND eventtypeid=prod.eventtypeid AND filetypeid=prod.filetypeid AND stepid=prod.stepid and visible=prod.visibilityflag and gotreplica=prod.gotreplica;
    dbms_output.put_line('Try update -> Production:' || prod.production || '->step:' || prod.stepid || '->file type:' || prod.filetypeid || '->visible:'||prod.visibilityflag||'->event type:'||prod.eventtypeid||'->replica flag:'||prod.gotreplica);
    if nb = 0 then -- we whant to update only the row, which has modified...
        -- we have to see which rows can be updated
        --we have tocheck if being updated row in the productionoutputfiles table
        --it can happen that the productionoutputfiles table contains more row, but the actual row which being updated is not in the table
        select count(*) into nb from productionoutputfiles where production=prod.production AND eventtypeid=prod.eventtypeid AND filetypeid=prod.filetypeid AND stepid=prod.stepid;
        if nb = 0 then
         INSERT INTO productionoutputfiles(production, stepid, filetypeid, visible, eventtypeid,gotreplica)VALUES(prod.production,prod.stepid, prod.filetypeid, prod.visibilityflag,prod.eventtypeid, prod.gotreplica);
        end if;
        for toupdate in (select * from (select production, stepid, eventtypeid, filetypeid, gotreplica, visible as visibilityflag from productionoutputfiles where production=v_production) minus
             SELECT j.production,J.STEPID, f.eventtypeid, f.filetypeid, f.gotreplica, f.visibilityflag FROM jobs j, files f WHERE
            j.jobid = f.jobid AND
            j.production= v_production and
            f.gotreplica IS NOT NULL and
            f.filetypeid NOT IN(9,17) GROUP BY j.production, J.STEPID, f.eventtypeid, f.filetypeid, f.gotreplica, f.visibilityflag) LOOP
                dbms_output.put_line('Update -> Production:' || prod.production || '->step:' || prod.stepid || '->file type:' || prod.filetypeid || '->visible:'||prod.visibilityflag||'->event type:'||prod.eventtypeid||'->replica flag:'||prod.gotreplica);
                UPDATE productionoutputfiles SET visible=prod.visibilityflag, gotreplica=prod.gotreplica WHERE production=prod.production AND eventtypeid=prod.eventtypeid AND filetypeid=prod.filetypeid AND stepid=prod.stepid and visible=toupdate.visibilityflag and gotreplica=toupdate.gotreplica;
        end loop;
   end if;
end loop;
end;
PROCEDURE updateProdOutputFiles IS
nbrows number;
nbrowstobeprocessed number;
nb number;
err_num NUMBER;
err_msg VARCHAR2(1000);
BEGIN
  --FOR toprod in (select distinct j.production from jobs j, files f where f.jobid=j.jobid and j.production>0 and f.gotreplica='Yes') LOOP
    FOR c IN (select j.production from jobs j, files f WHERE
        f.inserttimestamp >= SYSTIMESTAMP - 1 AND
        j.jobid = f.jobid AND
        --j.production=toprod.production and
        f.gotreplica IS NOT NULL and
        f.filetypeid NOT IN(9,17) group by j.production) LOOP
        SELECT count(*) INTO nbrows FROM  productionoutputfiles WHERE production=c.production;
        select count(*) into nbrowstobeprocessed from (SELECT j.production,J.STEPID, f.eventtypeid, f.filetypeid, f.gotreplica, f.visibilityflag FROM jobs j, files f WHERE
            j.jobid = f.jobid AND
            j.production= c.production and
            f.gotreplica IS NOT NULL and
            f.filetypeid NOT IN(9,17) GROUP BY j.production, J.STEPID, f.eventtypeid, f.filetypeid, f.gotreplica, f.visibilityflag Order by f.gotreplica,f.visibilityflag);
        if nbrows > 0 then
               if nbrows = nbrowstobeprocessed then
                   updateProtoPordoutput(c.production);
               elsif nbrows>nbrowstobeprocessed then
                   updateProtoPordoutput(c.production);
                   FOR toDelete in (select * from (select production, stepid, eventtypeid, filetypeid, gotreplica, visible as visibilityflag from productionoutputfiles where production=c.production) minus
                     SELECT j.production,J.STEPID, f.eventtypeid, f.filetypeid, f.gotreplica, f.visibilityflag FROM jobs j, files f WHERE
                        j.jobid = f.jobid AND
                        j.production= c.production and
                        f.gotreplica IS NOT NULL and
                        f.filetypeid NOT IN(9,17) GROUP BY j.production, J.STEPID, f.eventtypeid, f.filetypeid, f.gotreplica, f.visibilityflag) LOOP
                        dbms_output.put_line('Delete -> Production:' || toDelete.production || '->step:' || toDelete.stepid || '->file type:' || toDelete.filetypeid || '->visible:'||toDelete.visibilityflag||'->event type:'||toDelete.eventtypeid||'->replica flag:'||toDelete.gotreplica);
                        delete productionoutputfiles WHERE production=toDelete.production AND eventtypeid=toDelete.eventtypeid AND filetypeid=toDelete.filetypeid AND stepid=toDelete.stepid and visible=toDelete.visibilityflag and gotreplica=toDelete.gotreplica;
                end loop;
            elsif nbrows < nbrowstobeprocessed then
                updateProtoPordoutput(c.production);
                FOR toInsert IN(select * from (SELECT j.production,J.STEPID, f.eventtypeid, f.filetypeid, f.gotreplica, f.visibilityflag FROM jobs j, files f WHERE
                    j.jobid = f.jobid AND
                    j.production= c.production and
                    f.gotreplica IS NOT NULL and
                    f.filetypeid NOT IN(9,17) GROUP BY j.production, J.STEPID, f.eventtypeid, f.filetypeid, f.gotreplica, f.visibilityflag Order by f.gotreplica,f.visibilityflag)  minus
                           select production, stepid, eventtypeid, filetypeid, gotreplica, visible as visibilityflag from productionoutputfiles where production=c.production) LOOP
                        dbms_output.put_line('Inserting -> Production:' || toInsert.production || '->step:' || toInsert.stepid || '->file type:' || toInsert.filetypeid || '->visible:'||toInsert.visibilityflag||'->event type:'||toInsert.eventtypeid||'->replica flag:'||toInsert.gotreplica);
                        INSERT INTO productionoutputfiles(production, stepid, filetypeid, visible, eventtypeid,gotreplica)VALUES(toInsert.production,toInsert.stepid, toInsert.filetypeid, toInsert.visibilityflag,toInsert.eventtypeid, toInsert.gotreplica);
              end loop;
            end if;
        else
            insertProtoPordoutput(c.production);
        end if;
        COMMIT;
    END LOOP;
--END LOOP;
    EXCEPTION
    WHEN OTHERS THEN
        err_num := SQLCODE;
        err_msg := SUBSTR(SQLERRM, 1, 1000);
        utl_mail.send(sender => 'lhcb-geoc@cern.ch',
                recipients => 'lhcb-bookkeeping@cern.ch',
                subject    => 'Failed to update productionoutputfiles',
                message    => 'ERROR number:'||err_num||' error message:'||err_msg||' More info: https://lhcb-dirac.readthedocs.io/en/latest/AdministratorGuide/Bookkeeping/administrate_oracle.html#automatic-updating-of-the-productionoutputfiles');
END;
END;
/
