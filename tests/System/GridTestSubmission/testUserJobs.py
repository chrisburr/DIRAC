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
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

import os.path
import tempfile
import time

from DIRAC import gLogger

from DIRAC.tests.Utilities.utils import find_all

from LHCbDIRAC import rootPath

from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers
try:
  from LHCbDIRAC.tests.Workflow.Integration.Test_UserJobs import createJob
except ImportError:
  from tests.Workflow.Integration.Test_UserJobs import createJob

gLogger.setLevel('DEBUG')

cwd = os.path.realpath('.')


def pad_test_file(in_name, out_fn):
  test_fn = find_all(in_name, rootPath, '/tests/System/GridTestSubmission')[0]
  with open(test_fn, 'rt') as fp:
    test_data = fp.read()
  test_data += str(time.time())
  with open(out_fn, 'wt') as fp:
    fp.write(test_data)


########################################################################################

gLogger.info("\n Submitting hello world job banning T1s")

helloJ = LHCbJob()
dirac = DiracLHCb()

helloJ.setName("helloWorld-test-T2s")
helloJ.setInputSandbox([find_all('exe-script.py', rootPath, '/tests/System/GridTestSubmission')[0]])

helloJ.setExecutable("exe-script.py", "", "helloWorld.log")

helloJ.setCPUTime(17800)
try:
  tier1s = DMSHelpers().getTiers(tier=(0, 1))
except AttributeError:
  tier1s = ['LCG.CERN.cern', 'LCG.CNAF.it', 'LCG.GRIDKA.de', 'LCG.IN2P3.fr',
            'LCG.NIKHEF.nl', 'LCG.PIC.es', 'LCG.RAL.uk', 'LCG.RRCKI.ru', 'LCG.SARA.nl']
cernSite = [s for s in tier1s if '.CERN.' in s][0]
helloJ.setBannedSites(tier1s)
result = dirac.submitJob(helloJ)
gLogger.info("Hello world job: ", result)

########################################################################################

gLogger.info("\n Submitting hello world job targeting %s" % cernSite)

helloJ = LHCbJob()
dirac = DiracLHCb()

helloJ.setName("helloWorld-test-CERN")
helloJ.setInputSandbox([find_all('exe-script.py', rootPath, '/tests/System/GridTestSubmission')[0]])
helloJ.setExecutable("exe-script.py", "", "helloWorld.log")

helloJ.setCPUTime(17800)
helloJ.setDestination(cernSite)
result = dirac.submitJob(helloJ)
gLogger.info("Hello world job: ", result)

########################################################################################

gLogger.info("\n Submitting hello world job targeting centos7 machines")

helloJ = LHCbJob()
dirac = DiracLHCb()

helloJ.setName("helloWorld-test-centos7")
helloJ.setInputSandbox([find_all('exe-script.py', rootPath, '/tests/System/GridTestSubmission')[0]])
helloJ.setExecutable("exe-script.py", "", "helloWorld.log")

helloJ.setCPUTime(17800)
helloJ.setPlatform('x86_64-centos7')
result = dirac.submitJob(helloJ)
gLogger.info("Hello world job: ", result)

########################################################################################

gLogger.info("\n Submitting hello world job targeting slc6 machines")

helloJ = LHCbJob()
dirac = DiracLHCb()

helloJ.setName("helloWorld-test-slc6")
helloJ.setInputSandbox([find_all('exe-script.py', rootPath, '/tests/System/GridTestSubmission')[0]])
helloJ.setExecutable("exe-script.py", "", "helloWorld.log")

helloJ.setCPUTime(17800)
helloJ.setPlatform('x86_64-slc6')
result = dirac.submitJob(helloJ)
gLogger.info("Hello world job: ", result)

########################################################################################

gLogger.info("\n Submitting a job that uploads an output")

helloJ = LHCbJob()
dirac = DiracLHCb()

with tempfile.NamedTemporaryFile() as tmp_file:
  pad_test_file('testFileUpload.txt', tmp_file.name)

  helloJ.setName("upload-Output-test")
  helloJ.setInputSandbox([
      find_all('exe-script.py', rootPath, '/tests/System/GridTestSubmission')[0],
      tmp_file.name,
  ])
  helloJ.setExecutable("exe-script.py", "", "helloWorld.log")

  helloJ.setCPUTime(17800)

  helloJ.setOutputData([os.path.basename(tmp_file.name)])

  result = dirac.submitJob(helloJ)
gLogger.info("Hello world with output: ", result)

########################################################################################

gLogger.info("\n Submitting a job that uploads an output and replicates it")

helloJ = LHCbJob()
dirac = DiracLHCb()

with tempfile.NamedTemporaryFile() as tmp_file:
  pad_test_file('testFileReplication.txt', tmp_file.name)

  helloJ.setName("upload-Output-test-with-replication")
  helloJ.setInputSandbox([
      find_all('exe-script.py', rootPath, '/tests/System/GridTestSubmission')[0],
      tmp_file.name,
  ])
  helloJ.setExecutable("exe-script.py", "", "helloWorld.log")

  helloJ.setCPUTime(17800)

  helloJ.setOutputData([os.path.basename(tmp_file.name)], replicate='True')

  result = dirac.submitJob(helloJ)
gLogger.info("Hello world with output and replication: ", result)

########################################################################################

gLogger.info("\n Submitting gaudiRun job (Gauss only)")

gaudirunJob = LHCbJob()

gaudirunJob.setName("gaudirun-Gauss-test")
gaudirunJob.setInputSandbox([find_all('prodConf_Gauss_00012345_00067890_1.py', rootPath,
                                      '/tests/System/GridTestSubmission')[0]])
gaudirunJob.setOutputSandbox('00012345_00067890_1.sim')

optGauss = "$APPCONFIGOPTS/Gauss/Sim08-Beam3500GeV-md100-2011-nu2.py;"
optDec = "$DECFILESROOT/options/34112104.py;"
optPythia = "$LBPYTHIAROOT/options/Pythia.py;"
optOpts = "$APPCONFIGOPTS/Gauss/G4PL_FTFP_BERT_EmNoCuts.py;"
optCompr = "$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py;"
optPConf = "prodConf_Gauss_00012345_00067890_1.py"
options = optGauss + optDec + optPythia + optOpts + optCompr + optPConf
# gaudirunJob.addPackage('AppConfig', 'v3r179')
# gaudirunJob.addPackage('Gen/DecFiles', 'v27r14p1')
# gaudirunJob.addPackage('ProdConf', 'v1r9')
gaudirunJob.setApplication('Gauss', 'v45r5', options,
                           extraPackages='AppConfig.v3r179;Gen/DecFiles.v27r14p1;ProdConf.v1r9',
                           systemConfig='x86_64-slc5-gcc43-opt')

gaudirunJob.setDIRACPlatform()
gaudirunJob.setCPUTime(172800)

result = dirac.submitJob(gaudirunJob)
gLogger.info('Submission Result: ', result)

########################################################################################

gLogger.info("\n Submitting gaudiRun job (Gauss only) that should use TAG to run on a multi-core queue")

gaudirunJob = LHCbJob()

gaudirunJob.setName("gaudirun-Gauss-test-TAG-multicore")
gaudirunJob.setInputSandbox([find_all('prodConf_Gauss_00012345_00067890_1.py', rootPath,
                                      '/tests/System/GridTestSubmission')[0]])
gaudirunJob.setOutputSandbox('00012345_00067890_1.sim')

optGauss = "$APPCONFIGOPTS/Gauss/Sim08-Beam3500GeV-md100-2011-nu2.py;"
optDec = "$DECFILESROOT/options/34112104.py;"
optPythia = "$LBPYTHIAROOT/options/Pythia.py;"
optOpts = "$APPCONFIGOPTS/Gauss/G4PL_FTFP_BERT_EmNoCuts.py;"
optCompr = "$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py;"
optPConf = "prodConf_Gauss_00012345_00067890_1.py"
options = optGauss + optDec + optPythia + optOpts + optCompr + optPConf
# gaudirunJob.addPackage('AppConfig', 'v3r179')
# gaudirunJob.addPackage('Gen/DecFiles', 'v27r14p1')
# gaudirunJob.addPackage('ProdConf', 'v1r9')
gaudirunJob.setApplication('Gauss', 'v45r5', options, extraPackages='AppConfig.v3r179;DecFiles.v27r14p1;ProdConf.v1r9',
                           systemConfig='x86_64-slc5-gcc43-opt')

gaudirunJob.setDIRACPlatform()
gaudirunJob.setCPUTime(172800)
gaudirunJob.setTag(['MultiProcessor'])

result = dirac.submitJob(gaudirunJob)
gLogger.info('Submission Result: ', result)

########################################################################################

gLogger.info("\n Submitting gaudiRun job (Gauss only) that should use 2 to 4 processors")

gaudirunJob = LHCbJob()

gaudirunJob.setName("gaudirun-Gauss-test-multicore-2to4")
gaudirunJob.setInputSandbox([find_all('prodConf_Gauss_00012345_00067890_1.py', rootPath,
                                      '/tests/System/GridTestSubmission')[0]])
gaudirunJob.setOutputSandbox('00012345_00067890_1.sim')

optGauss = "$APPCONFIGOPTS/Gauss/Sim08-Beam3500GeV-md100-2011-nu2.py;"
optDec = "$DECFILESROOT/options/34112104.py;"
optPythia = "$LBPYTHIAROOT/options/Pythia.py;"
optOpts = "$APPCONFIGOPTS/Gauss/G4PL_FTFP_BERT_EmNoCuts.py;"
optCompr = "$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py;"
optPConf = "prodConf_Gauss_00012345_00067890_1.py"
options = optGauss + optDec + optPythia + optOpts + optCompr + optPConf
# gaudirunJob.addPackage('AppConfig', 'v3r179')
# gaudirunJob.addPackage('Gen/DecFiles', 'v27r14p1')
# gaudirunJob.addPackage('ProdConf', 'v1r9')
gaudirunJob.setApplication('Gauss', 'v45r5', options, extraPackages='AppConfig.v3r179;DecFiles.v27r14p1;ProdConf.v1r9',
                           systemConfig='x86_64-slc5-gcc43-opt')

gaudirunJob.setDIRACPlatform()
gaudirunJob.setCPUTime(172800)
gaudirunJob.setNumberOfProcessors(minNumberOfProcessors=2, maxNumberOfProcessors=4)

result = dirac.submitJob(gaudirunJob)
gLogger.info('Submission Result: ', result)

########################################################################################

gLogger.info("\n Submitting gaudiRun job (Gauss only) that should use 8 processors")

gaudirunJob = LHCbJob()

gaudirunJob.setName("gaudirun-Gauss-test-multicore-8")
gaudirunJob.setInputSandbox([find_all('prodConf_Gauss_00012345_00067899_1.py', rootPath,
                                      '/tests/System/GridTestSubmission')[0]])
gaudirunJob.setOutputSandbox('00012345_00067899_1.sim')

# lb-run --unset LD_LIBRARY_PATH --unset PYTHONPATH --unset XrdSecPROTOCOL
# --siteroot=/cvmfs/lhcb.cern.ch/lib/ --allow-containers -c x86_64-centos7-gcc9-opt
# --use="AppConfig v3r400"  --use="Gen/DecFiles v30r42"  --use="ProdConf"
# Gauss/v54r3 gaudirun.py -T --ncpus 8
# $APPCONFIGOPTS/Gauss/Beam7000GeV-mu100-nu7.6-HorExtAngle.py
# $APPCONFIGOPTS/Gauss/EnableSpillover-25ns.py
# $DECFILESROOT/options/12143001.py
# $LBPYTHIA8ROOT/options/Pythia8.py
# $APPCONFIGOPTS/Gauss/Gauss-Upgrade-Baseline-20150522.py
# $APPCONFIGOPTS/Gauss/G4PL_FTFP_BERT_EmOpt2.py
# $APPCONFIGOPTS/Gauss/GaussMPpatch20200701.py
# $APPCONFIGOPTS/Persistency/Compression-LZMA-4.py
# prodConf_Gauss_00111263_00000023_1.py

options = "$APPCONFIGOPTS/Gauss/Beam7000GeV-mu100-nu7.6-HorExtAngle.py;"
options += "$APPCONFIGOPTS/Gauss/EnableSpillover-25ns.py;"
options += "$DECFILESROOT/options/12143001.py;"
options += "$LBPYTHIA8ROOT/options/Pythia8.py;"
options += "$APPCONFIGOPTS/Gauss/Gauss-Upgrade-Baseline-20150522.py;"
options += "$APPCONFIGOPTS/Gauss/G4PL_FTFP_BERT_EmOpt2.py;"
options += "$APPCONFIGOPTS/Gauss/GaussMPpatch20200701.py;"
options += "$APPCONFIGOPTS/Persistency/Compression-LZMA-4.py"


# gaudirunJob.addPackage('AppConfig', 'v3r179')
# gaudirunJob.addPackage('Gen/DecFiles', 'v27r14p1')
# gaudirunJob.addPackage('ProdConf', 'v1r9')
gaudirunJob.setApplication('Gauss', 'v54r3', options,
                           extraPackages='AppConfig.v3r400;Gen/DecFiles.v30r42;ProdConf.v3r0',
                           systemConfig='x86_64-centos7-gcc9-opt')

gaudirunJob.setDIRACPlatform()
gaudirunJob.setCPUTime(172800)
gaudirunJob.setNumberOfProcessors(numberOfProcessors=8)  # exact number

result = dirac.submitJob(gaudirunJob)
gLogger.info('Submission Result: ', result)

########################################################################################

gLogger.info("\n Submitting gaudiRun job (Boole only)")

gaudirunJob = LHCbJob()

gaudirunJob.setName("gaudirun-Boole-test")
gaudirunJob.setInputSandbox([find_all('prodConf_Boole_00012345_00067890_1.py', rootPath,
                                      '/tests/System/GridTestSubmission')[0]])
gaudirunJob.setOutputSandbox('00012345_00067890_1.digi')

opts = "$APPCONFIGOPTS/Boole/Default.py;"
optDT = "$APPCONFIGOPTS/Boole/DataType-2011.py;"
optTCK = "$APPCONFIGOPTS/Boole/Boole-SiG4EnergyDeposit.py;"
optComp = "$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py;"
optPConf = "prodConf_Boole_00012345_00067890_1.py"
options = opts + optDT + optTCK + optComp + optPConf

# gaudirunJob.addPackage( 'AppConfig', 'v3r171' )
gaudirunJob.setApplication('Boole', 'v26r3', options,
                           inputData='/lhcb/user/f/fstagni/test/12345/12345678/00012345_00067890_1.sim',
                           extraPackages='AppConfig.v3r171;ProdConf.v1r9',
                           systemConfig='x86_64-slc5-gcc43-opt')

gaudirunJob.setDIRACPlatform()
gaudirunJob.setCPUTime(172800)

result = dirac.submitJob(gaudirunJob)
gLogger.info('Submission Result: ', result)

########################################################################################

gLogger.info("\n Submitting gaudiRun job (Gauss only) that will use a configuration file that contains wrong info")
gLogger.info("This will generate a job that should become Completed, use the failover, and only later it will be Done")

gaudirunJob = createJob()
result = dirac.submitJob(gaudirunJob)
gLogger.info('Submission Result: ', result)
