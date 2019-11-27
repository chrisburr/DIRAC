# This dockerfile creates a container of a LHCbDIRAC client installation.
# Based on CC7
#
# The purpose of this container can be:
# 1. for running a client, as simple as that (no cvmfs)
# 2. for debugging (interacting with LHCb production or certification environment)
#     for this second case you can keep the DIRAC code on your local machine,
#     and use only the DIRACOS that sits on the container
#


# CC7 as base
FROM cern/cc7-base
MAINTAINER Federico Stagni <federico.stagni@cern.ch>

# Specify the version and setup
ENV LHCB_DIRAC_VERSION v10r0-pre3
ENV LHCB_DIRAC_SETUP LHCb-Certification
ENV LHCB_DIRAC_CS dips://lhcb-cert-conf-dirac.cern.ch:9135/Configuration/Server

# Add the EGI trustanchors and CAs
ADD http://repository.egi.eu/sw/production/cas/1/current/repo-files/EGI-trustanchors.repo /etc/yum.repos.d/EGI-trustanchors.repo
RUN yum install -y ca-policy-egi-core

# install packages
RUN yum install -y tar which git && yum clean all

# Copying in ENTRYPOINT script
COPY ./dockerEntrypoint.sh /
RUN chmod 755 /dockerEntrypoint.sh
ENTRYPOINT [ "/dockerEntrypoint.sh" ]

# Create DIRAC dirs
RUN mkdir -p /opt/dirac && \
    mkdir -p /opt/dirac/etc/grid-security

WORKDIR /opt/dirac

# Creating dirac user
RUN useradd -ms /bin/bash dirac && \
    mkdir -p /home/dirac/.globus && \
    chown -R dirac:dirac /opt/dirac && \
    chown -R dirac:dirac /home/dirac

# Installing LHCbDIRAC in /opt/dirac
USER dirac

RUN \
    curl -L -o dirac-install https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/Core/scripts/dirac-install.py && \
    chmod +x dirac-install && \
    ./dirac-install -r $LHCB_DIRAC_VERSION -l LHCb -e LHCb -t client && \
    rm -rf /opt/dirac/.installCache && \
    rm dirac-install && \
    ln -s /etc/grid-security/certificates/ /opt/dirac/etc/grid-security/certificates

# This is necessary, as below we are sourcing
SHELL ["/bin/bash", "-c"]

# there's no proxy, but anyway the dirac.cfg file will be created
RUN \
    source bashrc && \
    dirac-configure \
    -S $LHCB_DIRAC_SETUP \
    -C $LHCB_DIRAC_CS \
    -o "/Registry/VO/lhcb/VOMSName=lhcb" \
    -o "/Registry/VO/lhcb/VOMSServers/voms2.cern.ch/DN=/DC=ch/DC=cern/OU=computers/CN=voms2.cern.ch" \
    -o "/Registry/VO/lhcb/VOMSServers/voms2.cern.ch/CA=/DC=ch/DC=cern/CN=CERN Grid Certification Authority" \
    -o "/Registry/VO/lhcb/VOMSServers/voms2.cern.ch/Port=15003" \
    -o "/Registry/VO/lhcb/VOMSServers/lcg-voms2.cern.ch/DN=/DC=ch/DC=cern/OU=computers/CN=lcg-voms2.cern.ch" \
    -o "/Registry/VO/lhcb/VOMSServers/lcg-voms2.cern.ch/CA=/DC=ch/DC=cern/CN=CERN Grid Certification Authority" \
    -o "/Registry/VO/lhcb/VOMSServers/lcg-voms2.cern.ch/Port=15003" \
    -FDH
