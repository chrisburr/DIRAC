# This dockerfile requires `--build-arg LHCB_DIRAC_VERSION=xyz` to be build

# https://cloud.google.com/solutions/best-practices-for-building-containers
# https://docs.docker.com/develop/develop-images/dockerfile_best-practices/

FROM centos:7
LABEL MAINTAINER Christophe HAEN <christophe.haen@cern.ch>

# Create a local dirac user,
# the directory structure
# and add mininimal dependencies
RUN groupadd -r dirac \
    && useradd --no-log-init -r -g dirac dirac \
    && mkdir -p /opt/dirac/etc \
    && chown -R dirac:dirac /opt/dirac \
    && yum install -y git tar \
    && yum clean all

# Run as dirac
USER dirac

# Copy the self pinging and entry point script
COPY --chown=dirac:dirac dockerEntrypoint.sh dirac_self_ping.py /opt/dirac/

WORKDIR /opt/dirac

EXPOSE 9100-9199

ENTRYPOINT [ "/opt/dirac/dockerEntrypoint.sh" ]

# Specify the version from the build command line
ARG LHCB_DIRAC_VERSION

RUN \
    cd /tmp && \
    curl -LO https://github.com/DIRACGrid/DIRACOS2/releases/latest/download/DIRACOS-Linux-x86_64.sh && \
    bash DIRACOS-Linux-x86_64.sh -u -p /opt/dirac && \
    source /opt/dirac/diracosrc && \
    pip install "LHCbDIRAC==${LHCB_DIRAC_VERSION}" && \
    rm -rf DIRACOS-Linux-x86_64.sh
