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
    curl -L -o dirac-install https://raw.githubusercontent.com/DIRACGrid/management/master/dirac-install.py \
    && chmod +x dirac-install \
    && ./dirac-install -r ${LHCB_DIRAC_VERSION} -l LHCb -e LHCb -t server -i 27 \
    && rm -rf /opt/dirac/.installCache
