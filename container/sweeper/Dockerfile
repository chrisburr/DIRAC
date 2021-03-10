FROM python:3.7-slim
LABEL MAINTAINER Marko Petric <marko.petric@cern.ch>

RUN apt-get update && \
    apt-get install -y git wget && \
    pip install requests python-gitlab pyyaml && \
    wget https://gitlab.cern.ch/sft/lcgcmake/-/raw/master/Sweep/sweep_MR.py?inline=false -O sweep_MR.py

