#==============================================================================
# Build a sif file for the project
#==============================================================================

#FROM ghcr.io/dask/dask
FROM ubuntu:22.04
ENV DEBIAN_FRONTEND=noninteractive
ENV APPNAME=imagery_composite

# install system dependencies
RUN apt-get update
RUN apt-get -qq install build-essential && \
    apt-get -qq install python3-dev     && \
    apt-get install -y slurm-wlm=21.08.5-2ubuntu1      && \
    apt-get install -y munge=0.5.14-6     && \
    apt-get -qq install python3-venv


#FROM ghcr.io/dask/dask
# create venv to isolate the installed python code from the system
WORKDIR /project/$APPNAME
RUN python3 -m venv ./venv && ./venv/bin/pip install --upgrade pip

ENV PATH="/project/$APPNAME/venv/bin:$PATH"
ENV PATH="/project/$APPNAME/venv/lib:$PATH"

# get the requirements.txt
COPY requirements.txt /project/$APPNAME-requirements.txt
# install dependencies
RUN pip3 install --no-cache-dir --upgrade -r /project/$APPNAME-requirements.txt

# perform cleanup
RUN py3clean /project/$APPNAME
RUN pip uninstall -y pip

COPY . /project/$APPNAME/
