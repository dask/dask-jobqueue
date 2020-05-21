# base image
ARG  HTCONDOR_VERSION
FROM htcondor/mini:${HTCONDOR_VERSION}${HTCONDOR_VERSION:+-}el7
LABEL description="HTCondor Minicondor Image with dask"

RUN yum install -y git python3-psutil 
RUN pip3 install pytest dask distributed


