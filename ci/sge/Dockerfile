FROM ubuntu:14.04 as base

ENV LANG C.UTF-8

RUN apt-get update && apt-get install curl bzip2 git gcc -y --fix-missing
RUN curl -ko miniconda.sh https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
    bash miniconda.sh -f -b -p /opt/anaconda && \
    /opt/anaconda/bin/conda clean -tipy && \
    rm -f miniconda.sh
ENV PATH /opt/anaconda/bin:$PATH
# environment.yml file is copied by CI script. If manually building, you should copy it too from parent directory
COPY environment.yml .
RUN conda env create --file environment.yml
SHELL ["conda", "run", "-n", "dask-jobqueue", "/bin/bash", "-c"]

COPY ./*.sh /
COPY ./*.txt /

FROM base as slave
RUN bash ./setup-slave.sh

FROM base as master
RUN bash ./setup-master.sh

# expose ports
EXPOSE 8000
EXPOSE 6444
EXPOSE 6445
EXPOSE 6446

ENV SGE_ROOT /var/lib/gridengine/
ENV SGE_CELL default
