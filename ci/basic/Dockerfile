FROM ubuntu:14.04

ENV LANG C.UTF-8

RUN apt-get update && apt-get install curl bzip2 git gcc -y --fix-missing

RUN curl -o miniconda.sh https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
    bash miniconda.sh -f -b -p /opt/anaconda && \
    /opt/anaconda/bin/conda clean -tipy && \
    rm -f miniconda.sh
ENV PATH /opt/anaconda/bin:$PATH
RUN conda install -n root conda=4.4.11 && conda clean -tipy
RUN conda install -c conda-forge dask distributed blas pytest mock ipython pip psutil && conda clean -tipy

