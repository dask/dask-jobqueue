FROM rockylinux:8
LABEL description="openpbs and conda"

#The pbs master node name, can be overridden if needed
ENV PBS_MASTER pbs_master
ENV PATH /opt/pbs/bin:/opt/anaconda/bin:$PATH
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8

# install openpbs and useful packages
RUN yum install -y unzip bzip2 git gcc sudo openssh-server && yum clean all
RUN curl -o openpbs.zip https://vcdn.altair.com/rl/OpenPBS/openpbs_23.06.06.rockylinux_8.8.zip && \
    unzip openpbs.zip && \
    mv openpbs_23.06.06.rockylinux_8.8/openpbs-*.rpm . && \
    rm -rf openpbs.zip openpbs_23.06.06.rockylinux_8.8
RUN yum install -y openpbs-debuginfo-*.rpm openpbs-server-*.rpm && yum clean all

# install python
RUN curl -o miniconda.sh https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
    bash miniconda.sh -f -b -p /opt/anaconda && \
    /opt/anaconda/bin/conda clean -tipy && \
    rm -f miniconda.sh

# environment.yml file is copied by CI script. If manually building, you should copy it too from parent directory
COPY environment.yml .
RUN conda env create --file environment.yml
SHELL ["conda", "run", "-n", "dask-jobqueue", "/bin/bash", "-c"]

# Copy entrypoint and other needed scripts
COPY ./*.sh /
RUN chmod a+x ./*.sh

# default entrypoint launch pbs master
ENTRYPOINT ["bash", "/master-entrypoint.sh"]
