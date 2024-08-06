# SPDX-FileCopyrightText: Copyright (c) 2019 Giovanni Torres
# SPDX-License-Identifier: BSD 3-Clause License
# SPDX-URL: https://github.com/giovtorres/slurm-docker-cluster/blob/65e3a83098eae4ae6f0dcf656e814129d279b7c9/LICENSE

# This is a modified version of the original Dockerfile
# https://github.com/giovtorres/slurm-docker-cluster/blob/65e3a83098eae4ae6f0dcf656e814129d279b7c9/Dockerfile
# Previously this image used "FROM giovtorres/slurm-docker-cluster:latest"
# but while that project is still being maintained it is no longer being pushed to Docker Hub
# so we are vendoring it in here.


FROM rockylinux:8

LABEL org.opencontainers.image.source="https://github.com/giovtorres/slurm-docker-cluster" \
    org.opencontainers.image.title="slurm-docker-cluster" \
    org.opencontainers.image.description="Slurm Docker cluster on Rocky Linux 8" \
    org.label-schema.docker.cmd="docker-compose up -d" \
    maintainer="Giovanni Torres"

RUN set -ex \
    && yum makecache \
    && yum -y update \
    && yum -y install dnf-plugins-core \
    && yum config-manager --set-enabled powertools \
    && yum -y install \
    iproute \
    wget \
    bzip2 \
    perl \
    gcc \
    gcc-c++\
    git \
    gnupg \
    make \
    munge \
    munge-devel \
    python3-devel \
    python3-pip \
    python3 \
    mariadb-server \
    mariadb-devel \
    psmisc \
    bash-completion \
    vim-enhanced \
    http-parser-devel \
    json-c-devel \
    && yum clean all \
    && rm -rf /var/cache/yum

RUN alternatives --set python /usr/bin/python3

RUN pip3 install Cython nose

ARG GOSU_VERSION=1.11

RUN set -ex \
    && wget -O /usr/local/bin/gosu "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-amd64" \
    && wget -O /usr/local/bin/gosu.asc "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-amd64.asc" \
    && export GNUPGHOME="$(mktemp -d)" \
    && gpg --batch --keyserver hkps://keys.openpgp.org --recv-keys B42F6819007F00F88E364FD4036A9C25BF357DD4 \
    && gpg --batch --verify /usr/local/bin/gosu.asc /usr/local/bin/gosu \
    && rm -rf "${GNUPGHOME}" /usr/local/bin/gosu.asc \
    && chmod +x /usr/local/bin/gosu \
    && gosu nobody true

ARG SLURM_TAG=slurm-21-08-6-1

RUN set -x \
    && git clone -b ${SLURM_TAG} --single-branch --depth=1 https://github.com/SchedMD/slurm.git \
    && pushd slurm \
    && ./configure --enable-debug --prefix=/usr --sysconfdir=/etc/slurm \
    --with-mysql_config=/usr/bin  --libdir=/usr/lib64 \
    && make install \
    && install -D -m644 etc/cgroup.conf.example /etc/slurm/cgroup.conf.example \
    && install -D -m644 etc/slurm.conf.example /etc/slurm/slurm.conf.example \
    && install -D -m644 etc/slurmdbd.conf.example /etc/slurm/slurmdbd.conf.example \
    && install -D -m644 contribs/slurm_completion_help/slurm_completion.sh /etc/profile.d/slurm_completion.sh \
    && popd \
    && rm -rf slurm \
    && groupadd -r --gid=990 slurm \
    && useradd -r -g slurm --uid=990 slurm \
    && mkdir /etc/sysconfig/slurm \
    /var/spool/slurmd \
    /var/run/slurmd \
    /var/run/slurmdbd \
    /var/lib/slurmd \
    /var/log/slurm \
    /data \
    && touch /var/lib/slurmd/node_state \
    /var/lib/slurmd/front_end_state \
    /var/lib/slurmd/job_state \
    /var/lib/slurmd/resv_state \
    /var/lib/slurmd/trigger_state \
    /var/lib/slurmd/assoc_mgr_state \
    /var/lib/slurmd/assoc_usage \
    /var/lib/slurmd/qos_usage \
    /var/lib/slurmd/fed_mgr_state \
    && chown -R slurm:slurm /var/*/slurm* \
    && /sbin/create-munge-key

CMD ["slurmdbd"]

RUN curl -o miniconda.sh https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
    bash miniconda.sh -f -b -p /opt/anaconda && \
    /opt/anaconda/bin/conda clean -tipy && \
    rm -f miniconda.sh
ENV PATH /opt/anaconda/bin:$PATH
# environment.yml file is copied by CI script. If manually building, you should copy it too from parent directory
COPY environment.yml .
RUN conda env create --file environment.yml
SHELL ["conda", "run", "-n", "dask-jobqueue", "/bin/bash", "-c"]

COPY slurm.conf /etc/slurm/slurm.conf
COPY slurmdbd.conf /etc/slurm/slurmdbd.conf
RUN set -x \
    && chown slurm:slurm /etc/slurm/slurmdbd.conf \
    && chmod 600 /etc/slurm/slurmdbd.conf
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]

ENV LC_ALL en_US.UTF-8
