FROM ubuntu:21.04

# Set debian to non interactive to avoid prompts
ENV DEBIAN_FRONTEND=noninteractive
# Setting UTF-8 as default lang
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

## Install python.
RUN apt-get update && apt-get install -y \
    # `curl` for downloading miniconda
    curl \
    # `gosu` for unpriviledged use
    gosu \
    # Slurm (slurmctl & workers)
    slurm-wlm \
    # Slurm (slurmdbd) Database
    slurmdbd \
    mariadb-server \
    # Slurm (slurmrestd) REST API
    slurmrestd \
    && \
    apt autoremove -y && \
    apt clean && \
    rm -rf /var/lib/apt/lists/*

# create dir for slurmrestd socket
# Add JWT key to controller in StateSaveLocation
RUN mkdir /var/run/slurm /var/lib/slurmd && \
    dd if=/dev/random of=/var/lib/slurmd/jwt_hs256.key bs=32 count=1 && \
    chown slurm:slurm /var/lib/slurmd/jwt_hs256.key && \
    chmod 0600 /var/lib/slurmd/jwt_hs256.key && \
    chown -R slurm:slurm /var/*/slurm*

RUN mkdir /run/munge && chown -R munge:munge /run/munge

RUN curl -o miniconda.sh https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
    bash miniconda.sh -f -b -p /opt/anaconda && \
    /opt/anaconda/bin/conda clean -tipy && \
    rm -f miniconda.sh
ENV PATH /opt/anaconda/bin:$PATH
RUN conda install --yes -c conda-forge python=3.6 dask distributed flake8 pytest pytest-asyncio

COPY --chown=slurm:slurm slurm.conf /etc/slurm/slurm.conf
COPY --chown=slurm:slurm slurmdbd.conf /etc/slurm/slurmdbd.conf
COPY --chown=slurm:slurm slurmrestd.conf /etc/slurm/slurmrestd.conf

COPY entrypoint.sh /usr/local/bin/entrypoint.sh
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
