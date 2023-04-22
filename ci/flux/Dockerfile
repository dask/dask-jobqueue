FROM fluxrm/flux-sched:el8
ARG replicas=3
ENV workers=${replicas}
USER root

# These are the STATE_DIR, system, and resources directories
ENV STATE_DIR=/var/lib/flux
ENV LC_ALL en_US.UTF-8
RUN mkdir -p ${STATE_DIR} /etc/flux/system /etc/flux/system/cron.d /etc/flux/config /run/flux && \
    mkdir -p /etc/flux/system/cron.d && \
    mkdir -p /mnt/curve && \
    flux keygen /mnt/curve/curve.cert && \
    # Important: "basic" is the directory name here
    flux R encode --hosts="node-[1-${workers}]" > /etc/flux/system/R
    
WORKDIR /home/fluxuser
RUN pip3 install --upgrade pip && \
    pip3 install pika --upgrade

# bind-utils provides nslookup
RUN yum install -y iproute bind-utils

# Use mamba for slightly faster install
RUN /bin/bash -c "curl -L https://github.com/conda-forge/miniforge/releases/latest/download/Mambaforge-Linux-x86_64.sh > mambaforge.sh && \
    bash mambaforge.sh -b -p /opt/anaconda && \
    rm mambaforge.sh" && \
    export PATH=/opt/conda/bin:$PATH  && \
    /opt/anaconda/bin/conda clean -tipy
ENV PATH /opt/anaconda/bin:$PATH

# environment.yml file is copied by CI script. If manually building, you should copy it too from parent directory
COPY environment.yml .
RUN mamba env update -n base --file environment.yml

# Important! In production flux should not be run as root
# USER fluxuser
WORKDIR /home/fluxuser/
COPY ./conf/entrypoint.sh ./
ENTRYPOINT /bin/bash /home/fluxuser/entrypoint.sh
