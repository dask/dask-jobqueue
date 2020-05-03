#!/usr/bin/env bash

function jobqueue_before_install {
    set -e
    set -x

    # document docker versions
    docker version
    docker-compose version

    # compose slurm app
    cd ./ci/slurm
    ./docker-setup-slurm.sh
    cd -

    # document docker env
    docker ps -a
    docker images --digests

    # start slurm
    cd ./ci/slurm
    ./start-slurm.sh
    cd -

    # show network setup (only possible after slurm is up)
    show_network_interfaces

    set +x
    set +e
}

function show_network_interfaces {
    for c in slurmctld c1 c2; do
        echo '------------------------------------------------------------'
        echo docker container: $c
        docker exec -it $c python -c 'import psutil; print(psutil.net_if_addrs().keys())'
        echo '------------------------------------------------------------'
    done
}

function jobqueue_install {
    set -x
    docker exec -it slurmctld /bin/bash -c "cd /dask-jobqueue; pip install -e ."
    set +x
}

function jobqueue_script {
    set -x
    docker exec -it slurmctld /bin/bash -c "pytest /dask-jobqueue/dask_jobqueue --verbose -E slurm -s"
    set +x
}

function jobqueue_after_script {
    set -x
    docker exec -it slurmctld bash -c 'sinfo'
    docker exec -it slurmctld bash -c 'squeue'
    docker exec -it slurmctld bash -c 'sacct -l'
    set +x
}
