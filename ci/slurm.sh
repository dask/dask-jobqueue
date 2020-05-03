#!/usr/bin/env bash

function jobqueue_before_install {
    docker version
    docker-compose version

    # compose slurm app
    cd ./ci/slurm || return 1
    ./docker-setup-slurm.sh
    cd - || return 1

    # document docker env
    docker ps -a
    docker images

    # start slurm
    cd ./ci/slurm || return 1
    ./start-slurm.sh
    cd - || return 1

    # show network setup (only possible after slurm is up)
    show_network_interfaces
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
    docker exec -it slurmctld /bin/bash -c "cd /dask-jobqueue; pip install -e ."
}

function jobqueue_script {
    docker exec -it slurmctld /bin/bash -c "pytest /dask-jobqueue/dask_jobqueue --verbose -E slurm -s"
}

function jobqueue_after_script {
    docker exec -it slurmctld bash -c 'sinfo'
    docker exec -it slurmctld bash -c 'squeue'
    docker exec -it slurmctld bash -c 'sacct -l'
}
