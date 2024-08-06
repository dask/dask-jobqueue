#!/usr/bin/env bash

function jobqueue_before_install {
    docker version
    docker compose version

    # start sge cluster
    cd ./ci/sge
    docker compose build
    ./start-sge.sh
    cd -

    #Set shared space permissions
    docker exec sge_master /bin/bash -c "chmod -R 777 /shared_space"

    docker ps -a
    docker images
    docker exec sge_master qconf -sq dask.q
}

function jobqueue_install {
    docker exec sge_master conda run -n dask-jobqueue /bin/bash -c "cd /dask-jobqueue; pip install -e ."
}

function jobqueue_script {
    docker exec sge_master conda run -n dask-jobqueue /bin/bash -c "cd; pytest /dask-jobqueue/dask_jobqueue --verbose --full-trace -s -E sge"
}

function jobqueue_after_script {
    echo "Daemon logs"
    docker exec sge_master bash -c 'cat /tmp/sge*' || echo "No sge_master logs"
    docker exec slave_one bash -c 'cat /tmp/exec*' || echo "No slave_one logs"
    docker exec slave_two bash -c 'cat /tmp/exec*' || echo "No slave_two logs"
}
