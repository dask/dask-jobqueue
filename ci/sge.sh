#!/usr/bin/env bash

function jobqueue_before_install {
    set -e
    set -x

    # document docker versions
    docker version
    docker-compose version

    # compose sge app
    cd ./ci/sge
    ./docker-setup-sge.sh
    cd -

    # document docker env
    docker ps -a
    docker images --digests

    # start sge cluster
    cd ./ci/sge
    ./start-sge.sh
    cd -

    set +x
    set +e
}

function jobqueue_install {
    docker exec -it sge_master /bin/bash -c "cd /dask-jobqueue; pip install -e ."
}

function jobqueue_script {
    docker exec -it sge_master /bin/bash -c "cd /dask-jobqueue; pytest dask_jobqueue --verbose -s -E sge"
}

function jobqueue_after_script {
    docker exec -it sge_master bash -c 'cat /tmp/sge*'
    docker exec -it slave_one bash -c 'cat /tmp/exec*'
    docker exec -it slave_two bash -c 'cat /tmp/exec*'
}
