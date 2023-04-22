#!/usr/bin/env bash

function jobqueue_before_install {
    docker version
    docker-compose version

    # build images and start flux cluster
    cd ./ci/flux
    cp ../environment.yml ./environment.yml 
    docker-compose build node-1
    docker-compose up -d
    cd -

    # Set shared space permissions (use sudo as owned by root and we are flux user)
    docker exec node-1 /bin/bash -c "chmod -R 777 /shared_space"

    docker ps -a
    docker images
    show_network_interfaces
}

function show_network_interfaces {
    for c in node-1 node-2 node-3; do
        echo '------------------------------------------------------------'
        echo docker container: $c
        docker exec $c python -c 'import psutil; print(psutil.net_if_addrs().keys())'
        echo '------------------------------------------------------------'
    done
}

function jobqueue_install {
    docker exec node-1 /bin/bash -c "cd /dask-jobqueue; pip install -e ."
}

function jobqueue_script {
    docker exec node-1 /bin/bash -c "cd; pytest /dask-jobqueue/dask_jobqueue --verbose -E flux -s"
}

function jobqueue_after_script {
    docker exec node-1 bash -c 'flux jobs -a'
    cd ./ci/flux
    docker-compose stop
    docker-compose rm --force
    cd -
}
