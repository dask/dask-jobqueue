#!/usr/bin/env bash

set -x

function jobqueue_before_install {
    docker version
    docker-compose version

    # start a container
    docker build -t dask-jobqueue:latest basic/
    docker run -d --name test_server -v `pwd`../..:/dask-jobqueue dask-jobqueue:latest sleep 600

    docker ps -a
    docker images
}

function jobqueue_install {
    docker exec -it test_server /bin/bash -c "cd /dask-jobqueue; python setup.py install"
}

function jobqueue_script {
    docker exec -it test_server /bin/bash -c "cd /dask-jobqueue; py.test dask_jobqueue --verbose"
}

function jobqueue_after_success {

}
