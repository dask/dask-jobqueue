#!/usr/bin/env bash

function jobqueue_before_install {
    docker version
    docker-compose version

    # start htcondor cluster
    cd ./ci/htcondor
    docker-compose pull
    docker-compose build
    ./start-htcondor.sh
    docker-compose exec -T submit /bin/bash -c "condor_status"
    docker-compose exec -T submit /bin/bash -c "condor_q"
    cd -

    docker ps -a
    docker images
}

function jobqueue_install {
    cd ./ci/htcondor
    docker-compose exec -T submit /bin/bash -c "cd /dask-jobqueue; pip3 install -e .; pip3 install --upgrade 'click<8'; chown -R submituser ."
    cd -
}

function jobqueue_script {
    cd ./ci/htcondor
    docker-compose exec -T --user submituser submit /bin/bash -c "cd; pytest /dask-jobqueue/dask_jobqueue --log-cli-level DEBUG --capture=tee-sys --verbose -E htcondor "
    cd -
}

function jobqueue_after_script {
    cd ./ci/htcondor
    docker-compose exec -T submit /bin/bash -c "condor_q"
    docker-compose exec -T submit /bin/bash -c "condor_status"
    docker-compose exec -T submit /bin/bash -c "condor_history"
    docker-compose exec -T cm /bin/bash -c " grep -R \"\" /var/log/condor/	"
    cd -
}
