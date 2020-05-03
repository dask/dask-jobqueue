#!/usr/bin/env bash

function jobqueue_before_install {
    set -e
    set -x

    # document docker versions
    docker version
    docker-compose version

    # compose pbs app
    cd ./ci/pbs
    ./docker-setup-pbs.sh
    cd -

    # document docker env
    docker exec -it -u pbsuser pbs_master pbsnodes -a
    docker ps -a
    docker images --digests

    # start pbs cluster
    cd ./ci/pbs
    ./start-pbs.sh
    cd -

    set +x
    set +e
}

function jobqueue_install {
    set -x
    docker exec -it pbs_master /bin/bash -c "cd /dask-jobqueue; pip install -e ."
    set +x
}

function jobqueue_script {
    set -x
    docker exec -it -u pbsuser pbs_master /bin/bash -c "cd; pytest /dask-jobqueue/dask_jobqueue --verbose -s -E pbs"
    set +x
}

function jobqueue_after_script {
    set -x
    docker exec -it -u pbsuser pbs_master qstat -fx
    docker exec -it pbs_master bash -c 'cat /var/spool/pbs/sched_logs/*'
    docker exec -it pbs_master bash -c 'cat /var/spool/pbs/server_logs/*'
    docker exec -it pbs_master bash -c 'cat /var/spool/pbs/server_priv/accounting/*'
    docker exec -it pbs_slave_1 bash -c 'cat /var/spool/pbs/mom_logs/*'
    docker exec -it pbs_slave_1 bash -c 'cat /var/spool/pbs/spool/*'
    docker exec -it pbs_slave_1 bash -c 'cat /tmp/*.e*'
    docker exec -it pbs_slave_1 bash -c 'cat /tmp/*.o*'
    docker exec -it pbs_slave_2 bash -c 'cat /var/spool/pbs/mom_logs/*'
    docker exec -it pbs_slave_2 bash -c 'cat /var/spool/pbs/spool/*'
    docker exec -it pbs_slave_2 bash -c 'cat /tmp/*.e*'
    docker exec -it pbs_slave_2 bash -c 'cat /tmp/*.o*'
    set +x
}
