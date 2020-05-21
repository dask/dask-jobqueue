#!/usr/bin/env bash

function jobqueue_before_install {
    docker version

    docker run --rm -d --name jobqueue-htcondor-mini tillkit/dask-jobqueue-ci:htcondor #TODO: changeme on final

    docker ps -a
    docker images
}
[[ "${BASH_SOURCE[0]}" != "${0}" ]] || jobqueue_before_install # excute if called as script

function jobqueue_install {
    docker cp . jobqueue-htcondor-mini:/dask-jobqueue 

    docker exec --user root jobqueue-htcondor-mini /bin/bash -c "
	cd /dask-jobqueue; 
	pip3 install --upgrade --upgrade-strategy eager -e .;
	chown -R submituser:submituser /dask-jobqueue 
	pip3 freeze
    "
}
[[ "${BASH_SOURCE[0]}" != "${0}" ]] || jobqueue_install # excute if called as script

function jobqueue_script {
    docker exec --user=submituser jobqueue-htcondor-mini /bin/bash -c "
    	cd /dask-jobqueue; 
	pytest dask_jobqueue --verbose -s -E htcondor"
}
[[ "${BASH_SOURCE[0]}" != "${0}" ]] || jobqueue_script # excute if called as script

function jobqueue_after_script {
    docker exec --user root jobqueue-htcondor-mini /bin/bash -c "
    	grep -R \"\" /var/log/condor/	
   "
}
[[ "${BASH_SOURCE[0]}" != "${0}" ]] || jobqueue_after_script # excute if called as script

function jobqueue_cleanup {
    docker stop jobqueue-htcondor-mini
}
[[ "${BASH_SOURCE[0]}" != "${0}" ]] || jobqueue_cleanup # excute if called as script
