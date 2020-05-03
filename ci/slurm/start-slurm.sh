#!/bin/bash

slept_for=0
sleep_for=2
while [ "$(./register_cluster.sh 2>&1 | grep -c 'sacctmgr: error' -)" -ne 0 ]
  do
    sleep $sleep_for
    slept_for=$((slept_for + sleep_for))
    echo "Waited ${slept_for}s for SLURM cluster to become ready";
  done
echo "SLURM properly configured after ${slept_for}s"

# On some clusters the login node does not have the same interface as the
# compute nodes. The next three lines allow to test this edge case by adding
# separate interfaces on the worker and on the scheduler nodes.
set -x
docker exec slurmctld ip addr add 10.1.1.20/24 dev eth0 label eth0:scheduler
docker exec c1 ip addr add 10.1.1.21/24 dev eth0 label eth0:worker
docker exec c2 ip addr add 10.1.1.22/24 dev eth0 label eth0:worker
set +x
