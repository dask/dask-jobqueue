#!/bin/bash

slept_for=0
sleep_for=2
while [ "$(docker exec -it -u pbsuser pbs_master pbsnodes -a | grep -c 'Mom = pbs_slave')" -ne 2 ]
do
    echo "Waited ${slept_for}s for PBS slave nodes to become available";
    sleep $sleep_for
    slept_for=$((slept_for + sleep_for))
done
echo "PBS properly configured after ${slept_for}s"
