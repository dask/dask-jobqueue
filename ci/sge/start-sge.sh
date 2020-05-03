#!/bin/bash

slept_for=0
sleep_for=2
while [ "$(docker exec -it sge_master qhost | grep -c 'lx26-amd64')" -ne 2 ]
  do
    echo "Waited ${slept_for}s for SGE slots to become available";
    sleep $sleep_for
    slept_for=$((slept_for + sleep_for))
  done
echo "SGE properly configured after ${slept_for}s"
