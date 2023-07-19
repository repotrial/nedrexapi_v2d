#!/bin/bash

while getopts c:h:p:d: flag
do
    case "${flag}" in
        c) config=${OPTARG};;
        p) port=${OPTARG};;
        h) host=${OPTARG};;
        d) db=${OPTARG};;
    esac
done


export NEDREX_CONFIG=$config
rq worker --url redis://$host:$port/$db default
