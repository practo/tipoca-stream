#!/bin/bash

## Example: ./insert.sh example.cluster.com 10
## this inserts 10 new rows in inventory.customers table

if [ -z "$1" ]; then
    echo "kubectl context missing"
    exit 1
else
    context=$1
fi

if [ -z "$2" ]; then
    echo "no of records to insert missing"
    exit 1
else
    records=$2
fi


set -e

echo "Inserting"
kubectl --context=${context} get pods -n kafka | grep inventory-mysql | awk '{print $1}' | xargs -I {} kubectl --context=${context} exec {} -n kafka -- bash /usr/local/bin/load.sh ${records}
