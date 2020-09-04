#!/bin/bash

## Example: ./insert.sh 10
## this inserts 10 new rows in inventory.customers table

set -e

echo "generating load"
kubectl get pods -n kafka | grep inventory-mysql  | awk '{print $1}' | xargs -I {} kubectl exec {} -n kafka -- bash /usr/local/bin/load.sh $1
