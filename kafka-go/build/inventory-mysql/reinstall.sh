#!/bin/bash

echo deleting
kubectl delete -f inventory-mysql.yaml -n kafka
sleep 2
kubectl delete -f inventory-mysql-pvc.yaml -n kafka
kubectl delete -f inventory-mysql-svc.yaml -n kafka

sleep 5

set -e

echo creating
kubectl create -f inventory-mysql-pvc.yaml -n kafka
kubectl create -f inventory-mysql.yaml -n kafka
kubectl create -f inventory-mysql-svc.yaml -n kafka

echo recreated
