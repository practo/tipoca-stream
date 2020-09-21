#!/bin/bash

pod=`kubectl get pods -n kafka | grep inventory-mysql  | awk '{print $1}'`

kubectl exec -it -n kafka $pod bash /usr/local/bin/lmysql
