# README

```
mysql-debezium-pod -> inventory-connector -> my-connect-cluster -> kafka
```

### Install
```
kubectl create -f mysql.yaml
kubectl create -f mysql-svc.yaml
kubectl create -f connector-configmap.yaml
kubectl create -f my-connect-cluster.yaml
sleep 120
kubectl create -f inventory-connector.yaml
```

### Test

- Watch over Kafka topic to see the change event reaches there:
```
    kubectl -n kafka exec k8s-kafka-0 -c kafka -i -t -- bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic dbserver1.inventory.customers
```

(with avro)
```
    kubectl -n kafka exec k8s-kafka-0 -c kafka -i -t -- bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic dbserver1.inventory.customers --property schema.registry.url=http://schema-registry501.kafka:8081 --formatter io.confluent.kafka.formatter.AvroMessageFormatter --property print.key=true
```


- Trigger change:
```
    kubectl get pods | grep mysql-debezium | awk '{print $1}' | xargs -I {} echo kubectl exec -it {} /bin/sh
    mysql -uroot -pdebezium -Dinventory
    UPDATE customers SET first_name='Anne Marie' WHERE id=1004;
```

### Cleanup
```
kubectl delete -f mysql.yaml
kubectl delete -f mysql-svc.yaml
kubectl delete -f connector-configmap.yaml
kubectl delete -f my-connect-cluster.yaml
kubectl delete -f inventory-connector.yaml
kubectl get kt -n kafka | grep dbserver1 | awk '{print $1}' | xargs -I {} kubectl delete kt {} -n kafka
```
