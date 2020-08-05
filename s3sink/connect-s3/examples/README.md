# README

Prerequiste: `producer/examples/` should be setup.

```
kafka -> s3connector -> my-connect-cluster -> s3
```

### Install
```
k create -f connector-configmap.yaml
k create -f kc-my-connect-cluster.yaml
k create -f kctr-s3-sink-connector.yaml
```

### Test
- Watch over Kafka topic to see the change event reaches there:
```
    kubectl -n kafka exec k8s-kafka-0 -c kafka -i -t -- bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic dbserver1.inventory.customers
```

- Trigger change:
```
    kubectl get pods | grep mysql-debezium | awk '{print $1}' | xargs -I {} echo kubectl exec -it {} /bin/sh
    mysql -uroot -pdebezium -Dinventory
    UPDATE customers SET first_name='Anne Marie' WHERE id=1004;
```

- Check S3 a file get created for every change triggered:
```
s3://docker-vault/tipoca-stream-s3sink
```

### Cleanup
```
k delete -f connector-configmap.yaml
k delete -f kc-my-connect-cluster.yaml
k delete -f kctr-s3-sink-connector.yaml
```
