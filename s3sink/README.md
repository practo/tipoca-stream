# S3 Sink

```
kafka topic    => S3-Connector    => Connect => S3
```

## No code project

This is a no code and only configuration deployment project, repo folder is just there for compatibility with other tools. Deploying the configurations creates the Strimzi Kafka Connector and Kafka Connect.

Kafka is a strimzi managed service running in Kubernetes.

### Staging

```
practl create st datapipe -p tipoca-stream-s3sink:master
```


### Production
Deplex has the following stream producer products under `/ship`.

```
tipoca-stream-s3sink
```
