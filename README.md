# tipoca-stream
[![CI Status](https://travis-ci.com/practo/tipoca-stream.svg?token=kWeQdLBoqkiCi2kdxHdt&branch=master)](https://travis-ci.com/practo/tipoca-stream)

<img src="arch.png">

---

Near real time cloud native data pipeline. Just another data pipeline.

## Install
The pipeline is a combination of services deployed independently.

- **RedshiftSink** Using the CRD written in this repo. [Instructions and code.](https://github.com/practo/tipoca-stream/blob/master/redshiftsink/README.md)
```
      kubectl get redshiftsink
```
Creating the RedshiftSink CRD object installs Batcher and Loader pods in the cluster. These pods sinks the data from Kafka topics to Redshift, it also takes care of the database migration when required. Redshiftsink also has a rich [masking](https://github.com/practo/tipoca-stream/blob/master/redshiftsink/MASKING.md) support. (TODO=>) It supports table reloads in Redshift when masking configurations are modified in Github.

- **Producer** Using [Strimzi](http://strimzi.io/) CRDs. [Instructions.](https://github.com/practo/tipoca-stream/blob/master/producer/README.md) (can be deployed as regular deployments as well)
```
      kubectl get kafkaconnect
      kubectl get kafkaconnector
```
Creating the kafkaconect and kafkaconnector creates a kafkaconnect pod in the cluster which start streaming the data from the source(MYSQL, RDS, etc..) to Kafka.

- **Kafka** Using [Strimzi](http://strimzi.io/) CRDs or self hosted or managed kafka. (TODO instructions)
```
      kubectl get kafka
```

## Contribute
This repo holds the code for [redshiftsink](./redshiftsink) only. Please follow [this](https://github.com/practo/tipoca-stream/blob/master/redshiftsink/README.md#contributing) for bringing a change.

## Thanks

- [Debezium](https://debezium.io/).
- [Strimzi.io](http://strimzi.io/) for the Kafka CRDs.
- Yelp for open-sourcing the [the blog](https://engineeringblog.yelp.com/2016/10/redshift-connector.html) on the redshift connector.
- Linkedin for open-sourcing [goavro](https://github.com/linkedin/goavro).
- Linkedin for donating [Kafka](https://kafka.apache.org).
- Shopify for open-sourcing [sarama](https://github.com/Shopify/sarama).
- Thockin for open-sourcing [go-build-template](https://github.com/thockin/go-build-template).
- Clever for open-sourcing [s3-to-redshift library](https://github.com/Clever/s3-to-redshift/).
- herryg91 for open-sourcing [go batch libray](https://github.com/herryg91/gobatch).
- danielqsj for [kafka-exporter](https://github.com/danielqsj/kafka_exporter)
