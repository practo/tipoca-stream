# tipoca-stream
[![CI Status](https://travis-ci.com/practo/tipoca-stream.svg?token=kWeQdLBoqkiCi2kdxHdt&branch=master)](https://travis-ci.com/practo/tipoca-stream)

<img src="arch.png">

---

Near real time cloud native data pipeline. Just another data pipeline.

## Install
The pipeline is a combination of multiple services deployment independently.

- **RedshiftSink** Using the CRD written in this repo. [Instructions.](https://github.com/practo/tipoca-stream/blob/master/redshiftsink/README.md)
```
      kubectl get redshiftsink (TODO)
```

- **Producer** Using Strimzi CRDs. [Instructions.](https://github.com/practo/tipoca-stream/blob/master/producer/README.md) (can be deployed as regular deployments as well)
```
      kubectl get kafkaconnect
      kubectl get kafkaconnector
```
- **Kafka** Using Strimzi CRDs or self hosted or managed kafka. (TODO instructions)
```
      kubectl get kafka
```
- **Schema Registry** Deployment using helm charts. (TODO instructions)

Note: Redshiftsink [supports masking](https://github.com/practo/tipoca-stream/blob/master/redshiftsink/MASKING.md).

## Contribute
This repo holds the code for redshiftsink only. Please follow [this](https://github.com/practo/tipoca-stream/blob/master/redshiftsink/README.md#contributing).

## Thanks

- [Debezium](https://debezium.io/).
- [Strimzi.io](http://strimzi.io/).
- Yelp for open-sourcing the [the blog](https://engineeringblog.yelp.com/2016/10/redshift-connector.html) on the redshift connector.
- Confluent for open-sourcing [Kafka Connect](https://docs.confluent.io/current/connect/index.html) and [Kafka Schema registry](https://github.com/confluentinc/schema-registry).
- Linkedin for open-sourcing [goavro](https://github.com/linkedin/goavro).
- Linkedin for donating [Kafka](https://kafka.apache.org).
- Shopify for open-sourcing [sarama](https://github.com/Shopify/sarama).
- Thockin for open-sourcing [go-build-template](https://github.com/thockin/go-build-template).
- Clever for open-sourcing [s3-to-redshift library](https://github.com/Clever/s3-to-redshift/).
- herryg91 for open-sourcing [go batch libray](https://github.com/herryg91/gobatch).
