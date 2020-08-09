# tipoca-stream

🚧Not ready for production. Work in progress.

<img src="arch.png">

---

Near real time data pipeline using Kafka and Strimzi applying the [Tipoca](https://github.com/practo/tipoca) transformations.

## Producers
RDS (mysql, postgres)

## Consumers
- S3 Sink (Raw)
- S3 Sink (Transformed)
- Tipoca Transformer

Note: Other applications can also write the consumers of the streamed data. But the code for them would not be part of this repository.

### S3 Sink (Raw)

Sink to S3 in the same format it is produced.

### S3 Sink (Transformed)

Sink to S3 after applying the tipoca transformations.

### Tipoca Transformer

- Watch over Producer outputted Kafka Topics.
- Apply tipoca transformations.
- Put the transformed data in a new Kafka Topic by prefix the topic name with `tipoca_`.
