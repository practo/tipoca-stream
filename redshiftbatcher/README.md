# Redshift Batcher

- Batches the debezium data in Kafka topics and uploads to S3.
- Signals the Redshift loader to load the batch in Redshift using Kafka Topics.
