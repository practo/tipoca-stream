# Redshift Sink

## Batcher
- Batches the debezium data in Kafka topics and uploads to S3.
- Signals the Redshift loader to load the batch in Redshift using Kafka Topics.

## Loader
TODO


### Building the project

```bash
make build
```

#### Note
- `export GOPRIVATE="github.com/practo"`. [More.](https://medium.com/mabar/today-i-learned-fix-go-get-private-repository-return-error-reading-sum-golang-org-lookup-93058a058dd8)
- `~/.netrc` should be configured to download from private github repo. [More.](https://golang.org/doc/faq#git_https)
