# Consumer GO

Consumer go holds the code of all the Kafka consumers written in Go.

- RedshiftSink: Batcher
- RedshiftSink: Loader

## Redshift Batcher
```
$ bin/darwin_amd64/redshiftbatcher --help
Consumes the Kafka Topics, transform them for redshfit, batches them and uploads to s3. Also signals the load of the batch on successful batch and upload operation..

Usage:
  redshiftbatcher [flags]

Flags:
      --config string   config file (default "config.yaml")
  -h, --help            help for redshiftbatcher
  -v, --v Level         number for the log level verbosity
 ```

- Batches the debezium data in Kafka topics and uploads to S3.
- Signals the Redshift loader to load the batch in Redshift using Kafka Topics.

### Configuration
Create a file config.yaml, refer [config-sample.yaml](./cmd/redshiftbatcher/config/config_sample.yaml).
```bash
cd cmd/redshiftbatcher/config/
cp config.sample.yaml config.yaml
```

### Contributing
```bash
make build
```

```bash
 bin/darwin_amd64/redshiftbatcher --config=config.yaml
```

###### Note:
- `export GOPRIVATE="github.com/practo"`. [More.](https://medium.com/mabar/today-i-learned-fix-go-get-private-repository-return-error-reading-sum-golang-org-lookup-93058a058dd8)
- `~/.netrc` should be configured to download from private github repo. [More.](https://golang.org/doc/faq#git_https)


## Redshift Loader
TODO
