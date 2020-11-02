# redshiftsink

redshiftsink reads the debezium events from Kafka and loads them to Redshift. It supports [masking](../MASKING.MD).

----

# Install Redshiftsink

### Install CRD
This installs the redshiftsink CRD in the cluster.
```bash
make install
```

### Verify Installation
Check the redshiftsink resource is accessible using kubectl
```bash
kubectl get redshiftsink
```

### Deploy Controller Manager
* Create secret(s):
```bash
cp config/manager/secret_sample config/manager/secret.txt
vim config/manager/secret.txt
```
* Install the controller. This creates service-account, secret and the controller manager deployment:
```bash
make deploy
```

### Configuration

## Redshiftsink Resource
```yaml


```

### Redshiftsink Spec Documentation:
| Spec          | Description   | Mandatory |
| :------------ | :----------- |:------------|


## RedshiftSink Managed Pods
Redshiftsink performs the sink by creating two pods. Creating a RedshiftSink CRD installs the batcher and loader pods. Batcher and loader pods details are below:

### Redshift Batcher
- Batches the debezium data in Kafka topics and uploads to S3.
- Signals the Redshift loader to load the batch in Redshift using Kafka Topics.
- **Batcher supports masking the data**. Please follow [this for enabling masking](https://github.com/practo/tipoca-stream/blob/master/redshiftsink/MASKING.md).

```bash
$ bin/darwin_amd64/redshiftbatcher --help
Consumes the Kafka Topics, trasnform them for redshfit, batches them and uploads to s3. Also signals the load of the batch on successful batch and upload operation..

Usage:
  redshiftbatcher [flags]

Flags:
      --config string   config file (default "./cmd/redshiftbatcher/config/config.yaml")
  -h, --help            help for redshiftbatcher
  -v, --v Level         number for the log level verbosity

```


### Configuration
Create a file config.yaml, refer [config-sample.yaml](./cmd/redshiftbatcher/config/config_sample.yaml).
```bash
cd cmd/redshiftbatcher/config/
cp config.sample.yaml config.yaml
```

## Redshift Loader
```bash
$ bin/darwin_amd64/redshiftloader --help
Loads the uploaded batch of debezium events to redshift.

Usage:
  redshiftloader [flags]

Flags:
      --config string   config file (default "./cmd/redshiftloader/config/config.yaml")
  -h, --help            help for redshiftloader
  -v, --v Level         number for the log level verbosity
```
- Loader performs schema migration.
- Loader performs the load of the data to Redshift by performing series of merge operations using Staging tables.

### Configuration
Create a file config.yaml, refer [config-sample.yaml](./cmd/redshiftbatcher/config/config_sample.yaml).
```bash
cd cmd/redshiftbatcher/config/
cp config.sample.yaml config.yaml
```

## Contributing
```bash
$ make build
binary: bin/darwin_amd64/redshiftbatcher
binary: bin/darwin_amd64/redshiftloader
```

###### Note (not required, all are internal now, FYI):
- `export GOPRIVATE="github.com/practo"`. [More.](https://medium.com/mabar/today-i-learned-fix-go-get-private-repository-return-error-reading-sum-golang-org-lookup-93058a058dd8)
- `~/.netrc` should be configured to download from private github repo. [More.](https://golang.org/doc/faq#git_https)
