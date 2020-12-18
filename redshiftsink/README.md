# redshiftsink

redshiftsink reads the debezium events from Kafka and loads them to Redshift. It has rich support for [masking](../MASKING.MD).

----

<img src="arch-loader.png">

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
* Create redshiftsink secret containing aws secrets, redshift secrets and mask salt:
```bash
cp config/manager/kustomization_sample.yaml config/manager/kustomization.yaml
vim config/manager/kustomization.yaml #fill in ur secrets
NAME=redshfitsink-secret SECRETFILE=./config/manager/secret.txt make create-secret (TODO)
```

* Install the controller. This creates service-account, secret and the controller manager deployment:
```bash
make deploy
```

# Example

* Create the `Redshiftsink` custom resource. On creating it, the batcher and loader pods would be created which will start batching, masking and loading data to Redshift from Kafka topics.

```yaml
apiVersion: tipoca.k8s.practo.dev/v1
kind: RedshiftSink
metadata:
  name: inventory
spec:
  secretRefName: redshfitsink-secret-2bh89m59ct
  secretRefNamespace: kube-system
  kafkaBrokers: "kafka1.example.com,kafka2.example.com"
  kafkaTopicRegexes: "^db.inventory*"
  kafkaLoaderTopicPrefix: "loader-"
  batcher:
    suspend: false
    maxSize: 10
    maxWaitSeconds: 30
    mask: true
    maskFile: "github.com/practo/tipoca-stream/redshiftsink/pkg/transformer/masker/database.yaml"
    podTemplate:
      resources:
        requests:
          cpu: 100m
          memory: 200Mi
  loader:
    suspend: false
    maxSize: 10
    maxWaitSeconds: 30
    redshiftSchema: "inventory"
    podTemplate:
      resources:
        requests:
          cpu: 100m
          memory: 200Mi
```

```bash
kubectl create -f config/samples/tipoca_v1_redshiftsink.yaml
```

This will start syncing all the Kakfa topics matching regex `"^db.inventory*"` from Kafka to Redshift via S3. If masking is turned on it will also mask the data. More on masking [here](./MASKING.MD)

### Configuration

### Redshiftsink Spec Documentation (TODO):
| Spec          | Description   | Mandatory |
| :------------ | :----------- |:------------|


## RedshiftSink Managed Pods
Redshiftsink performs the sink by creating two pods. Creating a RedshiftSink CRD installs the batcher and loader pods. Batcher and loader pods details are below:

### Redshift Batcher
- Batches the debezium data in Kafka topics and uploads to S3.
- Signals the Redshift loader to load the batch in Redshift using Kafka Topics.
- **Batcher supports masking the data**. Please follow [this for enabling masking](https://github.com/practo/tipoca-stream/blob/master/redshiftsink/MASKING.md).

<img src="arch-batcher.png">

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

<img src="arch-loader.png">

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

* Generate CRD code and manifests.
```bash
make generate
make manifests
```

* Make changes and build code.
```bash
make build
binary: bin/darwin_amd64/redshiftbatcher
binary: bin/darwin_amd64/redshiftloader
binary: bin/darwin_amd64/redshiftsink
```

* Run the controller locally.
```bash
make run
```
