## Downloading the connector binary
```
curl https://repo1.maven.org/maven3/io/debezium/debezium-connector-mysql/1.2.1.Final/debezium-connector-mysql-1.2.1.Final-plugin.tar.gz | tar xvz
```
Note: You might need to download from browser.

More Info: https://strimzi.io/blog/2020/01/27/deploying-debezium-with-kafkaconnector-resource/
More Info: Debezium Docker images https://github.com/debezium/docker-images/tree/master/connect/1.2
AVRO JARS: https://github.com/debezium/docker-images/blob/master/connect-base/1.2/Dockerfile

### Helpful script for downloading binary
`./docker-maven-dowload.sh`

```
export MAVEN_DEP_DESTINATION="."
export CONFLUENT_VERSION=5.5.0
export AVRO_VERSION=1.9.2
export AVRO_JACKSON_VERSION=1.9.13
export APICURIO_VERSION=1.2.2.Final
./docker-maven-download.sh confluent kafka-connect-avro-converter "$CONFLUENT_VERSION" 16c38a7378032f850f0293b7654aa6bf && \
./docker-maven-download.sh confluent kafka-connect-avro-data "$CONFLUENT_VERSION" 63022db9533689968540f45be705786d && \
./docker-maven-download.sh confluent kafka-avro-serializer "$CONFLUENT_VERSION" b1379606e1dcc5d7b809c82abe294cc7 && \
./docker-maven-download.sh confluent kafka-schema-serializer "$CONFLUENT_VERSION" b68a7eebf7ce6a1b826bd5bbb443b176 && \
./docker-maven-download.sh confluent kafka-schema-registry-client "$CONFLUENT_VERSION" e3631a8a3fe10312a727e9d50fcd5527 && \
./docker-maven-download.sh confluent common-config "$CONFLUENT_VERSION" e1a4dc2b6d1d8d8c2df47db580276f38 && \
./docker-maven-download.sh confluent common-utils "$CONFLUENT_VERSION" ad9e39d87c6a9fa1a9b19e6ce80392fa && \
./docker-maven-download.sh central org/codehaus/jackson jackson-core-asl "$AVRO_JACKSON_VERSION" 319c49a4304e3fa9fe3cd8dcfc009d37 && \
./docker-maven-download.sh central org/codehaus/jackson jackson-mapper-asl "$AVRO_JACKSON_VERSION" 1750f9c339352fc4b728d61b57171613 && \
./docker-maven-download.sh central org/apache/avro avro "$AVRO_VERSION" cb70195f70f52b27070f9359b77690bb
```

## Instructions to build the image
```
export DOCKER_ORG=practodev
docker build . -t ${DOCKER_ORG}/connect-debezium:0.19.0-kakfa-2.5.0-mysqlconnector-1.2.1-avro-5.5.0
docker push ${DOCKER_ORG}/connect-debezium:0.19.0-kakfa-2.5.0-mysqlconnector-1.2.1-avro-5.5.0
```
