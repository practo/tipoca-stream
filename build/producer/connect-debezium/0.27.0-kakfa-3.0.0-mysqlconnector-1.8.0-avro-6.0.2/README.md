## Downloading the connector binary
```
curl https://repo1.maven.org/maven2/io/debezium/debezium-connector-mysql/1.8.0.Final/debezium-connector-mysql-1.8.0.Final-plugin.tar.gz | tar xvz
```
Note: You might need to download from browser.

More Info: https://strimzi.io/blog/2020/01/27/deploying-debezium-with-kafkaconnector-resource/
More Info: Debezium Docker images from which jar versions are referred 
- debezium jars: https://github.com/debezium/docker-images/tree/master/connect/1.8
- AVRO JARS: https://github.com/debezium/docker-images/blob/master/connect-base/1.8/Dockerfile

### Helpful script for downloading binary

```
export MAVEN_DEP_DESTINATION="."
export CONFLUENT_VERSION=6.0.2
export AVRO_VERSION=1.9.2
export APICURIO_VERSION=2.0.2.Final
./docker-maven-download.sh confluent kafka-connect-avro-converter "$CONFLUENT_VERSION" 4671dec77c8af4689e20419538e7b915 && \
./docker-maven-download.sh confluent kafka-connect-avro-data "$CONFLUENT_VERSION" 5dc1111ccc4dc9c57397a2c298e6a221 && \
./docker-maven-download.sh confluent kafka-avro-serializer "$CONFLUENT_VERSION" 5bb0c8078919e5aed55e9b59323a661e && \
./docker-maven-download.sh confluent kafka-schema-serializer "$CONFLUENT_VERSION" 907f384780d9b75e670e6a5f4f522873 && \
./docker-maven-download.sh confluent kafka-schema-registry-client "$CONFLUENT_VERSION" 727ef72bcc04c7a8dbf2439edf74ed38  && \
./docker-maven-download.sh confluent common-config "$CONFLUENT_VERSION" 0cfba1fc7203305ed25bd67b29b6f094 && \
./docker-maven-download.sh confluent common-utils "$CONFLUENT_VERSION" a940fcd0449613f956127f16cdea9935 && \
./docker-maven-download.sh central org/apache/avro avro "$AVRO_VERSION" cb70195f70f52b27070f9359b77690bb 
```

## Instructions to build the image
```
export REGISTRY=public.ecr.aws/practo
docker build . -t ${REGISTRY}/connect-debezium:0.27.0-kakfa-3.0.0-mysqlconnector-1.8.0-avro-6.0.2
docker push ${REGISTRY}/connect-debezium:0.27.0-kakfa-3.0.0-mysqlconnector-1.8.0-avro-6.0.2
```
