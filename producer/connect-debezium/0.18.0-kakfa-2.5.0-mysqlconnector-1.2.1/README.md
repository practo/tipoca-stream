## Downloading the connector binary
```
curl https://repo1.maven.org/maven2/io/debezium/debezium-connector-mysql/1.2.1.Final/debezium-connector-mysql-1.2.1.Final-plugin.tar.gz | tar xvz
```
Note: You might need to download from browser.

More Info: https://strimzi.io/blog/2020/01/27/deploying-debezium-with-kafkaconnector-resource/


## Instructions to build the image
```
export DOCKER_ORG=practodev
docker build . -t ${DOCKER_ORG}/connect-debezium:0.18.0-kakfa-2.5.0-mysqlconnector-1.2.1
docker push ${DOCKER_ORG}/connect-debezium:0.18.0-kakfa-2.5.0-mysqlconnector-1.2.1
```