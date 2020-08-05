## Downloading the connector binary
```
https://repo1.maven.org/maven2/org/apache/camel/kafkaconnector/camel-aws-s3-kafka-connector/0.4.0/camel-aws-s3-kafka-connector-0.4.0-package.tar.gz
```

Note: You might need to download from browser.

More Info:
- https://github.com/apache/camel-kafka-connector
- https://ibm-cloud-architecture.github.io/refarch-eda/scenarios/connect-s3/
- https://github.com/strimzi/strimzi-kafka-operator/issues/3110


## Instructions to build the image
```
export DOCKER_ORG=practodev
docker build . -t ${DOCKER_ORG}/connect-s3:0.18.0-kafka-2.5.0-camels3connector-0.4.0
docker push ${DOCKER_ORG}/connect-s3:0.18.0-kafka-2.5.0-camels3connector-0.4.0
```