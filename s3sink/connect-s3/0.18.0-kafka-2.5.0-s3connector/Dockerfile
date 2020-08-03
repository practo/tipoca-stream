FROM strimzi/kafka:0.18.0-kafka-2.5.0
USER root:root
RUN mkdir -p /opt/kafka/plugins/camel-aws-s3
COPY ./debezium-connector-mysql/ /opt/kafka/plugins/camel-aws-s3/
USER 1001
