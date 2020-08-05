FROM strimzi/kafka:0.18.0-kafka-2.5.0
USER root:root
RUN mkdir -p /opt/kafka/plugins/camel-aws-s3
COPY ./camel-aws-s3-kafka-connector-0.4.0-package/camel-aws-s3-kafka-connector/ /opt/kafka/plugins/camel-aws-s3/
USER 1001
