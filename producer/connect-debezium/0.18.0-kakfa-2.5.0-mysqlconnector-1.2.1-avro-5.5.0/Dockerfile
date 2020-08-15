FROM strimzi/kafka:0.18.0-kafka-2.5.0
USER root:root
RUN mkdir -p /opt/kafka/plugins/debezium
COPY ./debezium-connector-mysql/ /opt/kafka/plugins/debezium/
COPY ./kafka-connect-avro-converter-5.5.0.jar /opt/kafka/libs/
USER 1001
