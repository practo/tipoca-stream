FROM strimzi/kafka:0.27.0-kafka-3.0.0
USER root:root
RUN mkdir -p /opt/kafka/plugins/debezium
COPY docker-maven-download.sh /usr/local/bin/docker-maven-download

COPY ./debezium-connector-mysql/ /opt/kafka/plugins/debezium/

COPY ./kafka-connect-avro-converter-6.0.2.jar /opt/kafka/libs/
COPY ./kafka-connect-avro-data-6.0.2.jar /opt/kafka/libs/
COPY ./kafka-avro-serializer-6.0.2.jar /opt/kafka/libs/
COPY ./kafka-schema-serializer-6.0.2.jar /opt/kafka/libs/
COPY ./kafka-schema-registry-client-6.0.2.jar /opt/kafka/libs/
COPY ./avro-1.9.2.jar /opt/kafka/libs/
COPY ./common-config-6.0.2.jar /opt/kafka/libs/
COPY ./common-utils-6.0.2.jar /opt/kafka/libs/
COPY ./jackson-core-asl-2.10.12.jar /opt/kafka/libs/
COPY ./jackson-mapper-asl-2.10.12.jar /opt/kafka/libs/
USER 1001
