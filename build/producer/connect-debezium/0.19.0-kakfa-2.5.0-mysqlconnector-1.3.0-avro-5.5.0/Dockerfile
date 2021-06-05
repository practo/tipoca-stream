FROM strimzi/kafka:0.19.0-kafka-2.5.0
USER root:root
RUN mkdir -p /opt/kafka/plugins/debezium
COPY docker-maven-download.sh /usr/local/bin/docker-maven-download

COPY ./debezium-connector-mysql/ /opt/kafka/plugins/debezium/

COPY ./kafka-connect-avro-converter-5.5.0.jar /opt/kafka/libs/
COPY ./kafka-connect-avro-data-5.5.0.jar /opt/kafka/libs/
COPY ./kafka-avro-serializer-5.5.0.jar /opt/kafka/libs/
COPY ./kafka-schema-serializer-5.5.0.jar /opt/kafka/libs/
COPY ./kafka-schema-registry-client-5.5.0.jar /opt/kafka/libs/
COPY ./avro-1.9.2.jar /opt/kafka/libs/
COPY ./common-config-5.5.0.jar /opt/kafka/libs/
COPY ./common-utils-5.5.0.jar /opt/kafka/libs/
COPY ./jackson-core-asl-1.9.13.jar /opt/kafka/libs/
COPY ./jackson-mapper-asl-1.9.13.jar /opt/kafka/libs/
USER 1001
