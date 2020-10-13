CREATE TABLE currentTable
( kafkaoffset INT(11),
debeziumop VARCHAR(32) NOT NULL,
othertablePK INT(11) NOT NULL,
othertableorderstatus VARCHAR(32) NOT NULL,
CONSTRAINT key1 PRIMARY KEY (kafkaoffset)
);
