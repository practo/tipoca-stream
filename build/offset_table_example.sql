 CREATE TABLE inventory.staged ( kafkaoffset VARCHAR(32), debeziumop VARCHAR(32) NOT NULL,
 othertablePK1 character varying(50) NOT NULL,
 othertablePK2 character varying(50) NOT NULL,
 value VARCHAR(32) NOT NULL,
 CONSTRAINT key1 PRIMARY KEY (kafkaoffset)
 );
