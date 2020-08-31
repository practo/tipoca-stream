CREATE TABLE currentTable
( kafkaOffset INT(11),
operation VARCHAR(32) NOT NULL,
otherTablePK INT(11) NOT NULL,
otherTableOrderStatus VARCHAR(32) NOT NULL,
CONSTRAINT key1 PRIMARY KEY (kafkaOffset)
);
