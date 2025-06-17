CREATE TABLE example_jms (
  field1 STRING,
  field2 INT
) WITH (
  'connector' = 'jms',
  'jms.initial-context-factory' = 'com.example.ContextFactory',
  'jms.provider-url' = 'mq://localhost:1414',
  'jms.destination' = 'MY.QUEUE',
  'format' = 'json'
);

-- run a simple query
SHOW TABLES;
