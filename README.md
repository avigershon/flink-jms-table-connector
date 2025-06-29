# Flink JMS Table Connector

This project contains a JMS table connector for [Apache Flink](https://flink.apache.org/). It now includes a fully functional JMS consumer and producer capable of exactly-once delivery. The connector relies on the community maintained [`flink-connector-jms`](https://github.com/miwurster/flink-connector-jms) library and uses the **Jakarta JMS** API. Exactly-once semantics are implemented using JMS transacted sessions that commit when Flink checkpoints complete. The source implements **ParallelSourceFunction**, so configuring the job with a parallelism of `N` will spawn `N` independent JMS consumers that share the queue. JMS distributes messages to these subtasks in a round-robin fashion. The factory registers under the identifier `jms` so you can define a table like:

```sql
CREATE TABLE ibm_mq (
  field1 STRING,
  field2 INT
) WITH (
  'connector'                    = 'jms',
  'jms.initial-context-factory'  = 'com.ibm.mq.jakarta.jms.context.WMQInitialContextFactory',
  'jms.provider-url'             = 'mq://host:1414/QMGR',
  'jms.destination'              = 'MY.QUEUE',
  'jms.username'                = 'myuser',
  'jms.password'                = 'secret',
  -- disable exactly once semantics if messages should be committed
  -- without waiting for a Flink checkpoint
  'jms.exactly-once'            = 'false',
  -- map logical queue names for the JNDI context (optional)
  'queue.MY.QUEUE'             = 'MY.QUEUE',
  'format'                       = 'json'
);
```

If the IBM MQ JNDI libraries are not available you can configure the MQ
connection directly without using `jms.initial-context-factory` and
`jms.provider-url`:

```sql
CREATE TABLE ibm_mq (
  field1 STRING,
  field2 INT
) WITH (
  'connector'       = 'jms',
  'jms.destination' = 'MY.QUEUE',
  'jms.mq-host'     = 'mq.example.com',
  'jms.mq-port'     = '1414',
  'jms.mq-queue-manager' = 'QMGR',
  'jms.mq-channel' = 'DEV.APP.SVRCONN',
  -- disable exactly-once semantics if desired
  'jms.exactly-once' = 'false',
  'format'         = 'json'
);
```

Any options prefixed with `queue.` are added to the JNDI environment. This allows
you to map logical names to JMS queues when using providers like Qpid that
expect such entries (e.g. `queue.MY.QUEUE = MY.QUEUE`). If `jms.destination`
is set and no corresponding `queue.<dest>` option is supplied, the connector will
automatically add `'queue.<dest>' = <dest>`.

Set `'jms.exactly-once' = 'false'` if you want the sink to commit each message
immediately without waiting for a Flink checkpoint.

The `jms.username` and `jms.password` options are optional and are passed to the
underlying JMS `ConnectionFactory` when establishing the connection.

When building the connector make sure that the required JMS implementation
libraries are available at runtime.  The `pom.xml` in this repository
uses the Maven Shade plugin so the resulting jar contains the JMS API and
the RabbitMQ JMS client.  If you prefer using **Qpid JMS** as provider
you also need to include its Netty dependencies (e.g. using `qpid-jms-client`
and `netty-all`).  Copy the shaded jar into Flink's `usrlib`
directory so the SQL client can load the connector together with all JMS
dependencies.

This project uses the **Jakarta JMS** API. Ensure that the JMS client
implementation you provide is compiled for the `jakarta.jms` package.
Mixing different JMS API versions (for example a client that still depends
on `javax.jms`) will lead to class loading errors at runtime.
