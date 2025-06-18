# Flink JMS Table Connector

This project contains a very small proof-of-concept implementation of a JMS table connector for [Apache Flink](https://flink.apache.org/). It shows how a custom table source and sink can be wired using Flink's `DynamicTableFactory` interfaces. The connector relies on the community maintained [`flink-connector-jms`](https://github.com/miwurster/flink-connector-jms) library and uses the **Jakarta JMS** API.

The implementation is intentionally minimal and does not include a real JMS consumer or producer. It is meant as a starting point for integrating a JMS queue with Flink SQL. The factory registers under the identifier `jms` so you can define a table like:

```sql
CREATE TABLE ibm_mq (
  field1 STRING,
  field2 INT
) WITH (
  'connector'                    = 'jms',
  'jms.initial-context-factory'  = 'com.ibm.mq.jms.context.WMQInitialContextFactory',
  'jms.provider-url'             = 'mq://host:1414/QMGR',
  'jms.destination'              = 'MY.QUEUE',
  'jms.username'                = 'myuser',
  'jms.password'                = 'secret',
  -- map logical queue names for the JNDI context
  'queue.MY.QUEUE'             = 'MY.QUEUE',
  'format'                       = 'json'
);
```

Any options prefixed with `queue.` are added to the JNDI environment. This allows
you to map logical names to JMS queues when using providers like Qpid that
expect such entries (e.g. `queue.MY.QUEUE = MY.QUEUE`).

The `jms.username` and `jms.password` options are optional and are passed to the
underlying JMS `ConnectionFactory` when establishing the connection.

To turn this into a functional connector you would need to implement JMS consumer and producer logic inside `JmsDynamicSource` and `JmsDynamicSink`.

When building the connector make sure that the required JMS implementation
libraries are available at runtime.  The `pom.xml` in this repository
uses the Maven Shade plugin so the resulting jar contains the JMS API and
the RabbitMQ JMS client.  Copy the shaded jar into Flink's `usrlib`
directory so the SQL client can load the connector together with the JMS
dependencies.

This project uses the **Jakarta JMS** API. Ensure that the JMS client
implementation you provide is compiled for the `jakarta.jms` package.
Mixing different JMS API versions (for example a client that still depends
on `javax.jms`) will lead to class loading errors at runtime.
