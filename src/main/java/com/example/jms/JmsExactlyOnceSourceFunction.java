package com.example.jms;

import java.util.Properties;

import jakarta.jms.BytesMessage;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;

import com.ibm.mq.jakarta.jms.MQConnectionFactory;
import com.ibm.msg.client.jakarta.wmq.WMQConstants;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.table.data.RowData;

/**
 * Exactly-once JMS SourceFunction using a transacted session. Messages are
 * committed when the Flink checkpoint completes.
 */

public class JmsExactlyOnceSourceFunction extends RichParallelSourceFunction<RowData>
        implements CheckpointedFunction, CheckpointListener {

    private static final long serialVersionUID = 1L;

    private final DeserializationSchema<RowData> deserializer;
    private final String contextFactory;
    private final String providerUrl;
    private final String destinationName;
    private final String username;
    private final String password;
    private final java.util.Map<String, String> jndiProperties;
    private final String mqHost;
    private final Integer mqPort;
    private final String mqQueueManager;
    private final String mqChannel;

    private transient Connection connection;
    private transient Session session;
    private transient MessageConsumer consumer;
    private volatile boolean running = true;

    public JmsExactlyOnceSourceFunction(
            DeserializationSchema<RowData> deserializer,
            String contextFactory,
            String providerUrl,
            String destinationName,
            String username,
            String password,
            java.util.Map<String, String> jndiProperties,
            String mqHost,
            Integer mqPort,
            String mqQueueManager,
            String mqChannel) {
        this.deserializer = deserializer;
        this.contextFactory = contextFactory;
        this.providerUrl = providerUrl;
        this.destinationName = destinationName;
        this.username = username;
        this.password = password;
        this.jndiProperties = jndiProperties;
        this.mqHost = mqHost;
        this.mqPort = mqPort;
        this.mqQueueManager = mqQueueManager;
        this.mqChannel = mqChannel;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        if (contextFactory != null && providerUrl != null) {
            Properties props = new Properties();
            props.setProperty(Context.INITIAL_CONTEXT_FACTORY, contextFactory);
            props.setProperty(Context.PROVIDER_URL, providerUrl);
            if (jndiProperties != null) {
                for (java.util.Map.Entry<String, String> e : jndiProperties.entrySet()) {
                    props.setProperty(e.getKey(), e.getValue());
                }
            }
            Context ctx = new InitialContext(props);
            ConnectionFactory factory = (ConnectionFactory) ctx.lookup("ConnectionFactory");
            Destination destination = (Destination) ctx.lookup(destinationName);

            if (username != null) {
                connection = factory.createConnection(username, password);
            } else {
                connection = factory.createConnection();
            }
            session = connection.createSession(true, Session.SESSION_TRANSACTED);
            consumer = session.createConsumer(destination);
        } else {
            MQConnectionFactory factory = new MQConnectionFactory();
            if (mqHost != null) {
                factory.setHostName(mqHost);
            }
            if (mqPort != null) {
                factory.setPort(mqPort);
            }
            if (mqQueueManager != null) {
                factory.setQueueManager(mqQueueManager);
            }
            if (mqChannel != null) {
                factory.setChannel(mqChannel);
            }
            factory.setTransportType(WMQConstants.WMQ_CM_CLIENT);

            if (username != null) {
                connection = factory.createConnection(username, password);
            } else {
                connection = factory.createConnection();
            }
            session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Destination destination = session.createQueue(destinationName);
            consumer = session.createConsumer(destination);
        }
        connection.start();

        deserializer.open(null);
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        while (running) {
            Message message = consumer.receive(1000);
            if (message == null) {
                continue;
            }

            byte[] bytes = null;
            try {
                if (message instanceof TextMessage) {
                    String text = ((TextMessage) message).getText();
                    if (text != null) {
                        bytes = text.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    }
                } else if (message instanceof BytesMessage) {
                    BytesMessage bm = (BytesMessage) message;
                    long length = bm.getBodyLength();
                    if (length > 0 && length <= Integer.MAX_VALUE) {
                        byte[] buffer = new byte[(int) length];
                        bm.readBytes(buffer);
                        bytes = buffer;
                    }
                }
            } catch (JMSException jmse) {
                System.err.println("Error extracting JMS payload: " + jmse.getMessage());
            }

            if (bytes != null) {
                RowData row = deserializer.deserialize(bytes);
                if (row != null) {
                    synchronized (ctx.getCheckpointLock()) {
                        ctx.collect(row);
                    }
                }
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
        try {
            if (consumer != null) {
                consumer.close();
            }
            if (session != null) {
                session.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (Exception ignore) {
        }
    }

    // CheckpointedFunction
    @Override
    public void snapshotState(org.apache.flink.runtime.state.FunctionSnapshotContext context) throws Exception {
        // nothing to store
    }

    @Override
    public void initializeState(org.apache.flink.runtime.state.FunctionInitializationContext context) throws Exception {
    }

    // CheckpointListener
    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        if (session != null) {
            long start = System.currentTimeMillis();
            System.out.println(
                    "Committing JMS session for checkpoint " + checkpointId + "...");
            session.commit();
            System.out.println(
                    "JMS session commit finished in "
                            + (System.currentTimeMillis() - start)
                            + " ms");
        }
    }
}
