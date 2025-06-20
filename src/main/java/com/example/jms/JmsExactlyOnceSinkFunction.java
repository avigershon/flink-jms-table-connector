package com.example.jms;

import jakarta.jms.BytesMessage;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.Destination;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import javax.naming.InitialContext;

import com.ibm.mq.jakarta.jms.MQConnectionFactory;
import com.ibm.msg.client.jakarta.wmq.WMQConstants;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.apache.flink.table.data.RowData;

/**
 * Exactly-once JMS sink using Flink's TwoPhaseCommitSinkFunction. Messages are
 * written within a JMS transaction that is committed once the Flink checkpoint
 * completes.
 */
public class JmsExactlyOnceSinkFunction extends TwoPhaseCommitSinkFunction<RowData, JmsExactlyOnceSinkFunction.JmsTransaction, Void> {

    private final SerializationSchema<RowData> serializer;
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

    public JmsExactlyOnceSinkFunction(
            SerializationSchema<RowData> serializer,
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
        super(RowData.class, JmsTransaction.class, Void.class);
        this.serializer = serializer;
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

    static class JmsTransaction {
        Connection connection;
        Session session;
        MessageProducer producer;
    }

    @Override
    protected JmsTransaction beginTransaction() throws Exception {
        JmsTransaction txn = new JmsTransaction();
        if (contextFactory != null && providerUrl != null) {
            Properties props = new Properties();
            props.setProperty(javax.naming.Context.INITIAL_CONTEXT_FACTORY, contextFactory);
            props.setProperty(javax.naming.Context.PROVIDER_URL, providerUrl);
            if (jndiProperties != null) {
                for (java.util.Map.Entry<String, String> e : jndiProperties.entrySet()) {
                    props.setProperty(e.getKey(), e.getValue());
                }
            }
            javax.naming.Context ctx = new InitialContext(props);
            ConnectionFactory factory = (ConnectionFactory) ctx.lookup("ConnectionFactory");
            Destination destination = (Destination) ctx.lookup(destinationName);
            if (username != null) {
                txn.connection = factory.createConnection(username, password);
            } else {
                txn.connection = factory.createConnection();
            }
            txn.session = txn.connection.createSession(true, Session.SESSION_TRANSACTED);
            txn.producer = txn.session.createProducer(destination);
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
                txn.connection = factory.createConnection(username, password);
            } else {
                txn.connection = factory.createConnection();
            }
            txn.session = txn.connection.createSession(true, Session.SESSION_TRANSACTED);
            Destination destination = txn.session.createQueue(destinationName);
            txn.producer = txn.session.createProducer(destination);
        }
        txn.connection.start();
        return txn;
    }

    @Override
    protected void invoke(JmsTransaction transaction, RowData value, Context context) throws Exception {
        byte[] bytes = serializer.serialize(value);
        BytesMessage message = transaction.session.createBytesMessage();
        message.writeBytes(bytes);
        transaction.producer.send(message);
    }

    @Override
    protected void preCommit(JmsTransaction transaction) throws Exception {
        // nothing to do, commit happens in commit()
    }

    @Override
    protected void commit(JmsTransaction transaction) {
        try {
            transaction.session.commit();
        } catch (Exception e) {
            throw new RuntimeException("Failed to commit JMS transaction", e);
        } finally {
            cleanup(transaction);
        }
    }

    @Override
    protected void abort(JmsTransaction transaction) {
        try {
            transaction.session.rollback();
        } catch (Exception ignore) {
        } finally {
            cleanup(transaction);
        }
    }

    private void cleanup(JmsTransaction transaction) {
        try {
            if (transaction.producer != null) {
                transaction.producer.close();
            }
            if (transaction.session != null) {
                transaction.session.close();
            }
            if (transaction.connection != null) {
                transaction.connection.close();
            }
        } catch (Exception ignore) {
        }
    }

    @Override
    public void initializeState(org.apache.flink.runtime.state.FunctionInitializationContext context) throws Exception {
        super.initializeState(context);
        serializer.open(null);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }
}
