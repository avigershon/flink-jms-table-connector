package com.example.jms;

import java.util.Properties;

import jakarta.jms.BytesMessage;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.Destination;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import javax.naming.InitialContext;

// IBM MQ classes used when bypassing JNDI
import com.ibm.mq.jakarta.jms.MQConnectionFactory;
import com.ibm.msg.client.jakarta.wmq.WMQConstants;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;

/**
 * A simple JMS sink function that writes {@link RowData} records to a JMS destination.
 */
public class JmsSinkFunction extends RichSinkFunction<RowData> {

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
    private final boolean asyncPut;

    private transient Connection connection;
    private transient Session session;
    private transient MessageProducer producer;

    public JmsSinkFunction(
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
            String mqChannel,
            boolean asyncPut) {
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
        this.asyncPut = asyncPut;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
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
            if (asyncPut && factory instanceof MQConnectionFactory) {
                ((MQConnectionFactory) factory)
                        .setIntProperty(
                                WMQConstants.WMQ_PUT_ASYNC_ALLOWED,
                                WMQConstants.WMQ_PUT_ASYNC_ALLOWED_ENABLED);
            }
            Destination destination = (Destination) ctx.lookup(destinationName);
            if (username != null) {
                connection = factory.createConnection(username, password);
            } else {
                connection = factory.createConnection();
            }
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            producer = session.createProducer(destination);
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
            if (asyncPut) {
                factory.setIntProperty(
                        WMQConstants.WMQ_PUT_ASYNC_ALLOWED,
                        WMQConstants.WMQ_PUT_ASYNC_ALLOWED_ENABLED);
            }

            if (username != null) {
                connection = factory.createConnection(username, password);
            } else {
                connection = factory.createConnection();
            }
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue(destinationName);
            producer = session.createProducer(destination);
        }
        connection.start();
        // initialize serializer after connection setup so it is ready to
        // convert RowData records into the target message format
        serializer.open(null);
    }

    @Override
    public void invoke(RowData value, SinkFunction.Context context) throws Exception {
        byte[] bytes = serializer.serialize(value);
        BytesMessage message = session.createBytesMessage();
        message.writeBytes(bytes);
        producer.send(message);
    }

    @Override
    public void close() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }
}
