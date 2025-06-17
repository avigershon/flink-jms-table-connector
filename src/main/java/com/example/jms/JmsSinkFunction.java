package com.example.jms;

import java.util.Properties;

import jakarta.jms.BytesMessage;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.Destination;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.naming.Context;
import jakarta.naming.InitialContext;

import org.apache.flink.api.common.functions.RichSinkFunction;
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

    private transient Connection connection;
    private transient Session session;
    private transient MessageProducer producer;

    public JmsSinkFunction(
            SerializationSchema<RowData> serializer,
            String contextFactory,
            String providerUrl,
            String destinationName) {
        this.serializer = serializer;
        this.contextFactory = contextFactory;
        this.providerUrl = providerUrl;
        this.destinationName = destinationName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Properties props = new Properties();
        props.setProperty(Context.INITIAL_CONTEXT_FACTORY, contextFactory);
        props.setProperty(Context.PROVIDER_URL, providerUrl);
        Context ctx = new InitialContext(props);
        ConnectionFactory factory = (ConnectionFactory) ctx.lookup("ConnectionFactory");
        Destination destination = (Destination) ctx.lookup(destinationName);
        connection = factory.createConnection();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        producer = session.createProducer(destination);
        connection.start();
    }

    @Override
    public void invoke(RowData value, Context context) throws Exception {
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
