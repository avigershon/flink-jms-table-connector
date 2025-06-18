package com.example.jms;

import java.util.Properties;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.naming.Context;
import javax.naming.InitialContext;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;

/**
 * A simple JMS {@link org.apache.flink.streaming.api.functions.source.SourceFunction}
 * that converts incoming JMS messages into {@link RowData} using a provided
 * {@link DeserializationSchema}.
 */
public class JmsSourceFunction extends RichSourceFunction<RowData> {

    private final DeserializationSchema<RowData> deserializer;
    private final String contextFactory;
    private final String providerUrl;
    private final String destinationName;

    private transient Connection connection;
    private transient Session session;
    private transient MessageConsumer consumer;
    private volatile boolean running = true;

    public JmsSourceFunction(
            DeserializationSchema<RowData> deserializer,
            String contextFactory,
            String providerUrl,
            String destinationName) {
        this.deserializer = deserializer;
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
        consumer = session.createConsumer(destination);
        connection.start();
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        while (running) {
            Message message = consumer.receive(1000);
            if (message instanceof BytesMessage) {
                BytesMessage bm = (BytesMessage) message;
                byte[] bytes = new byte[(int) bm.getBodyLength()];
                bm.readBytes(bytes);
                RowData row = deserializer.deserialize(bytes);
                ctx.collect(row);
            } else if (message != null) {
                // try to handle text messages
                byte[] bytes = message.getBody(byte[].class);
                if (bytes != null) {
                    RowData row = deserializer.deserialize(bytes);
                    ctx.collect(row);
                }
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (Exception ignore) {
        }
    }
}
