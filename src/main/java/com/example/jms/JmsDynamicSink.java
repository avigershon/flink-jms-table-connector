package com.example.jms;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import com.example.jms.JmsSinkFunction;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink.SinkRuntimeProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

/**
 * Simple JMS table sink skeleton.
 */
public class JmsDynamicSink implements DynamicTableSink {

    private final EncodingFormat<SerializationSchema<RowData>> encodingFormat;
    private final DataType consumedDataType;
    private final String contextFactory;
    private final String providerUrl;
    private final String destination;
    private final String username;
    private final String password;
    private final java.util.Map<String, String> jndiProperties;
    private final String mqHost;
    private final Integer mqPort;
    private final String mqQueueManager;
    private final String mqChannel;

    public JmsDynamicSink(
            EncodingFormat<SerializationSchema<RowData>> encodingFormat,
            DataType consumedDataType,
            String contextFactory,
            String providerUrl,
            String destination,
            String username,
            String password,
            java.util.Map<String, String> jndiProperties,
            String mqHost,
            Integer mqPort,
            String mqQueueManager,
            String mqChannel) {
        this.encodingFormat = encodingFormat;
        this.consumedDataType = consumedDataType;
        this.contextFactory = contextFactory;
        this.providerUrl = providerUrl;
        this.destination = destination;
        this.username = username;
        this.password = password;
        this.jndiProperties = jndiProperties;
        this.mqHost = mqHost;
        this.mqPort = mqPort;
        this.mqQueueManager = mqQueueManager;
        this.mqChannel = mqChannel;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        SerializationSchema<RowData> serializer =
                encodingFormat.createRuntimeEncoder(context, consumedDataType);

        JmsSinkFunction sinkFunction =
                new JmsSinkFunction(
                        serializer,
                        contextFactory,
                        providerUrl,
                        destination,
                        username,
                        password,
                        jndiProperties,
                        mqHost,
                        mqPort,
                        mqQueueManager,
                        mqChannel);

        return SinkFunctionProvider.of(sinkFunction);
    }

    @Override
    public DynamicTableSink copy() {
        return new JmsDynamicSink(
                encodingFormat,
                consumedDataType,
                contextFactory,
                providerUrl,
                destination,
                username,
                password,
                jndiProperties,
                mqHost,
                mqPort,
                mqQueueManager,
                mqChannel);
    }

    @Override
    public String asSummaryString() {
        return "JMS Table Sink";
    }
}
