package com.example.jms;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.connector.format.DecodingFormat;
import com.example.jms.JmsExactlyOnceSourceFunction;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

/**
 * Simple JMS table source skeleton.
 */
public class JmsDynamicSource implements ScanTableSource {

    private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
    private final DataType producedDataType;
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

    public JmsDynamicSource(
            DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
            DataType producedDataType,
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
        this.decodingFormat = decodingFormat;
        this.producedDataType = producedDataType;
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
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanTableSource.ScanRuntimeProvider getScanRuntimeProvider(
            ScanTableSource.ScanContext runtimeProviderContext) {
        DeserializationSchema<RowData> deserializer =
                decodingFormat.createRuntimeDecoder(runtimeProviderContext, producedDataType);

        JmsExactlyOnceSourceFunction sourceFunction =
                new JmsExactlyOnceSourceFunction(
                        deserializer,
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

        // mark the source as parallel so each subtask gets its own JMS consumer
        return SourceFunctionProvider.of(sourceFunction, true);
    }

    @Override
    public DynamicTableSource copy() {
        return new JmsDynamicSource(
                decodingFormat,
                producedDataType,
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
        return "JMS Table Source (exactly once)";
    }
}
