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

    public JmsDynamicSink(
            EncodingFormat<SerializationSchema<RowData>> encodingFormat,
            DataType consumedDataType,
            String contextFactory,
            String providerUrl,
            String destination) {
        this.encodingFormat = encodingFormat;
        this.consumedDataType = consumedDataType;
        this.contextFactory = contextFactory;
        this.providerUrl = providerUrl;
        this.destination = destination;
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
                new JmsSinkFunction(serializer, contextFactory, providerUrl, destination);

        return SinkFunctionProvider.of(sinkFunction);
    }

    @Override
    public DynamicTableSink copy() {
        return new JmsDynamicSink(
                encodingFormat, consumedDataType, contextFactory, providerUrl, destination);
    }

    @Override
    public String asSummaryString() {
        return "JMS Table Sink";
    }
}
