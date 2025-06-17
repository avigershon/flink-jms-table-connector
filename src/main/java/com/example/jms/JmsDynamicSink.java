package com.example.jms;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink.SinkRuntimeProvider;
import org.apache.flink.table.types.DataType;

/**
 * Simple JMS table sink skeleton.
 */
public class JmsDynamicSink implements DynamicTableSink {

    private final DataType consumedDataType;

    public JmsDynamicSink(DataType consumedDataType) {
        this.consumedDataType = consumedDataType;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        // TODO: create JMS producer here and return SinkFunction provider
        return SinkFunctionProvider.of(null);
    }

    @Override
    public DynamicTableSink copy() {
        return new JmsDynamicSink(consumedDataType);
    }

    @Override
    public String asSummaryString() {
        return "JMS Table Sink";
    }
}
