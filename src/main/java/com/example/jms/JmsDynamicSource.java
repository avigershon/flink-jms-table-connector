package com.example.jms;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

/**
 * Simple JMS table source skeleton.
 */
public class JmsDynamicSource implements ScanTableSource {

    private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
    private final DataType producedDataType;

    public JmsDynamicSource(
            DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
            DataType producedDataType) {
        this.decodingFormat = decodingFormat;
        this.producedDataType = producedDataType;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanTableSource.ScanRuntimeProvider getScanRuntimeProvider(
            ScanTableSource.ScanContext runtimeProviderContext) {
        // TODO: create JMS consumer here and return SourceFunction provider
        return null;
    }

    @Override
    public DynamicTableSource copy() {
        return new JmsDynamicSource(decodingFormat, producedDataType);
    }

    @Override
    public String asSummaryString() {
        return "JMS Table Source";
    }
}
