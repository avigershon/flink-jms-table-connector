package com.example.jms;

import java.util.Set;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DecodingFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.EncodingFormatFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

/**
 * Factory for JMS table connector.
 */
public class JmsTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String IDENTIFIER = "jms";

    public static final ConfigOption<String> INITIAL_CONTEXT_FACTORY = ConfigOptions
            .key("jms.initial-context-factory")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> PROVIDER_URL = ConfigOptions
            .key("jms.provider-url")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> DESTINATION = ConfigOptions
            .key("jms.destination")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> USERNAME = ConfigOptions
            .key("jms.username")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> PASSWORD = ConfigOptions
            .key("jms.password")
            .stringType()
            .noDefaultValue();

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Set.of(INITIAL_CONTEXT_FACTORY, PROVIDER_URL, DESTINATION);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        // Allow specifying a data format such as 'json'
        // so users can define 'format' in the WITH clause.
        return Set.of(FactoryUtil.FORMAT, USERNAME, PASSWORD);
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
                helper.discoverDecodingFormat(
                        DecodingFormatFactory.class,
                        FactoryUtil.FORMAT);

        DataType dataType = context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();

        String contextFactory = helper.getOptions().get(INITIAL_CONTEXT_FACTORY);
        String providerUrl = helper.getOptions().get(PROVIDER_URL);
        String destination = helper.getOptions().get(DESTINATION);
        String username = helper.getOptions().get(USERNAME);
        String password = helper.getOptions().get(PASSWORD);

        // validation
        helper.validate();

        return new JmsDynamicSource(
                decodingFormat,
                dataType,
                contextFactory,
                providerUrl,
                destination,
                username,
                password);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        EncodingFormat<SerializationSchema<RowData>> encodingFormat =
                helper.discoverEncodingFormat(
                        EncodingFormatFactory.class,
                        FactoryUtil.FORMAT);

        DataType dataType = context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();

        String contextFactory = helper.getOptions().get(INITIAL_CONTEXT_FACTORY);
        String providerUrl = helper.getOptions().get(PROVIDER_URL);
        String destination = helper.getOptions().get(DESTINATION);
        String username = helper.getOptions().get(USERNAME);
        String password = helper.getOptions().get(PASSWORD);

        // validation
        helper.validate();

        return new JmsDynamicSink(
                encodingFormat,
                dataType,
                contextFactory,
                providerUrl,
                destination,
                username,
                password);
    }
}
