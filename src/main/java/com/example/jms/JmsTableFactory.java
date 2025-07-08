package com.example.jms;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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

    // Direct IBM MQ configuration if no JNDI context is available
    public static final ConfigOption<String> MQ_HOST = ConfigOptions
            .key("jms.mq-host")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<Integer> MQ_PORT = ConfigOptions
            .key("jms.mq-port")
            .intType()
            .noDefaultValue();

    public static final ConfigOption<String> MQ_QUEUE_MANAGER = ConfigOptions
            .key("jms.mq-queue-manager")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> MQ_CHANNEL = ConfigOptions
            .key("jms.mq-channel")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<Boolean> EXACTLY_ONCE = ConfigOptions
            .key("jms.exactly-once")
            .booleanType()
            .defaultValue(true);

    public static final ConfigOption<Boolean> ASYNC_PUT = ConfigOptions
            .key("jms.async.put")
            .booleanType()
            .defaultValue(false);

    public static final String QUEUE_PREFIX = "queue.";
    public static final ConfigOption<String> QUEUE = ConfigOptions
            .key(QUEUE_PREFIX + "*")
            .stringType()
            .noDefaultValue();

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        // destination is always required. The connection can be configured
        // either via JNDI (initial-context-factory + provider-url) or
        // directly for IBM MQ using the mq-* options.
        return Set.of(DESTINATION);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        // Allow specifying a data format such as 'json'
        // so users can define 'format' in the WITH clause.
        return Set.of(
                FactoryUtil.FORMAT,
                USERNAME,
                PASSWORD,
                QUEUE,
                INITIAL_CONTEXT_FACTORY,
                PROVIDER_URL,
                MQ_HOST,
                MQ_PORT,
                MQ_QUEUE_MANAGER,
                MQ_CHANNEL,
                EXACTLY_ONCE,
                ASYNC_PUT);
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
        String mqHost = helper.getOptions().get(MQ_HOST);
        Integer mqPort = helper.getOptions().get(MQ_PORT);
        String mqQueueManager = helper.getOptions().get(MQ_QUEUE_MANAGER);
        String mqChannel = helper.getOptions().get(MQ_CHANNEL);
        boolean exactlyOnce = helper.getOptions().get(EXACTLY_ONCE);
        boolean asyncPut = helper.getOptions().get(ASYNC_PUT);
        Map<String, String> queueProps =
                context.getCatalogTable().getOptions().entrySet().stream()
                        .filter(e -> e.getKey().startsWith(QUEUE_PREFIX))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        if (!queueProps.containsKey(QUEUE_PREFIX + destination)) {
            queueProps.put(QUEUE_PREFIX + destination, destination);
        }

        // validation while ignoring queue.* options
        helper.validateExcept(QUEUE_PREFIX + "*");

        return new JmsDynamicSource(
                decodingFormat,
                dataType,
                contextFactory,
                providerUrl,
                destination,
                username,
                password,
                queueProps,
                mqHost,
                mqPort,
                mqQueueManager,
                mqChannel);
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
        String mqHost = helper.getOptions().get(MQ_HOST);
        Integer mqPort = helper.getOptions().get(MQ_PORT);
        String mqQueueManager = helper.getOptions().get(MQ_QUEUE_MANAGER);
        String mqChannel = helper.getOptions().get(MQ_CHANNEL);
        boolean exactlyOnce = helper.getOptions().get(EXACTLY_ONCE);
        Map<String, String> queueProps =
                context.getCatalogTable().getOptions().entrySet().stream()
                        .filter(e -> e.getKey().startsWith(QUEUE_PREFIX))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        if (!queueProps.containsKey(QUEUE_PREFIX + destination)) {
            queueProps.put(QUEUE_PREFIX + destination, destination);
        }

        // validation while ignoring queue.* options
        helper.validateExcept(QUEUE_PREFIX + "*");

        return new JmsDynamicSink(
                encodingFormat,
                dataType,
                contextFactory,
                providerUrl,
                destination,
                username,
                password,
                queueProps,
                mqHost,
                mqPort,
                mqQueueManager,
                mqChannel,
                exactlyOnce,
                asyncPut);
    }
}
