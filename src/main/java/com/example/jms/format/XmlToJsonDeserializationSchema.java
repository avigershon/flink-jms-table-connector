package com.example.jms.format;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.json.JSONObject;
import org.json.XML;

/**
 * DeserializationSchema that converts XML payloads to JSON and then delegates
 * to Flink's JsonRowDataDeserializationSchema.
 */
public class XmlToJsonDeserializationSchema implements DeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final JsonRowDataDeserializationSchema jsonDeserializer;

    public XmlToJsonDeserializationSchema(RowType rowType) {
        this.jsonDeserializer = new JsonRowDataDeserializationSchema(
                rowType,
                InternalTypeInfo.of(rowType),
                false,
                false,
                TimestampFormat.ISO_8601);
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        jsonDeserializer.open(context);
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        try {
            String xml = new String(message, StandardCharsets.UTF_8);
            JSONObject jsonObject = XML.toJSONObject(xml);
            byte[] jsonBytes = jsonObject.toString().getBytes(StandardCharsets.UTF_8);
            return jsonDeserializer.deserialize(jsonBytes);
        } catch (Exception e) {
            throw new IOException("Failed to convert XML to JSON", e);
        }
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return jsonDeserializer.getProducedType();
    }
}
