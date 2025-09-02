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
import org.json.JSONArray;
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
            normalizeItemArrays(jsonObject);
            byte[] jsonBytes = jsonObject.toString().getBytes(StandardCharsets.UTF_8);
            return jsonDeserializer.deserialize(jsonBytes);
        } catch (Exception e) {
            throw new IOException("Failed to convert XML to JSON", e);
        }
    }

    private void normalizeItemArrays(Object node) {
        if (node instanceof JSONObject) {
            JSONObject json = (JSONObject) node;
            if (json.has("item")) {
                Object item = json.get("item");
                if (item instanceof JSONObject) {
                    JSONArray array = new JSONArray();
                    array.put(item);
                    json.put("item", array);
                }
            }
            for (String key : json.keySet()) {
                normalizeItemArrays(json.get(key));
            }
        } else if (node instanceof JSONArray) {
            JSONArray array = (JSONArray) node;
            for (int i = 0; i < array.length(); i++) {
                normalizeItemArrays(array.get(i));
            }
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
