package com.gmh.cdc;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Objects;


/**
 * com.chuangke.flink
 *
 * @program: com.chuangke.flink
 * @description:
 * @author: fanzhe
 * @create: 2021-03-25 14:39
 **/
public class JsonDebeziumDeserializationSchema implements DebeziumDeserializationSchema<JSONObject> {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<JSONObject> collector) throws Exception {
        JSONObject result = new JSONObject();
        String topic = sourceRecord.topic();
        String[] split = StringUtils.split(topic, ".");
        String database = split[1];
        String table = split[2];
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        Struct struct = (Struct) sourceRecord.value();
        Struct after = struct.getStruct("after");

        if (Objects.isNull(after)) {
            return;
        }
        result.put("database", database);
        result.put("table", table);
        String type = operation.toString().toLowerCase();
        if ("create".equals(type)) {
            type = "insert";
        } else if ("update".equals(type)) {
            type = "update";
        } else if ("delete".equals(type)) {
            type = "delete";
            return;
        } else {
            type = "read";
            return;
        }
        result.put("type", type);
        JSONObject value = new JSONObject();
        Schema schema = after.schema();
        schema.fields().stream().filter(Objects::nonNull).forEach(s -> {
            if (!Objects.isNull(s.schema())
                    && !StringUtils.isEmpty(s.schema().name())
                    && s.schema().name().equals("io.debezium.time.Timestamp")) {
                Object obj = after.get(s.name());
                if (!Objects.isNull(obj)) {
                    LocalDateTime time = LocalDateTime.ofInstant(Instant.ofEpochMilli((long) obj), ZoneId.of("GMT"));
                    value.put(s.name(), time);
                }
            } else {
                value.put(s.name(), after.get(s.name()));

            }
        });
        result.put("data", value.toJSONString());
        collector.collect(result);
    }

    @Override
    public TypeInformation<JSONObject> getProducedType() {
        return BasicTypeInfo.of(JSONObject.class);
    }
}
