package com.gmh.cdc;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

public class CustomerStringDebeziumDeserializationSchema implements DebeziumDeserializationSchema<JSONObject> {

/**
     * 变为一个JSON格式
     * {
     *     "database":"",
     *     "tableName":"",
     *     "operate":"",
     *     // 修改之前的数据
     *     "before":{
     *
     *     },
     *     // 修改之后的数据
     *     "after":{
     *
     *     }
     * }
    **/

    public void deserialize(SourceRecord sourceRecord, Collector<JSONObject> collector) throws Exception {
        // 1. 创建JSON对象
        JSONObject result = new JSONObject();
        // 2. 获取库名&表名
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        String database = fields[1];
        String tableName = fields[2];

        Struct struct = (Struct) sourceRecord.value();
        // 3. 获取before数据
        Struct before = struct.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        if(before != null) {
            Schema beforeSchema = before.schema();
            for (Field field : beforeSchema.fields()) {
                Object beforeValue = before.get(field);
                beforeJson.put(field.name(), beforeValue);
            }
        }
        // 4. 获取after数据
        Struct after = struct.getStruct("after");
        JSONObject afterJson = new JSONObject();
        if(after != null) {
            Schema afterSchema = after.schema();
            for (Field field : afterSchema.fields()) {
                Object afterValue = after.get(field);
                afterJson.put(field.name(), afterValue);
            }
        }
        // 5. 获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String opName = operation.toString().toLowerCase();
        // 为后续方便转一下
        if("create".equals(opName)){
            opName = "insert";
        }
        // 6. 将字段写入JSON对象
        result.put("database",database);
        result.put("tableName",tableName);
        result.put("before",beforeJson);
        result.put("after",afterJson);
        result.put("operate",opName);
        // 7. 输出数据
        collector.collect(result);
    }

    // 和 StringDebeziumDeserializationSchema 保持一致
    public TypeInformation<JSONObject> getProducedType() {
        return BasicTypeInfo.of(JSONObject.class);
    }

}
