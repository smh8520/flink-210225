package com.atguigu.app.testt.fun;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * @author smh
 * @create 2021-07-31 16:44
 */
public class MyDeseSchema implements DebeziumDeserializationSchema<String> {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        //创建json字符串保存结果
        JSONObject result = new JSONObject();
        //获取库名,表名
        String[] split = sourceRecord.topic().split("\\.");
        String databaseName = split[1];
        String tableName = split[2];

        Struct value = (Struct) sourceRecord.value();
        //获取更新后的数据结构体
        Struct after = value.getStruct("after");
        JSONObject afterData = new JSONObject();

        //获取更新前的数据结构体
        Struct before = value.getStruct("before");
        JSONObject beforeData = new JSONObject();
        //封装更新后的数据
        if(after != null){
            Schema schema = after.schema();
            List<Field> fields = schema.fields();
            for (Field field : fields) {
                Object o = after.get(field);
                afterData.put(field.name(),o);
            }
        }
        //封装更新前的数据
        if(before != null){
            Schema schema = before.schema();
            List<Field> fields = schema.fields();
            for (Field field : fields) {
                Object o = before.get(field);
                beforeData.put(field.name(),o);
            }
        }

        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
        if("create".equals(type)){
            type="insert";
        }

        result.put("databaseName",databaseName);
        result.put("tableName",tableName);
        result.put("type",type);
        result.put("data",afterData);
        result.put("before",beforeData);

        collector.collect(result.toJSONString());

    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }


}
