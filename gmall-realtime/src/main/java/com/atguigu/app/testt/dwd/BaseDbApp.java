package com.atguigu.app.testt.dwd;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.app.testt.fun.MyDeseSchema;
import com.atguigu.app.testt.fun.MySinkFunction;
import com.atguigu.app.testt.fun.TableFunction;
import com.atguigu.bean.TableProcess;
import com.atguigu.utils.MyKafkaUtils;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * @author smh
 * @create 2021-07-31 17:09
 */
public class BaseDbApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //读取主流数据并转化为JSON
        DataStreamSource<String> dataStreamSource = env.addSource(MyKafkaUtils.getFlinkKafkaConsumer("ods_base_db", "testtDbApp"));
        SingleOutputStreamOperator<JSONObject> map = dataStreamSource.map(JSONObject::parseObject);

        //读取广播流数据,并转化为广播流
        DebeziumSourceFunction<String> build = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall_realtime")
                .tableList("gmall_realtime.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyDeseSchema())
                .build();
        DataStreamSource<String> dataStreamSource1 = env.addSource(build);
        MapStateDescriptor<String, TableProcess> mapState = new MapStateDescriptor<>("testt", String.class, TableProcess.class);
        BroadcastStream<String> broadcast = dataStreamSource1.broadcast(mapState);

        //将两个流connect
        BroadcastConnectedStream<JSONObject, String> connect = map.connect(broadcast);

        SingleOutputStreamOperator<JSONObject> kafkaStream = connect.process(new TableFunction(mapState));

        DataStream<JSONObject> hbaseStream = kafkaStream.getSideOutput(new OutputTag<JSONObject>("hbase") {
        });

        kafkaStream.print("kafka>>>>>>>");
        hbaseStream.print("hbase>>>>>>>");

        //将数据发送到不同的目的地
        kafkaStream.addSink(MyKafkaUtils.getFlinkKafkaProducer(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                return new ProducerRecord<>(jsonObject.getString("sinkTable")
                ,jsonObject.getString("data").getBytes());
            }
        }));

        hbaseStream.addSink(new MySinkFunction());

        env.execute();
    }
}
