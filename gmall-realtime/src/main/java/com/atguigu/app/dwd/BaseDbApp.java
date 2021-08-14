package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.app.function.DimSinkFunction;
import com.atguigu.app.function.MyDeserializationSchema;
import com.atguigu.app.function.TableProcessFunction;
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
 * @create 2021-07-28 19:03
 */
public class BaseDbApp {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME","atguigu");
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
////        设置checkpoint,设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/checkpoint/"));
//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);

        //读取kafka中ods_base_db的数据
        DataStreamSource<String> dataStreamSource = env.addSource(MyKafkaUtils.getFlinkKafkaConsumer("ods_base_db", "dwd_db"));

        //过滤数据并将数据转化为json格式
        SingleOutputStreamOperator<JSONObject> jsonStream = dataStreamSource.map(JSONObject::parseObject).filter(x -> !"delete".equals(x.getString("type")));

        jsonStream.print();

        //创建flinkCDC,通过cdc来监控mysql中配置表的数据变化
        DebeziumSourceFunction<String> build = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall_realtime")
                .tableList("gmall_realtime.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyDeserializationSchema())
                .build();
        DataStreamSource<String> tableProcessStream = env.addSource(build);
//        tableProcessStream.print();

        MapStateDescriptor<String, TableProcess> stringTableProcessMapStateDescriptor =
                new MapStateDescriptor<>("table-process-state", String.class, TableProcess.class);
        //将流变成广播流
        BroadcastStream<String> broadcast = tableProcessStream.
                broadcast(stringTableProcessMapStateDescriptor);
//
        //主流和广播流进行connect
        BroadcastConnectedStream<JSONObject, String> connect = jsonStream.connect(broadcast);


//
        SingleOutputStreamOperator<JSONObject> kafkaStream = connect.process(new TableProcessFunction(stringTableProcessMapStateDescriptor));


//
        DataStream<JSONObject> hbaseStream = kafkaStream.getSideOutput(new OutputTag<JSONObject>("hbase") {});

        kafkaStream.print("kafka>>>>>>>");
        hbaseStream.print("hbase>>>>>>>");
        //数据插入到hbase的表中
        hbaseStream.addSink(new DimSinkFunction());
        //数据输出到kafka对应的主题中
        kafkaStream.addSink(MyKafkaUtils.getFlinkKafkaProducer(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                return new ProducerRecord<>(jsonObject.getString("sinkTable"),
                        jsonObject.getString("data").getBytes());
            }
        }));

        env.execute();
    }
}
