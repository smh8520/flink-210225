package com.atguigu.app.test.dwd;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.app.function.MyDeserializationSchema;
import com.atguigu.app.test.fun.DimHBaseSink;
import com.atguigu.app.test.fun.TableProcessTestFunction;
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
 * @create 2021-07-29 18:43
 */
public class BaseDbAppTest {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);

        //设置checkpoint,状态后端
//        env.setStateBackend(new FsStateBackend(""));
//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);

        //从kafka中读取主流数据
        DataStreamSource<String> dataStreamSource = env.addSource(MyKafkaUtils.getFlinkKafkaConsumer("ods_base_db", "realtime-test"));

        //将主流数据转为json格式
        SingleOutputStreamOperator<JSONObject> odsJsonDbStream = dataStreamSource.map(JSONObject::parseObject);
        //从mysql中读取广播流数据
        DebeziumSourceFunction<String> build = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall_realtime")
                .tableList("gmall_realtime.table_process")
                .deserializer(new MyDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();
        DataStreamSource<String> tableProcessStream = env.addSource(build);

        //将该流变为广播流
        MapStateDescriptor<String, TableProcess> stringTableProcessMapStateDescriptor = new MapStateDescriptor<>("test-broad", String.class, TableProcess.class);

        BroadcastStream<String> broadcast = tableProcessStream.broadcast(stringTableProcessMapStateDescriptor);

        //两个流进行connect,并对结果流进行操作
        BroadcastConnectedStream<JSONObject, String> connect = odsJsonDbStream.connect(broadcast);
        SingleOutputStreamOperator<JSONObject> kafkaStream = connect.process(new TableProcessTestFunction(stringTableProcessMapStateDescriptor));

        //获取分流结果
        DataStream<JSONObject> hbaseStream = kafkaStream.getSideOutput(new OutputTag<JSONObject>("hbase") {});

        //将不同的流发往不同的地方
        kafkaStream.print("kafka>>>>>");
        hbaseStream.print("hbase>>>>>");

        //将不同流的数据发往不同的目的地
        hbaseStream.addSink(new DimHBaseSink());

        //将数据写到kafka对应分区中
        kafkaStream.addSink(MyKafkaUtils.getFlinkKafkaProducer(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {

                return new ProducerRecord<>(jsonObject.getString("sinkTable"),
                        jsonObject.getString("data").getBytes());
            }
        }));
        //执行任务
        env.execute();
    }
}
