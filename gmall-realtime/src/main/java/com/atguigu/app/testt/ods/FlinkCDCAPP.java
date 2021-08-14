package com.atguigu.app.testt.ods;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.app.testt.fun.MyDeseSchema;
import com.atguigu.utils.MyKafkaUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author smh
 * @create 2021-07-31 16:40
 */
public class FlinkCDCAPP {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DebeziumSourceFunction<String> build = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall_flink")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyDeseSchema())
                .build();

        DataStreamSource<String> dataStreamSource = env.addSource(build);

        dataStreamSource.addSink(MyKafkaUtils.getFlinkKafkaProducer("ods_base_db"));

        env.execute();
    }
}
