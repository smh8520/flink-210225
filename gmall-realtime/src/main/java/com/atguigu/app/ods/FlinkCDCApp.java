package com.atguigu.app.ods;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.app.function.MyDeserializationSchema;
import com.atguigu.utils.MyKafkaUtils;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;



/**
 * @author smh
 * @create 2021-07-28 9:57
 */
public class FlinkCDCApp {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME","atguigu");
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /*
        设置并行度.这里的并行度最好与kafka的分区数相同,如果比kafka分区数少的话就会降低效率.
        如果比kafka分区数大的话就会有并行度中没有数据.这样如果以事件时间来当时间线设置watermark
        ,空闲的并行度发送的watermark就会一直是long.minValue.而下游按最小的watermark做标准.
        这时如果开窗的话就会导致数据无法输出.
         */
        env.setParallelism(1);
//        //设置checkpoint
//        env.enableCheckpointing(5000l, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        //设置状态后端模式
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/checkpoint/"));

        //创建MysqlSource监控mysql中变化的数据
        DebeziumSourceFunction<String> build = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall_flink")
                .startupOptions(StartupOptions.latest())
                .deserializer(new MyDeserializationSchema())
                .build();

        DataStreamSource<String> dataStreamSource = env.addSource(build);
        //将数据写入kafka
        dataStreamSource.print();
        dataStreamSource.addSink(MyKafkaUtils.getFlinkKafkaProducer("ods_base_db"));

        env.execute();
    }
}
