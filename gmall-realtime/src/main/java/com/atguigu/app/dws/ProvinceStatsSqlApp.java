package com.atguigu.app.dws;

import com.atguigu.bean.ProvinceStats;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.MyKafkaUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author smh
 * @create 2021-08-06 11:19
 */
public class ProvinceStatsSqlApp {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
////        设置checkpoint,设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/checkpoint/"));
//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        System.setProperty("HADOOP_USER_NAME","atguigu");

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //创建表
        String groupId = "province_stats";
        String orderWideTopic = "dwm_order_wide";

        tableEnv.executeSql("CREATE TABLE order_wide (  " +
                "  `province_id` BIGINT,  " +
                "  `province_name` STRING,  " +
                "  `province_area_code` STRING,  " +
                "  `province_iso_code` STRING,  " +
                "  `province_3166_2_code` STRING,  " +
                "  `create_time` STRING,  " +
                "  `order_id` BIGINT,  " +
                "  `total_amount` DOUBLE,  " +
                "  `rt` as TO_TIMESTAMP(create_time),  " +
                "  WATERMARK FOR rt AS rt - INTERVAL '2' SECOND  " +
                ")"+ MyKafkaUtils.getSqlWith(orderWideTopic,groupId));



        //sql查询
        Table table = tableEnv.sqlQuery("select " +
                "  DATE_FORMAT(TUMBLE_START(rt,INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') as stt, " +
                "  DATE_FORMAT(TUMBLE_END(rt,INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') as edt, " +
                "  province_id, " +
                "  province_name, " +
                "  province_area_code, " +
                "  province_iso_code, " +
                "  province_3166_2_code, " +
                "  sum(total_amount) as order_amount, " +
                "  count(distinct order_id) as order_count , " +
                "  UNIX_TIMESTAMP() as ts  " +
                "from order_wide " +
                "group by  " +
                "  province_id, " +
                "  province_name, " +
                "  province_area_code, " +
                "  province_iso_code, " +
                "  province_3166_2_code, " +
                "  TUMBLE(rt,INTERVAL '10' SECOND)");

        DataStream<ProvinceStats> result = tableEnv.toAppendStream(table, ProvinceStats.class);


        //转化为流输出到clickHouse
        result.addSink(ClickHouseUtil.getSink("insert into province_stats_2021 values(?,?,?,?,?,?,?,?,?,?)"));

        //执行程序
        env.execute();
    }
}
