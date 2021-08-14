package com.atguigu.app.dws;

import com.atguigu.app.function.MyUDTF;
import com.atguigu.bean.KeywordStats;
import com.atguigu.common.GmallConstant;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.MyKafkaUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author smh
 * @create 2021-08-06 17:56
 */
public class KeywordStatsApp {
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
        //读取数据
        String groupId = "keyword_stats_app";
        String pageViewSourceTopic ="dwd_page_log";
        //创建表
        tableEnv.executeSql("  create table page_view( " +
                "      `page` MAP<STRING,STRING>, " +
                "      `ts` BIGINT, " +
                "      `rt` as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000,'yyyy-MM-dd HH:mm:ss')), " +
                "      WATERMARK for rt as rt - INTERVAL '1' SECOND " +
                "    )"+ MyKafkaUtils.getSqlWith(pageViewSourceTopic,groupId));



        Table fullWordTable = tableEnv.sqlQuery("select  " +
                "    page['item'] full_word,  " +
                "    rt  " +
                "  from page_view  " +
                "  where page['item_type']='keyword' and page['item'] is not null");

        tableEnv.createTemporarySystemFunction("splitFunction", MyUDTF.class);

        Table table = tableEnv.sqlQuery("  select  " +
                "    word,  " +
                "    rt  " +
                "  from " + fullWordTable +
                "  ,LATERAL TABLE(splitFunction(full_word))");

        Table resultTable = tableEnv.sqlQuery("  select    " +
                "    word keyword,    " +
                "    count(*) ct,    " +
                "    '"+ GmallConstant.KEYWORD_SEARCH +"' source,    " +
                "    DATE_FORMAT(TUMBLE_START(rt,INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') as stt,    " +
                "    DATE_FORMAT(TUMBLE_END(rt,INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') as edt,    " +
                "    UNIX_TIMESTAMP() as ts    " +
                "  from "+table +
                "  group by    " +
                "    word,    " +
                "    TUMBLE(rt,INTERVAL '10' SECOND)");

        DataStream<KeywordStats> keywordStatsAppDataStream = tableEnv.toAppendStream(resultTable, KeywordStats.class);

        keywordStatsAppDataStream.print();

        keywordStatsAppDataStream.addSink(ClickHouseUtil.getSink("insert into keyword_stats_2021(keyword,ct,source,stt,edt,ts)values(?,?,?,?,?,?)"));

        env.execute();
    }
}
