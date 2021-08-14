package com.atguigu.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.VisitorStats;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.MyKafkaUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

/**
 * @author smh
 * @create 2021-08-02 18:18
 */
public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME","atguigu");
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
////        设置checkpoint,设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/checkpoint/"));
//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);

        //访客主题 求pv,uv,跳出,进入app数,一次会话的页面停留时长
        //uv有dwm层的dwm_user_visit主题 跳出有dwm层的dwm_unique_visit,其余指标可以有dwd_page_log来获取
        //读取三个流的数据转化为样例类,使用补零union的方式将数据结合起来
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";
        String groupId = "visitor_stats_app";

        //读取转化uv数据
        SingleOutputStreamOperator<VisitorStats> visitStateWithUvStream = env.addSource(MyKafkaUtils.getFlinkKafkaConsumer(uniqueVisitSourceTopic, groupId))
                .map(value -> {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    JSONObject common = jsonObject.getJSONObject("common");
                    return new VisitorStats("", "", common.getString("vc"),
                            common.getString("ch"), common.getString("ar"), common.getString("is_new")
                            , 1L, 0L, 0L, 0L, 0L, jsonObject.getLong("ts"));
                });
        //读取转化跳出数据
        SingleOutputStreamOperator<VisitorStats> visitStateWithUJStream = env.addSource(MyKafkaUtils.getFlinkKafkaConsumer(userJumpDetailSourceTopic, groupId))
                .map(value -> {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    JSONObject common = jsonObject.getJSONObject("common");
                    return new VisitorStats("", "", common.getString("vc"),
                            common.getString("ch"), common.getString("ar"), common.getString("is_new")
                            , 0L, 0L, 0L, 1L, 0L, jsonObject.getLong("ts"));

                });

        SingleOutputStreamOperator<VisitorStats> visitStatePageStream = env.addSource(MyKafkaUtils.getFlinkKafkaConsumer(pageViewSourceTopic, groupId))
                .map(value -> {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    JSONObject common = jsonObject.getJSONObject("common");
                    JSONObject page = jsonObject.getJSONObject("page");
                    long sv = 0L;
                    if (page.getString("last_page_id") == null) {
                        sv = 1L;
                    }
                    return new VisitorStats("", "", common.getString("vc"),
                            common.getString("ch"), common.getString("ar"), common.getString("is_new")
                            , 0L, 0L, sv, 0L, page.getLong("during_time"), jsonObject.getLong("ts"));

                });

        //union三个流的数据
        DataStream<VisitorStats> visitStateStream = visitStatePageStream.union(visitStateWithUJStream, visitStateWithUvStream);
        //指定事件时间,waterMark
        SingleOutputStreamOperator<VisitorStats> visitorStatsSingleOutputStreamOperator = visitStateStream.assignTimestampsAndWatermarks(WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                    @Override
                    public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));

        //开窗 由于过滤单跳数据的时候,过期时间为10s,所以这里的窗口要设置10s的乱序时间
        KeyedStream<VisitorStats, String> visitorStatsStringKeyedStream = visitorStatsSingleOutputStreamOperator.keyBy(x -> {
            String s = x.getAr() + x.getCh() + x.getIs_new() + x.getVc();
            return s;
        });

        SingleOutputStreamOperator<VisitorStats> reduce = visitorStatsStringKeyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<VisitorStats>() {
                    @Override
                    public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                        return new VisitorStats(value1.getStt(), value1.getEdt(), value1.getVc(),
                                value1.getCh(), value1.getAr(), value1.getIs_new(), value1.getUv_ct() + value2.getUv_ct()
                                , value1.getPv_ct() + value2.getPv_ct(), value1.getSv_ct() + value2.getSv_ct(),
                                value1.getUj_ct() + value2.getUj_ct(), value1.getDur_sum() + value2.getDur_sum(), value1.getTs());
                    }
                }, new WindowFunction<VisitorStats, VisitorStats, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<VisitorStats> input, Collector<VisitorStats> out) throws Exception {
                        VisitorStats next = input.iterator().next();
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        next.setStt(sdf.format(new Date(window.getStart())));
                        next.setEdt(sdf.format(new Date(window.getEnd())));
                        out.collect(next);
                    }
                });
        reduce.print();
        //将数据写到clickHouse
        reduce.addSink(ClickHouseUtil.<VisitorStats>getSink("insert into visitor_stats_20210225 values(?,?,?,?,?,?,?,?,?,?,?,?)"));
        env.execute();
    }
}
