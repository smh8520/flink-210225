package com.atguigu.app.testt.dwm;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author smh
 * @create 2021-07-31 18:04
 */
public class Dwm_Jump_Detail {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        设置checkpoint,设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/checkpoint/"));
//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);

        //读取kafka的dwd_page_log主题的数据,并将数据转化为json字符串
        String sourceTopic="dwd_page_log";
        String groupID="UserJumpDetailApp0225";
        String sinkTopic="dwm_user_jump_detail";

        DataStreamSource<String> dataStreamSource = env.addSource(MyKafkaUtils.getFlinkKafkaConsumer(sourceTopic, groupID));
        //转为json
        SingleOutputStreamOperator<JSONObject> jsonStream = dataStreamSource.map(JSONObject::parseObject);
        //设置waterMark,指定事件时间字段
        jsonStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(1))
        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("ts");
            }
        }));

        //分组
        KeyedStream<JSONObject, String> keyedStream = jsonStream.keyBy(x -> x.getJSONObject("common").getString("mid"));
        //编写序列模式
        Pattern<JSONObject, JSONObject> within = Pattern.<JSONObject>begin("start")
                .where(new IterativeCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject, Context<JSONObject> context) throws Exception {
                        return jsonObject.getJSONObject("page").getString("last_page_id") == null;
                    }
                }).next("next")
                .where(new IterativeCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject, Context<JSONObject> context) throws Exception {
                        return jsonObject.getJSONObject("page").getString("last_page_id") == null;
                    }
                }).within(Time.seconds(10));

        PatternStream<JSONObject> pattern = CEP.pattern(keyedStream, within);

        SingleOutputStreamOperator<String> select = pattern.select(new OutputTag<String>("withIn") {
                                                                   },
                (PatternTimeoutFunction<JSONObject, String>) (map, l) -> map.get("start").get(0).toJSONString(),
                (PatternSelectFunction<JSONObject, String>) map -> map.get("start").get(0).toJSONString());

        DataStream<String> sideOutput = select.getSideOutput(new OutputTag<String>("withIn") {});

        select.union(sideOutput).addSink(MyKafkaUtils.getFlinkKafkaProducer(sinkTopic));

        env.execute();
    }
}
