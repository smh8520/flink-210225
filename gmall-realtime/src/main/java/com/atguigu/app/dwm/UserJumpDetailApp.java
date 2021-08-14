package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSONAware;
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
 * @create 2021-07-31 16:17
 */
public class UserJumpDetailApp {
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
        SingleOutputStreamOperator<JSONObject> jsonDStream = dataStreamSource.map(JSONObject::parseObject)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                }));

        KeyedStream<JSONObject, String> keyedStream = jsonDStream.keyBy(l -> l.getJSONObject("common").getString("mid"));

        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start")
                .where(new IterativeCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject, Context<JSONObject> context) throws Exception {
                        return jsonObject.getJSONObject("page").getString("last_page_id") == null;
                    }
                }).times(2).consecutive().within(Time.seconds(10));

        PatternStream<JSONObject> pattern1= CEP.pattern(keyedStream, pattern);

        SingleOutputStreamOperator<JSONObject> select = pattern1.select(new OutputTag<JSONObject>("withIn") {
                                                                        },
                new PatternTimeoutFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                        return map.get("start").get(0);
                    }
                },
                new PatternSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                        return map.get("start").get(0);
                    }
                });

        DataStream<JSONObject> sideOutput = select.getSideOutput(new OutputTag<JSONObject>("withIn") {
        });

        SingleOutputStreamOperator<String> map = select.union(sideOutput).map(JSONAware::toJSONString);
        map.addSink(MyKafkaUtils.getFlinkKafkaProducer(sinkTopic));
        map.print();
        env.execute();
    }
}
