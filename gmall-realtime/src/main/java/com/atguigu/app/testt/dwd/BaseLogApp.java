package com.atguigu.app.testt.dwd;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author smh
 * @create 2021-07-31 16:49
 */
public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从ods_base_log读取数据
        DataStreamSource<String> dataStreamSource = env.addSource(MyKafkaUtils.getFlinkKafkaConsumer("ods_base_log", "testtOds"));



        //去除脏数据,脏数据输出到侧流中
        OutputTag<String> dirtyTag = new OutputTag<String>("dirty") {
        };
        SingleOutputStreamOperator<JSONObject> process = dataStreamSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    //如果能正常解析为json,则正常输出
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    //否则输出到脏数据流中
                    ctx.output(dirtyTag, value);
                }
            }
        });
        //输出帐数据
        process.getSideOutput(dirtyTag).print("脏数据>>>>>");

        //判断是否真的是第一次登录
        //以mid分组
        KeyedStream<JSONObject, String> keyedStream = process.keyBy(x -> x.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> map = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> flag;

            @Override
            public void open(Configuration parameters) throws Exception {
                flag = getRuntimeContext().getState(new ValueStateDescriptor<String>("isNew", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                String isNew = flag.value();

                if (isNew == null) {
                    flag.update("0");
                } else {
                    value.getJSONObject("common").put("is_new", 0);
                }
                return value;
            }
        });

        //分流
        OutputTag<String> startTag = new OutputTag<String>("start") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("display") {
        };
        SingleOutputStreamOperator<String> pageStream = map.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                if (value.getJSONObject("start") != null) {
                    ctx.output(startTag, value.toJSONString());
                } else {
                    out.collect(value.toJSONString());

                    JSONArray displays = value.getJSONArray("displays");

                    if (displays != null && displays.size() > 0) {

                        String pageId = value.getJSONObject("page").getString("page_id");

                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("page_id", pageId);
                            ctx.output(displayTag, display.toJSONString());
                        }
                    }
                }
            }
        });


        //输出
        DataStream<String> displayStream = pageStream.getSideOutput(displayTag);
        DataStream<String> startStream = pageStream.getSideOutput(startTag);

        pageStream.print("页面");
        displayStream.print("曝光");
        startStream.print("启动");

        pageStream.addSink(MyKafkaUtils.getFlinkKafkaProducer("dwd_page_log"));
        displayStream.addSink(MyKafkaUtils.getFlinkKafkaProducer("dwd_display_log"));
        startStream.addSink(MyKafkaUtils.getFlinkKafkaProducer("dwd_start_log"));


        env.execute();
    }
}
