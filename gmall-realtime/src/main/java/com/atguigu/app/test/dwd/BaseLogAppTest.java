package com.atguigu.app.test.dwd;

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
 * @create 2021-07-28 18:22
 */
public class BaseLogAppTest {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);

        //设置checkpoint,状态后端
//        env.setStateBackend(new FsStateBackend(""));
//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);

        //从kafka中读取数据
        DataStreamSource<String> dataStreamSource = env.addSource(MyKafkaUtils.getFlinkKafkaConsumer("ods_base_log", "test"));

        //将数据转为jsonObject,并过滤掉脏数据,将脏数据输出到侧流中
        SingleOutputStreamOperator<JSONObject> jsonStream = dataStreamSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(new OutputTag<String>("dirty") {
                    }, value);
                }
            }
        });
        //将脏数据从侧流中输出
        jsonStream.getSideOutput(new OutputTag<String>("dirty"){}).print("dirty");

        //使用状态编程来判断用户是不是新用户
        KeyedStream<JSONObject, String> keyedStream = jsonStream.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<JSONObject> jsonWithNewStream = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            //声明状态
            private ValueState<String> flag;

            @Override
            public void open(Configuration parameters) throws Exception {
                //初始化
                flag = getRuntimeContext().getState(new ValueStateDescriptor<String>("flag", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                //获取is_new字段
                String isNew = value.getJSONObject("common").getString("is_new");
                if ("1".equals(isNew)) {
                    if (flag.value() != null) {
                        //如果状态不是null,则说明这条数据并不是真正的新用户,则将is_new修改为0
                        value.getJSONObject("common").put("is_new", "0");
                    } else {
                        //如果状态是null,则此用户是新用户,同时给状态赋值.
                        flag.update("0");
                    }
                }
                return value;
            }
        });
        //创建侧流
        OutputTag<String> displayTag = new OutputTag<String>("display") {};
        OutputTag<String> startTag = new OutputTag<String>("start") {};
        //将不同类型的数据放入不同的侧流中.
        SingleOutputStreamOperator<String> pageStream = jsonWithNewStream.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                //先查看数据是否是启动数据
                String start = value.getString("start");
                if (start != null && start.length() > 0) {
                    //如果是启动数据,则将数据写入start侧流
                    ctx.output(startTag, value.toJSONString());
                } else {
                    //否则就是页面数据,将页面数据从主流输出
                    out.collect(value.toJSONString());

                    //判断是否是display数据
                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        //获取pageID
                        String pageId = value.getJSONObject("page").getString("page_id");

                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("page_id", pageId);
                            //将display数据写入到曝光侧流中
                            ctx.output(displayTag, display.toJSONString());
                        }
                    }

                }
            }
        });

        //获取侧输出流
        DataStream<String> startStream = pageStream.getSideOutput(startTag);
        DataStream<String> displayStream = pageStream.getSideOutput(displayTag);

        //将数据写入kafka对应的topic
        startStream.addSink(MyKafkaUtils.getFlinkKafkaProducer("dwd_start_log"));
        displayStream.addSink(MyKafkaUtils.getFlinkKafkaProducer("dwd_display_log"));
        pageStream.addSink(MyKafkaUtils.getFlinkKafkaProducer("dwd_page_log"));

        //开启任务
        env.execute();
    }
}
