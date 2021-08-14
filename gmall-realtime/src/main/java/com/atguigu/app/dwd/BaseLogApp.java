package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtils;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


/*
日志数据->nginx->spring boot->kafka(ods层)-->flinkCdc-->对数据操作-->kafka(dwd层)

需要启动的软件
mock日志生成jar包->nginx->logger.sh->kafka(zk)->BaseLogApp
 */

/**
 * @author smh
 * @create 2021-07-28 11:56
 */
public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);
        //设置状态后端和checkpoint
//        env.enableCheckpointing(5000l, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        //设置状态后端模式
//        env.setStateBackend(new FsStateBackend(""));

        //从kafka中读取ods层的log数据
        DataStreamSource<String> kafkaDStream = env.addSource(MyKafkaUtils.getFlinkKafkaConsumer("ods_base_log", "gmall-flink"));

        //将数据转化为json对象
        //创建侧流,将脏数据输出到侧流中
        /*
        创建侧流对象的时候要加上{}来创建它的匿名实现类对象.
        因为如果直接创建侧流对象的话,它是一个泛型类,会对泛型进行擦除而无法推断出正确的类型.
         */

        //将数据转为json对象,并过滤脏数据
        //创建侧输出流
        OutputTag<String> dirtyTag = new OutputTag<String>("dirty") {};
        SingleOutputStreamOperator<JSONObject> jsonStream = kafkaDStream.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {

                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(dirtyTag, value);
                }
            }
        });
        jsonStream.getSideOutput(dirtyTag).print("脏数据>>>>>>");
        //对数据进行分组,根据状态判断是否是新用户
        KeyedStream<JSONObject, String> keyedStream = jsonStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("mid");
            }
        });

        SingleOutputStreamOperator<JSONObject> isNewStream = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> flag;

            @Override
            public void open(Configuration parameters) throws Exception {
                flag = getRuntimeContext().getState(new ValueStateDescriptor<String>("isNew", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                String isNew = value.getJSONObject("common").getString("is_new");
                if ("1".equals(isNew)) {
                    //如果状态不是null,则说明不是新用户,将is_new的1改为0
                    if (flag.value() != null) {
                        value.getJSONObject("common").put("is_new", "0");
                    } else {
                        flag.update("0");
                    }
                }

                return value;
            }
        });
        //创建侧流
        OutputTag<String> startTag = new OutputTag<String>("start"){};
        OutputTag<String> displayTag = new OutputTag<String>("display"){};
        //将不同类型的日志数据输入到不同的kafka主题中
        SingleOutputStreamOperator<String> pageStream = isNewStream.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                String start = value.getString("start");
                if (start != null && start.length() > 0) {
                    //将启动日志加入到启动侧流中
                    ctx.output(startTag, value.toJSONString());
                } else {
                    //否则的话就是页面数据,页面数据放到主流中输出
                    out.collect(value.toJSONString());
                    //获取曝光数据
                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        //获取页面id
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
