package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtils;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 求日访客量
 * @author smh
 * @create 2021-07-30 11:33
 */
public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
//        System.setProperty("HADOOP_USER_NAME","atguigu");
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        设置checkpoint,设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/checkpoint/"));
//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);

        //读取kafka的dwd_page_log主题的数据,并将数据转化为json字符串
        String sourceTopic="dwd_page_log";
        String groupID="UniqueVisitApp0225";
        String sinkTopic="dwm_unique_visit";

        DataStreamSource<String> dataStreamSource = env.addSource(MyKafkaUtils.getFlinkKafkaConsumer(sourceTopic, groupID));
        SingleOutputStreamOperator<JSONObject> jsonDStream = dataStreamSource.map(JSONObject::parseObject);

        //对数据按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonDStream.keyBy(value -> value.getJSONObject("common").getString("mid"));

        //对数据进行过滤,求日访客量,以每个访客每天第一次登录为主
        SingleOutputStreamOperator<JSONObject> uvStream = keyedStream.filter(new RichFilterFunction<JSONObject>() {
            private ValueState<String> visitState;
            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stringValueStateDescriptor = new ValueStateDescriptor<String>("uv-state", String.class);

                StateTtlConfig build = new StateTtlConfig.Builder(Time.hours(24)).setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build();

                stringValueStateDescriptor.enableTimeToLive(build);

                visitState = getRuntimeContext().getState(stringValueStateDescriptor);

                sdf = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                //按天设置状态,以last_page_id为null作为登录标准
                String lastId = value.getJSONObject("page").getString("last_page_id");

                String currentDate = sdf.format(new Date(value.getLong("ts")));

                if (lastId == null) {
                    //查看状态是否为null,或是否是同一天的登录
                    if (visitState.value() == null || !visitState.value().equals(currentDate)) {
                        //状态为null或日期与状态存的日期不同,说明是当天第一次登录,此时,更新状态
                        visitState.update(currentDate);
                        return true;
                    } else {
                        //不是第一次登录,过滤掉
                        return false;
                    }

                } else {
                    return false;
                }
            }
        });
        uvStream.print();
        //将过滤后的结果发送到kafka对应的主题中 dwm_unique_visit
        uvStream.map(JSONAware::toJSONString).addSink(MyKafkaUtils.getFlinkKafkaProducer(sinkTopic));
        //启动程序
        env.execute();
    }
}
