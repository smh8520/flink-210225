package com.atguigu.app.testt.dwm;

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
 * @author smh
 * @create 2021-07-31 17:51
 */
public class Dwm_Uv {
    public static void main(String[] args) {
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
        SingleOutputStreamOperator<JSONObject> map = dataStreamSource.map(JSONObject::parseObject);

        //分组
        KeyedStream<JSONObject, String> keyedStream = map.keyBy(x -> x.getJSONObject("common").getString("mid"));
        //过滤
        keyedStream.filter(new RichFilterFunction<JSONObject>() {
            private ValueState<String> flag;
            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> uv = new ValueStateDescriptor<>("uv", String.class);
                //设置状态的生命周期为24H,刷新方式为创建或更新
                StateTtlConfig build = new StateTtlConfig.Builder(Time.hours(24)).setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build();
                uv.enableTimeToLive(build);
                flag=getRuntimeContext().getState(uv);
                sdf=new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                JSONObject page = value.getJSONObject("page");
                if(page.getString("last_page_id")!=null){
                    String currentDate = sdf.format(new Date(value.getLong("ts")));

                    if(flag.value()==null || !flag.value().equals(currentDate)){
                        flag.update(currentDate);
                        return true;
                    }else{
                        return false;
                    }
                }else{
                    return false;
                }

            }
        });
    }
}
