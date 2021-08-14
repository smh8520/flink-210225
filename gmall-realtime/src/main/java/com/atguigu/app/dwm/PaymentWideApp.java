package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.OrderWide;
import com.atguigu.bean.PaymentInfo;
import com.atguigu.bean.PaymentWide;
import com.atguigu.utils.MyKafkaUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * @author smh
 * @create 2021-08-02 11:49
 */
public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME","atguigu");
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
////        设置checkpoint,设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/checkpoint/"));
//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";
        //读取dwd层的支付信息
        DataStreamSource<String> paymentInfoStream = env.addSource(MyKafkaUtils.getFlinkKafkaConsumer(paymentInfoSourceTopic, groupId));
        //读取dwm层的orderWide信息
        DataStreamSource<String> orderWideSource = env.addSource(MyKafkaUtils.getFlinkKafkaConsumer(orderWideSourceTopic, groupId));
        //转化为样例类并设置事件时间
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SingleOutputStreamOperator<PaymentInfo> paymentInfoSingleOutputStreamOperator = paymentInfoStream.map(value -> {
            PaymentInfo paymentInfo = JSONObject.parseObject(value, PaymentInfo.class);
            return paymentInfo;
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                    @Override
                    public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                        try {
                            return sdf.parse(element.getCreate_time()).getTime();
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                        return recordTimestamp;
                    }
                }));
        SingleOutputStreamOperator<OrderWide> orderWideSingleOutputStreamOperator = orderWideSource.map(value -> {
            OrderWide orderWide = JSONObject.parseObject(value, OrderWide.class);
            return orderWide;
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                    @Override
                    public long extractTimestamp(OrderWide element, long recordTimestamp) {

                        try {
                            return sdf.parse(element.getCreate_time()).getTime();
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                        return recordTimestamp;
                    }
                }));
        SingleOutputStreamOperator<PaymentWide> paymentWideStream = orderWideSingleOutputStreamOperator.keyBy(OrderWide::getOrder_id).intervalJoin(paymentInfoSingleOutputStreamOperator.keyBy(PaymentInfo::getOrder_id))
                .between(Time.seconds(0), Time.seconds(15 * 60))
                .process(new ProcessJoinFunction<OrderWide, PaymentInfo, PaymentWide>() {
                    @Override
                    public void processElement(OrderWide orderWide, PaymentInfo paymentInfo, Context ctx, Collector<PaymentWide> out) throws Exception {
                        PaymentWide paymentWide = new PaymentWide(paymentInfo, orderWide);
                        out.collect(paymentWide);
                    }
                });
        SingleOutputStreamOperator<String> map = paymentWideStream.map(JSONObject::toJSONString);
        map.print("payment>>>>>>>>>>>");
        map.addSink(MyKafkaUtils.getFlinkKafkaProducer(paymentWideSinkTopic));

        env.execute();
    }
}
