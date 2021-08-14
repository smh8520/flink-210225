package com.atguigu.app.test.dwm;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.test.fun.DimAsyncTestttUtil;
import com.atguigu.bean.OrderDetail;
import com.atguigu.bean.OrderInfo;
import com.atguigu.bean.OrderWide;
import com.atguigu.utils.MyKafkaUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

/**
 * @author smh
 * @create 2021-08-01 23:03
 */
public class DwmOrderWideTestApp {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //        设置checkpoint,设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/checkpoint/"));
//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);

        //读取kafka的dwd_page_log主题的数据,并将数据转化为json字符串
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group";
        //读取数据
        DataStreamSource<String> dataStreamSource = env.addSource(MyKafkaUtils.getFlinkKafkaConsumer(orderInfoSourceTopic, groupId));
        DataStreamSource<String> dataStreamSource1 = env.addSource(MyKafkaUtils.getFlinkKafkaConsumer(orderDetailSourceTopic, groupId));
        //转换为样例类并设置waterMark
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        //订单流
        SingleOutputStreamOperator<OrderInfo> orderInfoStream = dataStreamSource.map(value -> {
            OrderInfo orderInfo = JSONObject.parseObject(value, OrderInfo.class);
            String create_time = orderInfo.getCreate_time();
            String[] s = create_time.split(" ");
            //设置创建日期,小时
            orderInfo.setCreate_date(s[0]);
            orderInfo.setCreate_hour(s[1].split(":")[0]);
            //解析时间
            long time = sdf.parse(create_time).getTime();
            orderInfo.setCreate_ts(time);
            return orderInfo;
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                    @Override
                    public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                }));
        SingleOutputStreamOperator<OrderDetail> orderDetailStream = dataStreamSource1.map(value -> {
            OrderDetail orderDetail = JSONObject.parseObject(value, OrderDetail.class);
            String create_time = orderDetail.getCreate_time();
            long time = sdf.parse(create_time).getTime();
            orderDetail.setCreate_ts(time);
            return orderDetail;
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                    @Override
                    public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                }));
        SingleOutputStreamOperator<OrderWide> orderWideStream = orderInfoStream.keyBy(OrderInfo::getId).intervalJoin(orderDetailStream.keyBy(OrderDetail::getOrder_id))
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo left, OrderDetail right, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(left, right));
                    }
                });

        AsyncDataStream.unorderedWait(orderWideStream,
                new DimAsyncTestttUtil<OrderWide>("") {
                    @Override
                    public void merge(OrderWide input, JSONObject info) {
                        input.setUser_gender(info.getString("user_gender"));

                        try {
                            long birthday = sdf.parse(info.getString("birthday")).getTime();
                            input.setUser_age((int)(System.currentTimeMillis()-birthday)
                            /1000/60/60/24/365);
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                    }

                    @Override
                    public String getId(OrderWide input) {
                        return input.getUser_id().toString();
                    }
                },60, TimeUnit.SECONDS);
    }
}
