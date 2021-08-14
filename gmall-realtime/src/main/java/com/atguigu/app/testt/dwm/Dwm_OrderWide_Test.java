package com.atguigu.app.testt.dwm;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.testt.fun.DimAsyncTestFunction;
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
 * @create 2021-08-01 19:21
 */
public class Dwm_OrderWide_Test {
    public static void main(String[] args) throws Exception {
        //TODO 获取执行环境
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
        //订单明细流
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
        //两个流以orderId进行分组,再join拼接,并设置乱序时间
        SingleOutputStreamOperator<OrderWide> orderWideStream = orderInfoStream.keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailStream.keyBy(OrderDetail::getOrder_id))
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });
        //关联维度数据,补全orderWide信息(六个维度)
        /*
        优化二
        由于orderWide需要与六个维度表进行关联,如果使用同步查询的话,在一条数据查询完之前,会阻塞.到本条数据完成之后
        下一条数据的查询才会开始.
        所以我们要使用异步查询的方式,在一条数据查询的时候不会阻塞其他数据查询.这样可以加快查询效率.
        这里使用的是线程池的方式来实现异步查询的.
         */
        //连接用户维度表
        AsyncDataStream.unorderedWait(orderWideStream, new DimAsyncTestFunction<OrderWide>("") {
            @Override
            public void merge(OrderWide input, JSONObject dimInfo) {
                //设置性别
                input.setUser_gender(dimInfo.getString("user_gender"));
                try {
                    //设置年龄
                    input.setUser_age((int)(System.currentTimeMillis()-
                    sdf.parse(dimInfo.getString("birthday")).getTime())/1000/60/60/24/365);
                } catch (ParseException e) {
                    e.printStackTrace();
                }

            }

            @Override
            public String getId(OrderWide input) {
                return input.getUser_id().toString();
            }
        },60, TimeUnit.SECONDS);


        //执行任务
        env.execute();
    }
}
