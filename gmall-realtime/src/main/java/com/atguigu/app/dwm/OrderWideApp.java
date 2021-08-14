package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.function.DimAsyncFunction;
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
 * @create 2021-08-01 9:57
 */
public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        //TODO 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //        设置checkpoint,设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/checkpoint/"));
//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        //读取kafka的dwd_page_log主题的数据,并将数据转化为json字符串
        //TODO 从kafka中读取对应的数据 dwd_order_info 和 dwd_order_detail
        DataStreamSource<String> orderInfoStream = env.addSource(MyKafkaUtils.getFlinkKafkaConsumer("dwd_order_info", "OrderWideApp0227"));
        DataStreamSource<String> orderDetailStream = env.addSource(MyKafkaUtils.getFlinkKafkaConsumer("dwd_order_detail", "OrderWideApp0227"));
        //TODO 将数据封装为javaBean,并提取时间戳,设置waterMark和事件时间
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SingleOutputStreamOperator<OrderInfo> orderStream = orderInfoStream.map(value -> {
            OrderInfo orderInfo = JSONObject.parseObject(value, OrderInfo.class);
            String create_time = orderInfo.getCreate_time();
            String[] s = create_time.split(" ");
            orderInfo.setCreate_date(s[0]);
            orderInfo.setCreate_hour(s[1]);
            orderInfo.setCreate_ts(sdf.parse(orderInfo.getCreate_time()).getTime());
            return orderInfo;
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forMonotonousTimestamps()
        .withTimestampAssigner((SerializableTimestampAssigner<OrderInfo>) (element, recordTimestamp) -> element.getCreate_ts()));
        SingleOutputStreamOperator<OrderDetail> detailStream = orderDetailStream.map(value -> {
            OrderDetail orderDetail = JSONObject.parseObject(value, OrderDetail.class);
            String create_time = orderDetail.getCreate_time();
            orderDetail.setCreate_ts(sdf.parse(orderDetail.getCreate_time()).getTime());
            return orderDetail;
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forMonotonousTimestamps()
                .withTimestampAssigner((SerializableTimestampAssigner<OrderDetail>) (element, recordTimestamp) -> element.getCreate_ts()));
        //TODO 双流join
        SingleOutputStreamOperator<OrderWide> orderWideStream = orderStream.keyBy(OrderInfo::getId).intervalJoin(detailStream.keyBy(OrderDetail::getOrder_id))
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });
//        orderDetailStream.print("orderWide>>>>>");
        //TODO 读取维度信息
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS =
                AsyncDataStream.unorderedWait(orderWideStream, new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
            @Override
            public void mergeInfo(OrderWide input, JSONObject dimInfo) {
                //性别字段
                String user_gender = dimInfo.getString("GENDER");
                input.setUser_gender(user_gender);
                //年龄
                try {
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                    long birthday = simpleDateFormat.parse(dimInfo.getString("BIRTHDAY")).getTime();
                    long l = (System.currentTimeMillis() - birthday) / 1000 / 60 / 60 / 24 / 365;
                    input.setUser_age((int) l);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
            @Override
            public String getId(OrderWide input) {
                return input.getUser_id().toString();
            }
        }, 60, TimeUnit.SECONDS);
        //5.2 关联地区维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(orderWideWithUserDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getId(OrderWide orderWide) {
                        return orderWide.getProvince_id().toString();
                    }

                    @Override
                    public void mergeInfo(OrderWide orderWide, JSONObject dimInfo) {
                        String name = dimInfo.getString("NAME");
                        String area_code = dimInfo.getString("AREA_CODE");
                        String iso_code = dimInfo.getString("ISO_CODE");
                        String iso_3166_2 = dimInfo.getString("ISO_3166_2");

                        orderWide.setProvince_name(name);
                        orderWide.setProvince_area_code(area_code);
                        orderWide.setProvince_iso_code(iso_code);
                        orderWide.setProvince_3166_2_code(iso_3166_2);
                    }
                }, 60, TimeUnit.SECONDS);
//        orderWideWithProvinceDS.print();

        //5.3 关联SKU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(
                orderWideWithProvinceDS, new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public void mergeInfo(OrderWide orderWide, JSONObject jsonObject){
                        orderWide.setSku_name(jsonObject.getString("SKU_NAME"));
                        orderWide.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                        orderWide.setSpu_id(jsonObject.getLong("SPU_ID"));
                        orderWide.setTm_id(jsonObject.getLong("TM_ID"));
                    }

                    @Override
                    public String getId(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSku_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //5.4 关联SPU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(
                orderWideWithSkuDS, new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    public void mergeInfo(OrderWide orderWide, JSONObject jsonObject) {
                        orderWide.setSpu_name(jsonObject.getString("SPU_NAME"));
                    }

                    @Override
                    public String getId(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSpu_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //5.5 关联品牌维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(
                orderWideWithSpuDS, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void mergeInfo(OrderWide orderWide, JSONObject jsonObject){
                        orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                    }

                    @Override
                    public String getId(OrderWide orderWide) {
                        return String.valueOf(orderWide.getTm_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //5.6 关联品类维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(
                orderWideWithTmDS, new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public void mergeInfo(OrderWide orderWide, JSONObject jsonObject){
                        orderWide.setCategory3_name(jsonObject.getString("NAME"));
                    }

                    @Override
                    public String getId(OrderWide orderWide) {
                        return String.valueOf(orderWide.getCategory3_id());
                    }
                }, 60, TimeUnit.SECONDS);

        orderWideWithCategory3DS.print("Result>>>>>>>>>>");
        //TODO  将最终结果放入kafka
        orderWideWithCategory3DS
                .map(JSONObject::toJSONString)
                .addSink(MyKafkaUtils.getFlinkKafkaProducer("dwm_order_wide"));
        //TODO 执行程序
        env.execute();
    }
}
