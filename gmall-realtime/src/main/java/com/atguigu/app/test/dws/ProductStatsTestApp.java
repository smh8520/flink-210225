package com.atguigu.app.test.dws;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.function.DimAsyncFunction;
import com.atguigu.app.testt.fun.DimAsyncTestFunction;
import com.atguigu.bean.OrderWide;
import com.atguigu.bean.PaymentWide;
import com.atguigu.bean.ProductStats;
import com.atguigu.common.GmallConstant;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.DateTimeUtil;
import com.atguigu.utils.MyKafkaUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * @author smh
 * @create 2021-08-05 22:59
 */
public class ProductStatsTestApp {
    public static void main(String[] args) {
        //获取执行环境
        System.setProperty("HADOOP_USER_NAME","atguigu");
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
////        设置checkpoint,设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/checkpoint/"));
//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);

        //读取kafka数据
        String groupId = "product_stats_app24";
        String pageViewSourceTopic = "dwd_page_log";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic = "dwd_favor_info";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";
        FlinkKafkaConsumer<String> pageViewSource  = MyKafkaUtils.getFlinkKafkaConsumer(pageViewSourceTopic,groupId);
        FlinkKafkaConsumer<String> orderWideSource  = MyKafkaUtils.getFlinkKafkaConsumer(orderWideSourceTopic,groupId);
        FlinkKafkaConsumer<String> paymentWideSource  = MyKafkaUtils.getFlinkKafkaConsumer(paymentWideSourceTopic,groupId);
        FlinkKafkaConsumer<String> favorInfoSourceSource  = MyKafkaUtils.getFlinkKafkaConsumer(favorInfoSourceTopic,groupId);
        FlinkKafkaConsumer<String> cartInfoSource  = MyKafkaUtils.getFlinkKafkaConsumer(cartInfoSourceTopic,groupId);
        FlinkKafkaConsumer<String> refundInfoSource  = MyKafkaUtils.getFlinkKafkaConsumer(refundInfoSourceTopic,groupId);
        FlinkKafkaConsumer<String> commentInfoSource  = MyKafkaUtils.getFlinkKafkaConsumer(commentInfoSourceTopic,groupId);

        DataStreamSource<String> pageViewDStream = env.addSource(pageViewSource);
        DataStreamSource<String> favorInfoDStream = env.addSource(favorInfoSourceSource);
        DataStreamSource<String> orderWideDStream= env.addSource(orderWideSource);
        DataStreamSource<String> paymentWideDStream= env.addSource(paymentWideSource);
        DataStreamSource<String> cartInfoDStream= env.addSource(cartInfoSource);
        DataStreamSource<String> refundInfoDStream= env.addSource(refundInfoSource);
        DataStreamSource<String> commentInfoDStream= env.addSource(commentInfoSource);


        //转变kafka数据为javaBean对象
        SingleOutputStreamOperator<ProductStats> productStatsWithClickAndDisplayDS = pageViewDStream.flatMap(new FlatMapFunction<String, ProductStats>() {
            @Override
            public void flatMap(String value, Collector<ProductStats> out) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);

                JSONObject page = jsonObject.getJSONObject("page");
                String item_type = page.getString("item_type");
                String page_id = page.getString("page_id");
                Long ts = jsonObject.getLong("ts");
                //如果是商品详情页且item_type=sku_id,则是点击事件
                if ("good_detail".equals(page_id) && "sku_id".equals(item_type)) {
                    out.collect(ProductStats.builder()
                            .ts(ts)//时间戳
                            .click_ct(1L)//点击加1
                            .sku_id(page.getLong("item"))//sku_id
                            .build());
                }

                JSONArray displays = jsonObject.getJSONArray("displays");
                if (displays != null && displays.size() > 0) {
                    for (int i = 0; i < displays.size(); i++) {
                        JSONObject display = displays.getJSONObject(i);
                        //过滤属于商品的曝光信息
                        if ("sku_id".equals(display.getString("item_type"))) {
                            out.collect(ProductStats.builder()
                                    .ts(ts)//时间戳
                                    .display_ct(1L)//点击加1
                                    .sku_id(display.getLong("item"))//sku_id
                                    .build());
                        }
                    }
                }
            }
        });

        SingleOutputStreamOperator<ProductStats> productStateWithFavDS = favorInfoDStream.map(line -> {
            JSONObject jsonObject = JSONObject.parseObject(line);

            return ProductStats.builder()
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .favor_ct(1L)
                    .sku_id(jsonObject.getLong("sku_id"))
                    .build();
        });
        //下单流
        SingleOutputStreamOperator<ProductStats> productStateWithOrderDS = orderWideDStream.map(line -> {
            OrderWide orderWide = JSONObject.parseObject(line, OrderWide.class);

            HashSet<Long> orderIdSet = new HashSet<>();
            orderIdSet.add(orderWide.getOrder_id());

            return ProductStats.builder()
                    .sku_id(orderWide.getSku_id())//sku_id
                    .order_sku_num(orderWide.getSku_num())//商品个数
                    .order_amount(orderWide.getTotal_amount())//商品价格
                    .orderIdSet(orderIdSet)//订单id
                    .ts(DateTimeUtil.toTs(orderWide.getCreate_time()))//时间戳
                    .build();
        });
        //支付流
        SingleOutputStreamOperator<ProductStats> productStateWithPaymentDS = paymentWideDStream.map(line -> {
            PaymentWide paymentWide = JSONObject.parseObject(line, PaymentWide.class);


            HashSet<Long> paymentIdSet = new HashSet<>();
            paymentIdSet.add(paymentWide.getOrder_id());

            return ProductStats.builder()
                    .sku_id(paymentWide.getSku_id())//sku_id
                    .ts(DateTimeUtil.toTs(paymentWide.getPayment_create_time()))//支付时间
                    .payment_amount(paymentWide.getTotal_amount())//支付金额
                    .paidOrderIdSet(paymentIdSet)//支付的订单id
                    .build();
        });
        //退单流
        SingleOutputStreamOperator<ProductStats> productStateWithRefundDS = refundInfoDStream.map(line -> {
            JSONObject jsonObject = JSONObject.parseObject(line);

            HashSet<Long> refundIdSet = new HashSet<>();
            refundIdSet.add(jsonObject.getLong("order_id"));

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .refund_amount(jsonObject.getBigDecimal("refund_amount"))
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .refundOrderIdSet(refundIdSet)
                    .build();
        });

        //加购流
        SingleOutputStreamOperator<ProductStats> productStateWithCartDS = cartInfoDStream.map(line -> {
            JSONObject jsonObject = JSONObject.parseObject(line);
            return ProductStats.builder()
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .sku_id(jsonObject.getLong("sku_id"))
                    .cart_ct(1L)
                    .build();
        });

        SingleOutputStreamOperator<ProductStats> productStateWithCommenDS = commentInfoDStream.map(line -> {
            JSONObject jsonObject = JSONObject.parseObject(line);

            String appraise = jsonObject.getString("appraise");
            long num = 0L;
            if (GmallConstant.APPRAISE_GOOD.equals(appraise)) {
                num = 1L;
            }

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .comment_ct(1L)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .good_comment_ct(num)
                    .build();
        });

        //union
        DataStream<ProductStats> unionDS = productStatsWithClickAndDisplayDS.union(productStateWithCartDS, productStateWithFavDS, productStateWithOrderDS
                , productStateWithPaymentDS, productStateWithRefundDS, productStateWithCommenDS);

        //设置事件时间,分组,开窗,聚合
        KeyedStream<ProductStats, Long> productStatsLongKeyedStream = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
                    @Override
                    public long extractTimestamp(ProductStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                })).keyBy(ProductStats::getSku_id);

        SingleOutputStreamOperator<ProductStats> reduce = productStatsLongKeyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<ProductStats>() {
                    @Override
                    public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {
                        stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                        stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                        stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                        stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());
                        stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
                        stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());

                        stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());
                        stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));

                        stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());

                        stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));

                        stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());


                        stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                        stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());
                        return stats1;
                    }
                }, new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                    @Override
                    public void apply(Long aLong, TimeWindow window, Iterable<ProductStats> input, Collector<ProductStats> out) throws Exception {
                        ProductStats next = input.iterator().next();

                        String start = DateTimeUtil.toYMDhms(new Date(window.getStart()));
                        String end = DateTimeUtil.toYMDhms(new Date(window.getEnd()));

                        next.setEdt(end);
                        next.setStt(start);

                        next.setRefund_order_ct((long) next.getRefundOrderIdSet().size());
                        next.setOrder_ct((long) next.getOrderIdSet().size());
                        next.setPaid_order_ct((long) next.getPaidOrderIdSet().size());
                    }
                });

        //查询维度表,补全维度数据
        SingleOutputStreamOperator<ProductStats> productStatsSingleOutputStreamOperator = AsyncDataStream.unorderedWait(reduce, new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {
            @Override
            public void mergeInfo(ProductStats productStats, JSONObject jsonObject) {
                productStats.setSku_name(jsonObject.getString("SKU_NAME"));
                productStats.setSku_price(jsonObject.getBigDecimal("PRICE"));
                productStats.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                productStats.setSpu_id(jsonObject.getLong("SPU_ID"));
                productStats.setTm_id(jsonObject.getLong("TM_ID"));
            }

            @Override
            public String getId(ProductStats input) {
                return input.getSku_id().toString();
            }
        }, 60, TimeUnit.SECONDS);
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDstream =
                AsyncDataStream.unorderedWait(productStatsSingleOutputStreamOperator,
                        new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
                            @Override
                            public void mergeInfo(ProductStats productStats, JSONObject jsonObject){
                                productStats.setSpu_name(jsonObject.getString("SPU_NAME"));
                            }
                            @Override
                            public String getId(ProductStats productStats) {
                                return String.valueOf(productStats.getSpu_id());
                            }
                        }, 60, TimeUnit.SECONDS);

//6.3 补充品类维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3Dstream =
                AsyncDataStream.unorderedWait(productStatsWithSpuDstream,
                        new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                            @Override
                            public void mergeInfo(ProductStats productStats, JSONObject jsonObject) {
                                productStats.setCategory3_name(jsonObject.getString("NAME"));
                            }
                            @Override
                            public String getId(ProductStats productStats) {
                                return String.valueOf(productStats.getCategory3_id());
                            }
                        }, 60, TimeUnit.SECONDS);

//6.4 补充品牌维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDstream =
                AsyncDataStream.unorderedWait(productStatsWithCategory3Dstream,
                        new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                            @Override
                            public void mergeInfo(ProductStats productStats, JSONObject jsonObject) {
                                productStats.setTm_name(jsonObject.getString("TM_NAME"));
                            }
                            @Override
                            public String getId(ProductStats productStats) {
                                return String.valueOf(productStats.getTm_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        productStatsWithTmDstream.print("to save");
        //TODO 7.将数据写入clickHouse
        productStatsWithTmDstream.addSink(ClickHouseUtil.getSink("insert into product_stats_2021 " +
                "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        //TODO 8.开启任务
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        //将数据写入clickHouse

        //执行代码
    }
}
