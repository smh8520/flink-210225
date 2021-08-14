package com.atguigu.utils;


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;


/**
 * @author smh
 * @create 2021-07-28 11:03
 */
public class MyKafkaUtils {

    private static String kafkaServer="hadoop102:9092,hadoop103:9092,hadoop104:9092";

    private static String defaultTopic="dwd_default";

    public static FlinkKafkaProducer<String> getFlinkKafkaProducer(String topic) {

        return new FlinkKafkaProducer<String>(kafkaServer,
                topic,
                new SimpleStringSchema());
    }

    public static <T> FlinkKafkaProducer<T> getFlinkKafkaProducer(KafkaSerializationSchema<T> x) {
        Properties p = new Properties();
        p.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaServer);
        p.setProperty(ProducerConfig.ACKS_CONFIG,"-1");
        p.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,1000);
       return  new FlinkKafkaProducer<T>(defaultTopic,
                x,
                p,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }



    public static FlinkKafkaConsumer<String> getFlinkKafkaConsumer(String topic, String groupId) {

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty("auto.offset.reset","earliest");
//        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_DOC,"earliest");


        return new FlinkKafkaConsumer<String>(topic,
                new SimpleStringSchema(),
                properties);
    }
    
    public static String getSqlWith(String topic,String groupId){
        return "WITH('connector' = 'kafka', " +
                " 'topic' = '"+topic+"',"   +
                " 'properties.bootstrap.servers' = '"+ kafkaServer +"', " +
                " 'properties.group.id' = '"+groupId+ "', " +
                "  'format' = 'json', " +
                "  'scan.startup.mode' = 'latest-offset')  ";
    }
}
