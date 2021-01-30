package com.atguigu.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * Author: Felix
 * Date: 2021/1/30
 * Desc: 操作Kafka的工具类
 */
public class MyKafkaUtil {
    private static String kafkaServer = "hadoop202:9092,hadoop203:9092,hadoop204:9092";
    //获取FlinkKafkaConsumer
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic,String groupId){
        //Kafka连接的一些属性配置
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaServer);
        return new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),props);
    }
}
