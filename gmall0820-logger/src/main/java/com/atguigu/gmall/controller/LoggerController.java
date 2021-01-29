package com.atguigu.gmall.controller;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Properties;

/**
 * Author: Felix
 * Date: 2021/1/29
 * Desc: 日志处理服务
 * @Slf4j  lombok注解  辅助第三方记录日志框架
 */
@RestController
@Slf4j
public class LoggerController {

    //KafkaTemplate是Spring提供对kafka操作的类

    @Autowired //注入
    KafkaTemplate kafkaTemplate;

    @RequestMapping("/applog")
    public String logger(@RequestParam("param") String jsonLog){
        //1.打印输出到控制台
        //System.out.println(jsonLog);
        //2.落盘   借助记录日志的第三方框架 log4j [logback]
        log.info(jsonLog);
        //3.将生成的日主发送到kafka对应的主题中
       kafkaTemplate.send("ods_base_log",jsonLog);

        return "success";
    }
}
