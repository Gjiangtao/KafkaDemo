package com.test;

import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Kafka 生产者API
 */
public class MyProdecer {
    public static void main(String[] args) throws IOException {
        // 加载配置信息
        Properties prop = new Properties();
        /**
         * 两种测试配置信息的方法
         * 1、put方法一条条的设置
         * 2、在在resource文件夹中的配置文件producer.properties中设置所有参数，然后load
         */

        //1、put方法一条条的设置
        prop.put("bootstrap.servers", "172.21.2.107:9092");
        prop.put("acks", "all");
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //2、在配置文件中设置所有参数，然后load
//        prop.load(MyProdecer.class.getClassLoader().getResourceAsStream("producer.properties"));


        /**
         * 创建执行入口，构建生产者
         */
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);
        //设置topic
        String topic = "mytopic";
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, "hello world");
        // 通过send发送
        producer.send(record);

        for (int i = 0; i < 100; i++)
            producer.send(new ProducerRecord<String, String>(topic, Integer.toString(i), Integer.toString(i)));


        //释放资源
        producer.close();


    }
}
