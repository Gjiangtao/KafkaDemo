package com.test;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * Kafka 消费者API
 */

public class MyConsumer {
    public static void main(String[] args) throws IOException {

        Properties props = new Properties();

        /**
         * 两种测试配置信息的方法
         * 1、put方法一条条的设置
         * 2、在resource文件夹中的配置文件consumer.properties中设置所有参数，然后load
         */

        //1、put方法一条条的设置
        props.setProperty("bootstrap.servers", "172.21.2.107:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //2、在配置文件中设置所有参数，然后load
//        props.load(MyConsumer.class.getClassLoader().getResourceAsStream("consumer.properties"));

        // 构建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        //订阅主题
        consumer.subscribe(Arrays.asList("mytopic"));
        while (true) {
            /**
             * 消费数据 timeout：从consumer的缓冲区Buffer获取可用数据的等待超时时间
             * 如果设置为0，则会返回该缓冲区内的所有数据，如果不设置为0，返回空，并且不能写负数
             */
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("topic = %s, offset = %d, key = %s, value = %s%n", record.topic(), record.offset(), record.key(), record.value());
            }

        }
    }
}
