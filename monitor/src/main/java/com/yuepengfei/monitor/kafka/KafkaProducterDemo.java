package com.yuepengfei.monitor.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;

public class KafkaProducterDemo {
    public static void main(String[] args) throws InterruptedException {
        /* 1、连接集群，通过配置文件的方式
         * 2、发送数据-topic:order，value
         */
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);

        String[] words = {"spark", "suning", "spring", "flink", "kafka", "hadoop"};

        int i = 0;
        while (true) {
            i++;
            // 发送数据 ,需要一个producerRecord对象,最少参数 String topic, V value
            Random random = new Random();
            String message = words[random.nextInt(6)];
            System.out.println(message);
            kafkaProducer.send(new ProducerRecord<String, String>("test", message));
            Thread.sleep(10);
            if (i > 10000){
                break;
            }
        }

        kafkaProducer.close();
    }
}
