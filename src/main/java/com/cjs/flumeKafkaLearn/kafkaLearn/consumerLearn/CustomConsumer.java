package com.cjs.flumeKafkaLearn.kafkaLearn.consumerLearn;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class CustomConsumer {
    public static void main(String[] args) {
        //若生产者放入数据．组里还没有消费者，则第一次启动的时候，会根据offset消费掉还未消费的数据
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop1:9092");
        props.put("group.id", "cjs");

        //自动提交offset，以及自动提交的延时
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //重置消费者的ｏｆｆｓｅｔ
        //显示条件：１）换组(offset变成新的了)，２）offset过了期限消失了
        //默认latest
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        //指定不存在的主题也可以
        consumer.subscribe(Arrays.asList("first","second"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);  //拉取延时
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s, partition = %s%n", record.offset(), record.key(), record.value(),record.partition());
        }
    }
}
