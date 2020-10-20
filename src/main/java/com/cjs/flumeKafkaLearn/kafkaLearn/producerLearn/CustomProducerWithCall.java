package com.cjs.flumeKafkaLearn.kafkaLearn.producerLearn;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class CustomProducerWithCall {
    public static void main(String[] args) {
        Properties props = new Properties();
        //kafka 集群，broker-list
        props.put("bootstrap.servers", "hadoop1:9092");
//        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop1:9092");
        props.put("acks", "all");
        //重试次数
        props.put("retries", 3);
        //批次大小,16k，到了１６ｋ往RecordAccumulator写
        props.put("batch.size", 16384);
        //等待时间,毫秒,若未到１６ｋ，到了１ｍｓ，也发送到RecordAccumulator
        props.put("linger.ms", 1);
        //RecordAccumulator 缓冲区大小
        props.put("buffer.memory", 33554432);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);
        /*
         * <1> 若指定Partition ID,则PR被发送至指定Partition
         * <2> 若未指定Partition ID,但指定了Key, PR会按照hasy(key)发送至对应Partition
         * <3> 若既未指定Partition ID也没指定Key，PR会按照round-robin模式发送到每个Partition
         * <4> 若同时指定了Partition ID和Key, PR只会发送到指定的Partition (Key不起作用，代码逻辑决定)
         * */
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>("first",
                    Integer.toString(i), Integer.toString(i)), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        System.out.println("offset:" + recordMetadata.offset() + ",partition:" + recordMetadata.partition() + " topic:" + recordMetadata.topic());
                    } else {
                        e.printStackTrace();
                    }
                }
            });

        }
        producer.close();
    }
}
