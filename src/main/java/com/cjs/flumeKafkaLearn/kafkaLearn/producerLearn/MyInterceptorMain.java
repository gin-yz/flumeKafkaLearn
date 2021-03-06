package com.cjs.flumeKafkaLearn.kafkaLearn.producerLearn;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.*;


public class MyInterceptorMain {
    public static void main(String[] args) throws Exception {
// 1 设置配置信息
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop1:9092");
        props.put("acks", "all");
        props.put("retries", 3);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
// 2 构建拦截链
        List<String> interceptors = new ArrayList<>();
        //写的拦截器类全类名,会按照顺序同时启动，因此要确保线程安全
        interceptors.add("com.cjs.flumeKafkaLearn.kafkaLearn.producerLearn.TimeInterceptor");
        interceptors.add("com.cjs.flumeKafkaLearn.kafkaLearn.producerLearn.CounterInterceptor");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);
        String topic = "first";
        Producer<String, String> producer = new KafkaProducer<>(props);
// 3 发送消息
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, "message" + i);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e==null){
                        System.out.println("回调的metadata:"+recordMetadata.toString());
                    }
                }
            });
        }
// 4 一定要关闭 producer，这样才会调用 interceptor 的 close 方法
        producer.close();
    }
}