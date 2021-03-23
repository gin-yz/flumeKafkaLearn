package com.cjs.flumeKafkaLearn.kafkaLearn.producerLearn;

import java.util.Map;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
public class CounterInterceptor implements ProducerInterceptor<String, String>{
    private int errorCounter = 0;
    private int successCounter = 0;
    @Override
    public void configure(Map<String, ?> configs) {
    }
    @Override
    public ProducerRecord<String, String>
    onSend(ProducerRecord<String, String> record) {
        return record;
    }

    //往RecordAccumulator发送后调用该方法,此时已经序列化,注意这个是每发送的消息都会遍历，而不是等积累后一起发送至RecordAccumulator才调用．
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
// 统计成功和失败的次数
        if (exception == null) {
            successCounter++;
            System.out.println("发送至RecordAccumulator的ｍｅｔｄａｔａ:"+metadata.toString());
        } else {
            errorCounter++;
        }
    }
    @Override
    public void close() {
        // 保存结果
        System.out.println("Successful sent: " + successCounter);
        System.out.println("Failed sent: " + errorCounter);
    }
}
