package com.cjs.flumeKafkaLearn.kafkaLearn.producerLearn;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

public class MyPartitions implements Partitioner {
    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {

        List<PartitionInfo> partitionInfos = cluster.availablePartitionsForTopic(s);

        Integer partitionCountForTopic = cluster.partitionCountForTopic(s);

        System.out.println(partitionCountForTopic);
        System.out.println(partitionInfos);

        //写业务逻辑，这里统一到1分区，做测试用
        return 1;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
