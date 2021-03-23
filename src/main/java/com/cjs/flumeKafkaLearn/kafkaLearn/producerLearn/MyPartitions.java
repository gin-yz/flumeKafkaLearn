package com.cjs.flumeKafkaLearn.kafkaLearn.producerLearn;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class MyPartitions implements Partitioner {
    /**
     *
     * @param s 主题名
     * @param o　The key to partition on (or null if no key)
     * @param bytes　serialized key to partition on (or null if no key)
     * @param o1　The value to partition on or null
     * @param bytes1　serialized value to partition on or null
     * @param cluster　The current cluster metadata
     * @return
     */
    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {

        System.out.println(s);
        System.out.println(o);
        System.out.println(new String(bytes, StandardCharsets.UTF_8));
        System.out.println(o1);
        System.out.println(new String(bytes1,StandardCharsets.UTF_8));

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
