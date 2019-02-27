package com.wolf.test.kafka.producer;


import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

/**
 * Description:自定义分区器，对指定key进行特殊散列
 * <br/> Created on 03/04/2018 8:49 PM
 *
 * @author 李超
 * @since 1.0.0
 */
public class BananaPartitioner implements Partitioner {


    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        if (null == keyBytes) {
            throw new InvalidRecordException("keybytes 不允许为空");
        }

        if (!(key instanceof String)) {
            throw new InvalidRecordException("key 应为string类型");
        }

        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topic);
        int numPartitions = partitionInfos.size();

        if (key.equals("Banana")) {//后期通过config传进来或者配置文件
            return numPartitions;//banana总是被分配到最后一个分区
        }

        //散列到其他分区(不包含最后一个)
        return (Math.abs(Utils.murmur2(keyBytes)) % (numPartitions - 1));
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
