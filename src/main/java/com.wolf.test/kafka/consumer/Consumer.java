package com.wolf.test.kafka.consumer;

import com.alibaba.fastjson.JSON;
import com.wolf.test.kafka.producer.CustomerObject;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Description:
 * <br/> Created on 03/04/2018 8:50 PM
 *
 * @author 李超
 * @since 1.0.0
 */
public class Consumer {

    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);

    public static void main(String[] args) {
//        testGetAllProperties();

        testBase();
//        testCommitSync();
//        testCommitSpecialOffset();
//        testRebalance();
//        testSecure();
//        testQuit();
//        testCustomPartition();
    }

    private static Properties getSimpleProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "CountryCounter");//所属消费者群组
        return properties;
    }

    private static void testBase() {

        Properties properties = getSimpleProperties();

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        consumer.subscribe(Collections.singletonList("test1"));//订阅，也支持正则

        Map<String, Integer> map = new HashMap<>();
        try {
            while (true) {
                //第一次调用poll时，会查找GroupCoordinator然后加入群组，接收分配的分区。若后期发生再均衡，则再poll时会感知到。心跳发送给协调器。
                //所以要确保轮询间隔内尽快完成。
                ConsumerRecords<String, String> records = consumer.poll(1000);//无数据则阻塞timeout，设0则立即返回。
                System.out.println("records count:"+records.count());
                for (ConsumerRecord<String, String> record : records) {
                    logger.debug("topic={},partition={},offset={},customer={},contry={}", record.topic(),
                            record.partition(), record.offset(), record.key(), record.value());

                    int updatedCount = 1;
                    if (map.containsKey(record.value())) {
                        updatedCount = map.get(record.value()) + 1;
                    }
                    map.put(record.value(), updatedCount);
                    System.out.println(JSON.toJSONString(map));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();//提交任何东西，向群组协调器发送消息，告知离开群组。网络连接和socket也一起关闭。并立即出发一次再均衡
        }
    }

    private static void testGetAllProperties() {
        Properties properties = new Properties();
        //拉取最小字节。服务器存在数据若小于此字节则等到够了再返回，降低双方工作负载
        //若没有很多可用数据，但消费者cpu使用率很高，调大此值？？？可能轮训次数过多导致cpu高
        // 若消费者数量多，调大此值降低broker工作负载
        properties.put("fetch.min.bytes", "10");
        properties.put("fetch.max.wait.ms", "500");//等待时长，与fetch.min.bytes二选一返回
        properties.put("max.partition.fetch.bytes", "1");//调用poll时，从每个分区拉取最大字节。考虑poll轮训之间间隔处理数据的能力，太长则可能被broker认为失联
        properties.put("session.timeout.ms", "3");//失联最大时间
        properties.put("heartbeat.interval.ms", "1");//poll方法向协调器发送心跳频率。一般是session.timeout.ms的1/3
        //没有偏移量可提交(第一次启动的消费者)或者偏移量无效、不存在时读取策略，最新位置/起始位置
        //应该是默认自动提交offset方式在poll中，第一次就按照这个来，若是偏移量无效那么也按这个来。
        properties.put("auto.offset.reset", "latest/earliest");
        properties.put("enable.auto.commit", "true");//自动提交偏移量。为避免重复和数据丢失可以false由程序控制合适提交。另一个参数auto.commit.interval.ms
        properties.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RangeAssignor/org.apache.kafka.clients.consumer.RoundRobinAssignor");//整除连续/逐个均衡
        properties.put("client.id", "xx");
        properties.put("max.poll.records", "11");//单词call方法时记录数
        properties.put("receive.buffer.bytes", "-1");
        properties.put("send.buffer.bytes", "-1");
    }


    //方式1会影响性能
    //方式2使用同步+异步进行提交，由于分区只能所属一个消费者，所以这次异步提交可能下次就成功了，但是若消费者异常了，那么需要finally中处理
    private static void testCommitSync() {

        Properties properties = getSimpleProperties();
        properties.put("auto.commit.offset", false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                logger.debug("topic={},partition={},offset={},customer={},contry={}", record.topic(),
                        record.partition(), record.offset(), record.key(), record.value());

                try {
                    /*1*/consumer.commitSync();//同步提交poll拉取的偏移量,不断重试直到成功

                    /*2*/consumer.commitAsync(new OffsetCommitCallback() {
                        @Override
                        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                            if (null != exception) {
                                logger.error("commit fail for offset:{}", offsets, exception);
                            }
                        }
                    });//异步，不会重试。但是有回调，一般用来记录提交错误或生成度量指标
                    // 若用来重试，则一定注意提交顺序.使用全局序号，若回调的序号与要提交的序号相等，则失败需要重新提交，否则放弃本次重试

                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    try {
                       /*2*/ consumer.commitSync();//确保关闭之前提交成功,失去对分区所有权之前提交最后一个已处理的记录的偏移量。
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        consumer.close();
                    }
                }
            }
        }
    }

    //部分提交
    private static void testCommitSpecialOffset() {

        Properties properties = getSimpleProperties();
        properties.put("auto.commit.offset", false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        int batchSize = 0;

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                logger.debug("topic={},partition={},offset={},customer={},contry={}", record.topic(),
                        record.partition(), record.offset(), record.key(), record.value());

                try {
                    nextOffsets.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1, "no metadata"));
                    if (batchSize % 1000 == 0) {//每1000数据进行提交一次offset
                        consumer.commitAsync(nextOffsets, null);
                    }
                    batchSize++;
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    try {
                       consumer.commitSync();//确保关闭之前提交成功,失去对分区所有权之前提交最后一个已处理的记录的偏移量。
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        consumer.close();
                    }
                }
            }
        }
    }

    private static void testRebalance() {

        Properties properties = getSimpleProperties();
        properties.put("auto.commit.offset", false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        try {
            consumer.subscribe(Collections.singletonList("test1"), new HandleRebalance(consumer));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);//从各个分区的最新offset拉取数据
                for (ConsumerRecord<String, String> record : records) {
                    logger.debug("topic={},partition={},offset={},customer={},contry={}", record.topic(),
                            record.partition(), record.offset(), record.key(), record.value());

                    nextOffsets.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1, "no metadata"));
                }
                //The committed offset should be the next message your application will consume
                consumer.commitAsync(nextOffsets, null);
            }
        } catch (WakeupException e) {
            //忽略异常，有人准备关闭此消费者
            e.printStackTrace();
        } catch (Exception e) {
            logger.error("Unexpected error", e);
        } finally {
            try {
                consumer.commitSync(nextOffsets);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                consumer.close();
                logger.info("Closed consumer and we are done");
            }
        }
    }

    //跟踪所有分区，记录下次准备处理的分区
    static Map<TopicPartition, OffsetAndMetadata> nextOffsets = new HashMap<>();

    static class HandleRebalance implements ConsumerRebalanceListener {

        KafkaConsumer<String, String> consumer;

        public HandleRebalance(KafkaConsumer<String, String> consumer) {
            this.consumer = consumer;
        }

        //再均衡之前和消费者停止读取消息之后被调用，这里提交偏移量，让下个接管分区的消费者开始
        //即将失去分区所有权,先提交我自己处理过的offset
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            logger.debug("lost partitions in rebalance. commit current offset:{}", nextOffsets);
            consumer.commitSync(nextOffsets);
        }

        //重新分配分区之后和消费者开始读取消息之前
        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

        }
    }

    //确保数据处理完入db后即使发生崩溃也能正确提交偏移量，即入db与提交offset两操作具有原子性。不会产生重复数据在db
    //offset保存在db中
    private static void testSecure() {

        Properties properties = getSimpleProperties();
        properties.put("auto.commit.offset", false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        try {
            consumer.subscribe(Collections.singletonList("test1"), new SaveOffsetOnRebalance(consumer));
            consumer.poll(0);//加入群组，先拉取一次，仅仅为了分区
            //定位
            for (TopicPartition partition : consumer.assignment()) {
                consumer.seek(partition, getOffsetFromDB(partition));
            }

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                startDBTransaciton();
                for (ConsumerRecord<String, String> record : records) {

                    nextOffsets.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1, "no metadata"));
                    saveRecordInDB(nextOffsets);
                    saveOffsetInDB();
                }
                commitDBTransaction();
            }
        } catch (WakeupException e) {
            //忽略异常，正在关闭消费者
            e.printStackTrace();
        } catch (Exception e) {
            logger.error("Unexpected error", e);
        } finally {
            try {
                commitDBTransaction();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                consumer.close();
                logger.info("Closed consumer and we are done");
            }
        }
    }

    private static void commitOffset(Map<TopicPartition, OffsetAndMetadata> nextOffsets) {

    }

    private static void startDBTransaciton() {

    }

    private static void saveRecordInDB(Map<TopicPartition, OffsetAndMetadata> nextOffsets) {

    }

    private static void saveOffsetInDB() {

    }

    static class SaveOffsetOnRebalance implements ConsumerRebalanceListener {

        KafkaConsumer<String, String> consumer;

        public SaveOffsetOnRebalance(KafkaConsumer<String, String> consumer) {
            this.consumer = consumer;
        }

        //失去分区控制权之前
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            commitDBTransaction();
        }

        //接管后
        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            for (TopicPartition partition : partitions) {
                consumer.seek(partition, getOffsetFromDB(partition));
            }
        }


    }

    private static void commitDBTransaction() {
    }

    private static long getOffsetFromDB(TopicPartition partition) {
        return 0;
    }

    //调用wakeup退出
    private static void testQuit() {

        Properties properties = getSimpleProperties();

        KafkaConsumer<String, String> consumer = null;

        Thread mainThread = Thread.currentThread();

        //jvm退出时触发
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("Starting exit ...");

                consumer.wakeup();

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        }));

        try {
            consumer.subscribe(Collections.singletonList("test1"));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);//若有wakeup则抛出WakeupException
                for (ConsumerRecord<String, String> record : records) {
                    logger.debug("topic={},partition={},offset={},customer={},contry={}", record.topic(),
                            record.partition(), record.offset(), record.key(), record.value());
                }
                for (TopicPartition partition : consumer.assignment()) {
                    logger.debug("Committing offset at position:{}",consumer.position(partition));
                }
                consumer.commitSync();
            }
        } catch (WakeupException e) {
            //忽略异常，正在关闭消费者
            e.printStackTrace();
        } catch (Exception e) {
            logger.error("Unexpected error", e);
        } finally {
            try {
                consumer.commitSync(nextOffsets);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                consumer.close();
                logger.info("Closed consumer and we are done");
            }
        }
    }


    private static void testAvroSerializer() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.serializer", "io.confluent.kafka.serializer.KafkaAvroDeserializer");
        properties.put("schema.registry.url", "schemaUrl");//schema存储位置
        properties.put("group.id", "CountryCounter");

        KafkaConsumer<String, CustomerObject> consumer = new KafkaConsumer<String, CustomerObject>(properties);
        consumer.subscribe(Collections.singletonList("test1"));

        while (true) {
            ConsumerRecords<String, CustomerObject> records = consumer.poll(1000);
            for (ConsumerRecord<String, CustomerObject> record : records) {
                System.out.println("Current customer :"+JSON.toJSONString(record.value()));
            }
            consumer.commitSync();
        }
    }


    //可能只有一个消费者拉取所有分区或从某个特定的分区拉数据
    //自己分区并拉取数据，不会发生再均衡和手动查找分区。订阅+加入消费者群组/自己分区，只能二选一
    private static void testCustomPartition() {

        KafkaConsumer<String, String> consumer = null;
        List<PartitionInfo> partitionInfos = consumer.partitionsFor("testTopic");//获得可用分区,若主题增加了分区，那么这里要重新获取。
        List<TopicPartition> list = new ArrayList<>();

        if (partitionInfos != null) {
            for (PartitionInfo partition : partitionInfos) {
                list.add(new TopicPartition(partition.topic(), partition.partition()));
            }
            consumer.assign(list);//关联特定分区
        }

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                logger.debug("topic={},partition={},offset={},customer={},contry={}", record.topic(),
                        record.partition(), record.offset(), record.key(), record.value());
            }
            consumer.commitSync();
        }
    }
}
