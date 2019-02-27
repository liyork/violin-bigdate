package com.wolf.test.kafka.producer;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * Description:
 * <br/> Created on 03/04/2018 8:36 PM
 *
 * @author 李超
 * @since 1.0.0
 */
public class Producer {

    public static void main(String[] args) {
//        testConfig();

        testBase();

//        testAvroSerializer();

//        testDefaultPartition();
    }

    private static void testConfig() {
        Properties properties = getAllProperties();

        KafkaProducer producer = new KafkaProducer<String,String>(properties);

        ProducerRecord<String, String> record =
                new ProducerRecord<>("test1", "Precision Products", "France");

        invoke(producer, record);
//        asyncInvoke(producer, record);
    }

    private static void testBase() {
        Properties properties = getSimpleProperties();

        KafkaProducer producer = new KafkaProducer<String,String>(properties);

        for (int i = 0; i < 100000; i++) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("test1", "France_"+i);

            invoke(producer, record);
//        asyncInvoke(producer, record);

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static Properties getSimpleProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");//提供两个集群中的broker，其他broker生产者自己查找
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return properties;
    }


    private static Properties getAllProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092,borker2:9092");//提供两个集群中的broker，其他broker生产者自己查找
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("acks", "0/1/all");//没有/首领/所有人接收后才认为成功
        properties.put("buffer.memory", "");//生产者内存缓冲区
        properties.put("compression.type", "snappy/gzip");//snappy占用cpu较少压缩比还可以。gzip占用cpu较多更高的压缩比。
        properties.put("retries", "3");//重试次数。测试一下恢复一个崩溃就节点需要时间
        properties.put("batch.size", "20");//字节。多个消息发送到一个分区。不一定满了再发
        properties.put("linger.ms", "20");//准备发送批次的时间，增加延迟提高吞吐量
        //生产者在接收服务器响应之前可以发送消息数量。值越高占用内存越多，提升吞吐量。若设定1则保证消息强有序，但是吞吐量降低
        properties.put("max.in.flight.requests.per.connection", "20");
        properties.put("request.timeout.ms", "20");//生产者发送数据时等待服务器返回相应的时间
        properties.put("metadata.fetch.timeout.ms", "20");//生产者获取元数据时等待服务器返回响应的时间。
        properties.put("timeout.ms", "20");//broker等待同步副本返回消息确认的时间。
        properties.put("max.block.ms", "20");//生产者发送缓冲区已满时，或者没有可用元数据时阻塞时长。
        properties.put("max.request.size", "20");//单个消息最大值
        //tcp socket接收和发送数据包的缓冲大小，-1使用操作系统默认值。若生产者、消费者与broker不同数据中心，则可以适当增加这些值，跨数据中心网络一般比较高的延迟和低的带宽
        properties.put("receive.buffer.bytes", "-1/20");
        properties.put("send.buffer.bytes", "-1/20");
        return properties;
    }

    private static void invoke(KafkaProducer producer, ProducerRecord<String, String> record) {
        try {
//            producer.send(record);//忽略结果
            producer.send(record).get();//同步发送
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void asyncInvoke(KafkaProducer producer, ProducerRecord<String, String> record) {

        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                System.out.println("onCompletion in send Callback,recordMetadata:" + JSON.toJSONString(recordMetadata));

                if (e != null) {
                    e.printStackTrace();
                }
            }
        });
    }

    //avro数据文件包含整个schema,若每条kafka记录中都含有schema会让记录很大。由于读取记录时使用schema，使用"schema注册表"实现
    //但是kafka-serialization-avro_2.11似乎下不来属于Confluent Schema Registry
    //github 似乎是这个https://github.com/confluentinc/schema-registry
    private static void testAvroSerializer() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "io.confluent.kafka.serializer.KafkaAvroSerializer");
        properties.put("value.serializer", "io.confluent.kafka.serializer.KafkaAvroSerializer");
        properties.put("schema.registry.url", "schemaUrl");//schema存储位置
        //也可以使用一般对象而非avro对象，但是在这里生成新的avro对象使用schema

        KafkaProducer producer = new KafkaProducer<String,String>(properties);

        CustomerObject customerObject = new CustomerObject(1,"xx");

        String topic = "customerContacts";
        ProducerRecord<Integer, CustomerObject> record =
                new ProducerRecord<Integer, CustomerObject>(topic, customerObject.getCustomerId(), customerObject);

        producer.send(record);
    }

    // 若不放入key+默认分区器，则轮询放入每个可用分区。
    // 若用key+默认分区器，则对key使用硬hash放入所有分区(可用、不可用)，
    // 那么若以后增加分区，则产生问题(老数据一个分区，新数据一个分区),最好提早规划分区以后不要增加分区??
    //可能修改分区影响kafka优化分区读取策略，这种分区只能保证同一分区的顺序性。一般情况客户不会要求某些消息的顺序性。
    //若很需要对某个key进行顺序性，那么可能一种方式是最早规划分区容量，还有一种是消费那边处理好扩容导致的问题，
    //还有一种是使用特定分区器进行指定分区写入(这样可以提早规划好分区容量)。
    private static void testDefaultPartition() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "io.confluent.kafka.serializer.KafkaAvroSerializer");
        properties.put("value.serializer", "io.confluent.kafka.serializer.KafkaAvroSerializer");

        KafkaProducer producer = new KafkaProducer<String,String>(properties);

        ProducerRecord<String, String> record =
                new ProducerRecord<>("test1", "Precision Products", "France");
        ProducerRecord<String, String> record2 =
                new ProducerRecord<>("test1",  "France");//无键

        producer.send(record);
        producer.send(record2);
    }
}
