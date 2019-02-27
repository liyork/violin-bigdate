package com.wolf.test.kafka.stream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Description:
 * bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic wordcount-input --partitions 1 --replication-factor 1
 *
 * bin/kafka-console-producer.sh --broker-list localhost:9092 --topic wordcount-input
 * a b
 * c d
 * a b
 * 123
 *
 * bin/kafka-console-consumer.sh --topic wordcount-output --from-beginning --bootstrap-server localhost:9092 --property print.key=true
 *
 * <br/> Created on 14/04/2018 7:04 PM
 *
 * @author 李超
 * @since 1.0.0
 */
public class StreamsTest {


    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount");//唯一
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // work-around for an issue around timing of creating internal topics
        // Fixed in Kafka 0.10.2.0
        // don't use in large production apps - this increases network load
        // props.put(CommonClientConfigs.METADATA_MAX_AGE_CONFIG, 500);

        KStreamBuilder builder = new KStreamBuilder();//创建拓扑

        KStream<String, String> source = builder.stream("wordcount-input");


        final Pattern pattern = Pattern.compile("\\W+");
        KStream counts = source.flatMapValues(value -> {
            String[] split = pattern.split(value.toLowerCase());
            System.out.println("split:"+Arrays.toString(split));

            ArrayList<String> result = new ArrayList<>(split.length);
            Collections.addAll(result, split);
            return result;
        })//应该是每次一行，拆分成一系列单词
                .map(new KeyValueMapper<String, String, KeyValue<?, ?>>() {
                    @Override
                    public KeyValue<?, ?> apply(String key, String value) {
                        System.out.println("key:"+key+",value:"+value);//上一步数组，那么key为null
                        return new KeyValue<Object, Object>(value, value);
                    }
                })
                .filter((key, value) -> (!value.equals("the")))
                .groupByKey()
                .count("CountStore").mapValues(value -> Long.toString(value)).toStream();
        counts.to("wordcount-output");//发送topic

        //运行
        KafkaStreams streams = new KafkaStreams(builder, props);

        // This is for reset to work. Don't use in production - it causes the app to re-load the state from Kafka on every start
        streams.cleanUp();

        streams.start();

        // usually the stream application would be running forever,
        // in this example we just let it run for some time and stop since the input data is finite.
        Thread.sleep(5000L);

        streams.close();

    }
}
