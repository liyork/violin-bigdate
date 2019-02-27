package com.wolf.test.hadoop.reducejoin;

import com.wolf.test.hadoop.ufo.UFOLocation;
import com.wolf.test.hadoop.ufo.UFORecordValidationMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.mapred.lib.MultipleInputs;

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Description:reduce端链接费流量省事，map端链接费事，需要考虑统一缓存或者多个map处理数据
 * 每个源都有不同的mapper，reduce进行合并
 * hadoop jar reducejoin.jar com.wolf.test.hadoop.reducejoin.ReduceJoin /Users/test/input/sales.txt /Users/test/input/accounts.txt /Users/test/output
 * hadoop fs -cat /Users/test/output/part-00000
 * <br/> Created on 1/2/18 8:44 AM
 *
 * @author 李超
 * @since 1.0.0
 */
public class ReduceJoin {


    static class SalesRecordMapper extends MapReduceBase
            implements Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String line = value.toString();
            String[] fields = line.split(" ");
                output.collect(new Text(fields[0]),new Text("sales\t"+fields[1]));
            }
        }

    static class AccountsRecordMapper extends MapReduceBase
            implements Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String line = value.toString();
            String[] fields = line.split(" ");
            output.collect(new Text(fields[0]),new Text("accounts\t"+fields[1]));
        }
    }

    public static class ReduceJoinReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String name = "";
            double total = 0.0;
            int count = 0 ;
            while (values.hasNext()) {
                Text next = values.next();
                String[] parts = next.toString().split("\t");
                if (parts[0].equalsIgnoreCase("sales")) {
                    count++;
                    total += Float.parseFloat(parts[1]);
                } else if (parts[0].equalsIgnoreCase("accounts")) {
                    name = parts[1];
                }
            }

            String str = String.format("%d\t%f", count, total);
            output.collect(new Text(name), new Text(str));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        JobConf conf = new JobConf(configuration,ReduceJoin.class);
        conf.setJobName("Reduce point join");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setReducerClass(ReduceJoinReducer.class);

        MultipleInputs.addInputPath(conf,new Path(args[0]),TextInputFormat.class,SalesRecordMapper.class);
        MultipleInputs.addInputPath(conf,new Path(args[1]),TextInputFormat.class,AccountsRecordMapper.class);

        FileOutputFormat.setOutputPath(conf, new Path(args[2]));

        JobClient.runJob(conf);
    }
}
