package com.wolf.test.hadoop.ufo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.LongSumReducer;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Description:使用ChainMapper进行多个mapper的复用
 * <br/> Created on 12/29/17 9:23 AM
 *
 * @author 李超
 * @since 1.0.0
 */
public class UFOLocation {

    static class MapClass extends MapReduceBase
            implements Mapper<LongWritable, Text, Text, LongWritable> {

        private final static LongWritable one = new LongWritable(1);
        private static Pattern locationPattern = Pattern.compile("[a-zA-Z]{2}[^a-zA-z]*$");

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            String[] fields = line.split(" ");
            String location = fields[2].trim();
            if (location.length() >= 2) {
                Matcher matcher = locationPattern.matcher(location);
                if (matcher.find()) {
                    int start = matcher.start();
                    String state = location.substring(start, start + 2);
                    output.collect(new Text(state.toUpperCase()),one);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        //公共conf
        JobConf conf = new JobConf(configuration,UFOLocation.class);
        conf.setJobName("UFOLocation");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(LongWritable.class);

        JobConf mapconf1 = new JobConf(false);
        ChainMapper.addMapper(conf,UFORecordValidationMapper.class,
                LongWritable.class,Text.class,LongWritable.class,Text.class,true,mapconf1);

        JobConf mapconf2 = new JobConf(false);
        ChainMapper.addMapper(conf,MapClass.class,
                LongWritable.class,Text.class,Text.class,LongWritable.class,true,mapconf2);

        conf.setMapperClass(ChainMapper.class);
        conf.setCombinerClass(LongSumReducer.class);
        conf.setReducerClass(LongSumReducer.class);

        FileInputFormat.setInputPaths(conf, args[0]);
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }

}
