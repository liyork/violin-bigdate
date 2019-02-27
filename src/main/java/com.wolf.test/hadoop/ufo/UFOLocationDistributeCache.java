package com.wolf.test.hadoop.ufo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Description:
 * <br/> Created on 12/29/17 9:23 AM
 *
 * @author 李超
 * @since 1.0.0
 */
public class UFOLocationDistributeCache {

    static class MapClass extends MapReduceBase
            implements Mapper<LongWritable, Text, Text, LongWritable> {

        @Override
        public void configure(JobConf job) {
            try {
                Path[] localCacheFiles = DistributedCache.getLocalCacheFiles(job);

                for (Path localCacheFile : localCacheFiles) {
                    BufferedReader bufferedReader = new BufferedReader(new FileReader(new File(localCacheFile.toString())));
                    String s = bufferedReader.readLine();
                    System.out.println(s);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            super.configure(job);
        }

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
        DistributedCache.addCacheFile(new URI("/Users/test/cache/states.txt"),configuration);
        //公共conf
        JobConf conf = new JobConf(configuration,UFOLocationDistributeCache.class);
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
