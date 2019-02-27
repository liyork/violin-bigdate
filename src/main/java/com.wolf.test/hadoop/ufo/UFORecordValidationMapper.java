package com.wolf.test.hadoop.ufo;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Description:用于chain，公共类
 * <br/> Created on 12/29/17 9:19 AM
 *
 * @author 李超
 * @since 1.0.0
 */
public class UFORecordValidationMapper extends MapReduceBase
        implements Mapper<LongWritable,Text,LongWritable,Text> {

    Logger logger = LoggerFactory.getLogger(UFORecordValidationMapper.class);

    @Override
    public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
        String line = value.toString();
        if (validate(line)) {
            output.collect(key,value);
        }else{
            logger.info("validate is not pass ,key:{},value{}",key,value);
        }
    }

    private boolean validate(String line) {
        String[] parts = line.split(" ");
        if (parts.length != 6) {
            return false;
        }
        return true;
    }

    public static void main(String[] args) {
        boolean validate = new UFORecordValidationMapper().validate("0 1 AB122222 3 4 5");
        System.out.println(validate);
    }
}
