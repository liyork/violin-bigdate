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
public class UFOCountingRecordValidationMapper extends MapReduceBase
        implements Mapper<LongWritable,Text,LongWritable,Text> {

    //Logger logger = LoggerFactory.getLogger(UFOCountingRecordValidationMapper.class);

    enum LineCounters{
        BAD_LINES,
        TOO_MANY_TABS,
        TOO_FEE_TABS
    }

    @Override
    public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
        String line = value.toString();
        if (validate(line,reporter)) {
            output.collect(key,value);
        }else{
            //logger.info("validate is not pass ,key:{},value{}",key,value);
            System.out.println("validate is not pass ,key:"+key+" value:"+value );
        }
    }

    private boolean validate(String line,Reporter reporter) {
        String[] parts = line.split(" ");
        if (parts.length != 6) {
            if (parts.length < 6) {
                reporter.incrCounter(LineCounters.TOO_FEE_TABS,1);
            }else{
                reporter.incrCounter(LineCounters.TOO_MANY_TABS,1);
            }
            reporter.incrCounter(LineCounters.BAD_LINES,1);

            if (reporter.getCounter(LineCounters.BAD_LINES).getCounter() % 10 == 0) {
                reporter.setStatus("get 10 bad lines");
                //logger.error("read 10 bad lines");
                System.err.println("read 10 bad lines");
            }
            return false;
        }
        return true;
    }


}
