package com.wolf.test.hadoop.graph;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.jgrapht.GraphMapping;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.Iterator;

/**
 * Description:
 * 对于map，如果是c状态则输出d状态，然后遍历关联节点发送distance+1，其他状态原样发送。只处理c状态的
 *
 * 对于reduce,如果是d直接输出，其他状态选择最大的输出。
 * 状态流转A(未处理)-C(正在处理)-D(处理完成)
 *
 * hadoop jar graph.jar com.wolf.test.hadoop.graph.GraphPath /Users/test/output1/part-00000 /Users/test/output2
 *
 * <br/> Created on 1/2/18 8:44 AM
 *
 * @author 李超
 * @since 1.0.0
 */
public class GraphPath {


    static class GraphPathMapper extends MapReduceBase
            implements Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            Node node = new Node(value.toString());

            if (node.getState().equalsIgnoreCase("C")) {
                output.collect(new Text(node.getId()), new Text(node.getNeighbors() + "\t" + node.getDistance() + "\t" + "D"));

                for (String neighbor : node.getNeighbors().split(",")) {
                    output.collect(new Text(neighbor), new Text("\t" + (node.getDistance() + 1) + "\t" + "C"));
                }
            }else{
                output.collect(new Text(node.getId()), new Text(node.getNeighbors() + "\t" + node.getDistance() + "\t" + node.getState()));
            }
        }
    }

    public static class GraphPathReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

            int distance = -1;
            String state = "";
            String neighbor = "";
            while (values.hasNext()) {
                Node node = new Node(key.toString(),values.next().toString());
                if (node.getState().equalsIgnoreCase("D")) {
                    output.collect(key, new Text(node.getNeighbors()+"\t"+node.getDistance()+"\t"+node.getState()));
                    return;
                }

                if (node.getDistance() > distance) {
                    distance = node.getDistance();
                }

                if (node.getNeighbors()!=null && !node.getNeighbors().equalsIgnoreCase("")) {
                    neighbor = node.getNeighbors();
                }

                if (state.hashCode() < node.getState().hashCode()) {
                    state = node.getState();
                }
            }
            output.collect(key, new Text(neighbor+"\t"+distance+"\t"+state));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        JobConf conf = new JobConf(configuration,GraphPath.class);
        conf.setJobName("GraphPath ");

        conf.setMapperClass(GraphPathMapper.class);
        conf.setReducerClass(GraphPathReducer.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(conf,new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);

    }


}
