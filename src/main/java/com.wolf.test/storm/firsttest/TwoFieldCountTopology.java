package com.wolf.test.storm.firsttest;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class TwoFieldCountTopology {


	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", new RandomSentenceSpout(), 2);
		//按照word分组，每个相同的word会被分配到相同的task上
		builder.setBolt("split", new WordCountComputingBolt(), 2).fieldsGrouping("spout", new Fields("word"));

		builder.setBolt("count", new PrintWordCountBolt(), 12).shuffleGrouping("split");

		Config conf = new Config();
		conf.setDebug(true);
		conf.setMaxTaskParallelism(1);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("word-count", conf, builder.createTopology());

		Thread.sleep(99990000);

		cluster.shutdown();
	}
}