package com.wolf.test.storm.firsttest;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * <b>功能</b>
 *
 * @author 李超
 * @Date 2015/11/8
 */
class PrintWordCountBolt extends BaseBasicBolt {

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		Integer count = tuple.getInteger(0);
		String word = tuple.getString(1);
		System.out.println("count:" + count + ",word:" + word);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}
}
