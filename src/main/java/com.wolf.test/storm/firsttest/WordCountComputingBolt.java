package com.wolf.test.storm.firsttest;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * <b>功能</b>
 *
 * @author 李超
 * @Date 2015/11/8
 */
class WordCountComputingBolt implements IRichBolt {

	private OutputCollector _collector;
	private static final long serialVersionUID = 637302730666830334L;
	private Map<String, Integer> map = new HashMap<String, Integer>();

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//定义要发送的字段
		declarer.declare(new Fields("count", "word"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this._collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		String string = input.getString(0);
		String[] split = string.split(" ");
		for (String s : split) {
			Integer integer = map.get(s);
			if (null == integer) {
				integer = 1;
			} else {
				integer = integer + 1;
			}
			map.put(s, integer);
			_collector.emit(new Values(integer, s));
		}
	}

	@Override
	public void cleanup() {

	}
}