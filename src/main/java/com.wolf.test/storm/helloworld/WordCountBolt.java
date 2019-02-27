package com.wolf.test.storm.helloworld;

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
 * @author blogchong
 * @Blog   www.blogchong.com
 * @email  blogchong@gmail.com
 * @QQ_G   191321336
 * @version 2014年11月9日 下午10:09:43
 */

@SuppressWarnings("serial")
public class WordCountBolt implements IRichBolt  {

	/**
	 * 这个bolt接收从normalizer处理的单个单词
	 * 然后数据被加载到一个map中，实时的将统计结果发布出去
	 */

	Integer id;
	String name;
	Map<String, Integer> counters;

	private OutputCollector collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
						OutputCollector collector) {

		this.collector = collector;

		//new一个hashmap实例
		this.counters = new HashMap<String, Integer>();

		this.name = context.getThisComponentId();

		this.id = context.getThisTaskId();

	}

	@Override
	public void execute(Tuple input) {
		String str = input.getString(0);
		System.out.println("this"+this+"____"+str);

		if (!counters.containsKey(str)) {
			counters.put(str, 1);
		} else {
			//若是map中有记录，则计数+1
			Integer c = counters.get(str) + 1;
			counters.put(str, c);
		}

		String send_str = null;

		int count = 0;

		for (String key : counters.keySet()) {
			if (count == 0) {
				send_str = "[" + key + " : " + counters.get(key) + "]";
			} else {
				send_str = send_str + ", [" + key + " : " + counters.get(key) + "]";
			}

			count++;

		}

		send_str = "The count:" + count + " #### " + send_str;

		this.collector.emit(new Values(send_str));



	}

	@Override
	public void cleanup() {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("send_str"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
