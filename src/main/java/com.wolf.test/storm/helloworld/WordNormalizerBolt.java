package com.wolf.test.storm.helloworld;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.wolf.test.storm.project.base.MacroDef;

import java.util.Map;

/**
 * @author blogchong
 * @Blog   www.blogchong.com
 * @email  blogchong@gmail.com
 * @QQ_G   191321336
 * @version 2014年11月9日 下午10:09:43
 */

@SuppressWarnings("serial")
public class WordNormalizerBolt implements IRichBolt  {

	/**
	 * 该部分将spout发送过来的一行记录进行标准化处理
	 * 即将记录拆分成单词，我们只取domain文件中的第一列和最后一列(格式是标准的)
	 */

	private OutputCollector collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
						OutputCollector collector) {
		this.collector = collector;

	}

	@Override
	public void execute(Tuple input) {
		String sentence = input.getString(0);
		//按'/t'拆分
		String[] words = sentence.split(MacroDef.FLAG_TABS);
		//只把第一列和最后一列发送出去

		if (words.length != 0) {

			String domain = words[0];
			String[] do_word = domain.split("\\.");

			for (int i = 0; i < do_word.length; i++) {
				String word = do_word[i].trim();
				//把域名也拆分成单词
				this.collector.emit(new Values(word));
			}

			//把买家直接发送出去
			String word = words[4].trim();
			this.collector.emit(new Values(word));
		}

	}

	@Override
	public void cleanup() {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
