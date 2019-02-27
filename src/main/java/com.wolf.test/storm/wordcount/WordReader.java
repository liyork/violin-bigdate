package com.wolf.test.storm.wordcount;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.*;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * <p> Description:
 * <p/>
 * Date: 2015/11/5
 * Time: 13:28
 *
 * @author 李超
 * @version 1.0
 * @since 1.0
 */
public class WordReader implements IRichSpout {
	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector collector;
	private FileReader fileReader;
	private boolean completed = false;
	private Long lastModified;

	public boolean isDistributed() {
		return false;
	}


	/**
	 * 这是第一个方法，里面接收了三个参数，第一个是创建Topology时的配置，
	 * 第二个是所有的Topology数据，第三个是用来把Spout的数据发射给bolt
	 * **/
	@Override
	public void open(Map conf, TopologyContext context,
					 SpoutOutputCollector collector) {
		final String wordsFile = conf.get("wordsFile").toString();
		System.out.println("WordReader open");//line 255
		Executors.newFixedThreadPool(1).execute(new Runnable() {
			@Override
			public void run() {
				while (true) {
					File file = new File(wordsFile);
					if (null == lastModified) {
						lastModified = file.lastModified();
					} else {
						lastModified = file.lastModified();
						completed = false;
					}
					try {
						Thread.sleep(2000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		});

		try {
			//获取创建Topology时指定的要读取的文件路径
			this.fileReader = new FileReader(wordsFile);
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Error reading file ["
					+ conf.get("wordFile") + "]");
		}
		//初始化发射器
		this.collector = collector;

	}
	/**
	 * 这是Spout最主要的方法，在这里我们读取文本文件，并把它的每一行发射出去（给bolt）
	 * 这个方法会不断被调用，为了降低它对CPU的消耗，当任务完成时让它sleep一下
	 * **/
	@Override
	public void nextTuple() {
		System.out.println("WordReader nextTuple");//第一行258 以后总执行
		System.out.println("completed is :"+completed);
		if (completed) {
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				// Do nothing
			}
			return;
		}
		String str;
		//fileReader是固定的，第一次读完，下次再进来使用BufferedReader包装，只读到最新的修改
		BufferedReader reader = new BufferedReader(fileReader);
		System.out.println(reader);
		try {
			// Read all lines
			while ((str = reader.readLine()) != null) {
				/**
				 * 发射每一行，Values是一个ArrayList的实现
				 */
				System.out.println("发射...");
				this.collector.emit(new Values(str), str);
			}
		} catch (Exception e) {
			throw new RuntimeException("Error reading tuple", e);
		} finally {
			System.out.println("finally ...");
			completed = true;
		}

	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		System.out.println("WordReader declareOutputFields");//line 173
		declarer.declare(new Fields("line"));

	}
	@Override
	public void close() {
		// TODO Auto-generated method stub
	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub

	}
	@Override
	public void deactivate() {
		// TODO Auto-generated method stub

	}
	@Override
	public void ack(Object msgId) {
		System.out.println("OK:" + msgId);
	}
	@Override
	public void fail(Object msgId) {
		System.out.println("FAIL:" + msgId);

	}
	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	public static void main(String[] args) throws Exception {
		FileReader in = new FileReader("d:\\words.txt");
		BufferedReader reader = new BufferedReader(in);
		String str;
		while ((str = reader.readLine()) != null) {
			/**
			 * 发射每一行，Values是一个ArrayList的实现
			 */
			System.out.println("发射..." + str);
		}

		Thread.sleep(5000);
		System.out.println("5秒后...");
		BufferedReader reader1 = new BufferedReader(in);
		while ((str = reader1.readLine()) != null) {
			/**
			 * 发射每一行，Values是一个ArrayList的实现
			 */
			System.out.println("发射...1111" + str);
		}
	}
}