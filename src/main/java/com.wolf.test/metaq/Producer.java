package com.wolf.test.metaq;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.producer.MessageProducer;
import com.taobao.metamorphosis.client.producer.SendResult;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.utils.ZkUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * <p> Description:
 * <p/>
 * Date: 2015/11/11
 * Time: 8:31
 *
 * @author 李超
 * @version 1.0
 * @since 1.0
 */
public class Producer {

	// New session factory
	private MessageSessionFactory sessionFactory ;
	// create producer MessageProducer是线程安全的
	private MessageProducer producer ;

	public static void main(String[] args) throws Exception {
		new Producer().init();
	}

	private  void init() throws MetaClientException, IOException, InterruptedException {
		MetaClientConfig metaClientConfig = new MetaClientConfig();
		ZkUtils.ZKConfig zkConfig = new ZkUtils.ZKConfig();
		zkConfig.setZkConnect("127.0.0.1:2181");
		metaClientConfig.setZkConfig(zkConfig);
		sessionFactory = new MetaMessageSessionFactory(metaClientConfig);
		producer = sessionFactory.createProducer();

		// publish topic
		final String topic = "meta-test";
		producer.publish(topic);

		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
		String line = null;
		//每次reader.readLine()会卡住，等用户输入
		while ((line = reader.readLine()) != null) {
			// send message
			SendResult sendResult = producer.sendMessage(new Message(topic, line.getBytes()));
			// check result
			if (!sendResult.isSuccess()) {
				System.err.println("Send message failed,error message:" + sendResult.getErrorMessage());
			}
			else {
				System.out.println("Send message successfully,sent to " + sendResult.getPartition());
			}
		}
	}

}
