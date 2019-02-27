package com.wolf.test.metaq;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.client.consumer.MessageListener;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.utils.ZkUtils;

import java.util.concurrent.Executor;

/**
 * <p> Description:
 * <p/>
 * Date: 2015/11/11
 * Time: 8:39
 *
 * @author 李超
 * @version 1.0
 * @since 1.0
 */
public class AsyncConsumer {
	// New session factory
	MessageSessionFactory sessionFactory ;
	// consumer group
	final String group = "meta-example";
	// create consumer MessageConsumer也是线程安全的
	MessageConsumer consumer;

	public static void main(String[] args) throws Exception {

		new AsyncConsumer().init();
	}

	private void init() throws MetaClientException {
		MetaClientConfig metaClientConfig = new MetaClientConfig();
		ZkUtils.ZKConfig zkConfig = new ZkUtils.ZKConfig();
		zkConfig.setZkConnect("127.0.0.1:2181");
		metaClientConfig.setZkConfig(zkConfig);
		sessionFactory = new MetaMessageSessionFactory(metaClientConfig);
		// subscribed topic
		final String topic = "meta-test";

		consumer = sessionFactory.createConsumer(new ConsumerConfig(group));
		// subscribe topic
		consumer.subscribe(topic, 1024 * 1024, new MessageListener() {

			public void recieveMessages(Message message) {
				//message参数即为消费者收到的消息，它必不为null
				System.out.println("Receive message " + new String(message.getData()));
			}


			public Executor getExecutor() {
				// Thread pool to process messages,maybe null.
				return null;
			}
		});
		// complete subscribe
		//subscribe仅是将订阅信息保存在本地，并没有实际跟meta服务器交互，要使得订阅关系生效必须调用一次completeSubscribe，
		// completeSubscribe仅能被调用一次，多次调用将抛出异常
		consumer.completeSubscribe();

		System.out.println("111111");
	}

}
