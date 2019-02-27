package com.wolf.test.storm.project.base;

import com.taobao.metamorphosis.Message;

import java.util.concurrent.CountDownLatch;

/**
 * @author blogchong
 * @Blog   www.blogchong.com
 * @email  blogchong@gmail.com
 * @QQ_G   191321336
 * @version 2014年11月15日 上午21:36:19
 */

public final class MetaMessageWrapper {

	public final Message message;
	public final CountDownLatch latch;
	public volatile boolean success = false;

	public MetaMessageWrapper(final Message message) {
		super();
		this.message = message;
		this.latch = new CountDownLatch(1);
	}
}