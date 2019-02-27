package com.wolf.test.storm.project.base;

import com.wolf.test.storm.project.bolt.MetaBolt;
import com.wolf.test.storm.project.bolt.MonitorBolt;
import com.wolf.test.storm.project.bolt.MysqlBolt;
import com.wolf.test.storm.project.spout.MetaSpout;

import java.io.File;

/**
 * @author blogchong
 * @Blog   www.blogchong.com
 * @email  blogchong@gmail.com
 * @QQ_G   191321336
 * @version 2014年11月15日 上午22:22:25
 */

/**
 * 该函数定时检测配置文件是否发生改变，
 * 实现了Topology的动态配置 即在不重启top的情况下，实现数据处理的动态配置
 */

public class ConfCheck extends Thread {

	private String xmlPath ;
	private int heartbeat = 1000;
	private String type = "type";

	public ConfCheck(String XmlPath, int HeartBeat, String type) {
		this.xmlPath = XmlPath;
		this.heartbeat = HeartBeat;
		this.type = type;
	}

	public void run() {
		long init_time = 0;// hash初始值

		for (int i = 0;; i++) {

			try {

				File file = new File(this.xmlPath);
				// 检查时间戳是否一致
				long lastTime = file.lastModified();

				if (i == 0) {
					init_time = lastTime;
				} else {
					if (init_time != lastTime) {

						init_time = lastTime;

						if (this.type.equals(MacroDef.Thread_type_metaqspout)) {
							MetaSpout.isload();
						} else if (this.type
								.equals(MacroDef.Thread_type_monitorbolt)) {
							MonitorBolt.isNotModified();
						} else if (this.type
								.equals(MacroDef.Thread_type_mysqlbolt)) {
							MysqlBolt.isload();
						} else if (this.type
								.equals(MacroDef.Thread_type_metaqbolt)) {
							MetaBolt.isload();
						}
					}
				}

				Thread.sleep(this.heartbeat);

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws InterruptedException {
		new ConfCheck("MysqlBolt.xml", 1000, "test").start();
		Thread.sleep(100000);
	}

}
