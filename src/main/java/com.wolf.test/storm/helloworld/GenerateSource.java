package com.wolf.test.storm.helloworld;

import java.io.*;
import java.util.Random;

/**
 * @author blogchong
 * @Blog   www.blogchong.com
 * @email  blogchong@gmail.com
 * @QQ_G   191321336
 * @version 2014年11月9日 上午11:26:29
 */

//该的目的是构造一个随机的domain数据集
public class GenerateSource {

	public static void main(String[] args) {

		Random random = new Random();

		int note_num = 100000;

		//构造一个随机记录
		String[] net0 = { "baidu", "hitwh", "google", "gooddy", "hadoop",
				"storm", "tengxun", "book", "phone", "fuck" };
		String[] net1 = { "com", "net", "cn", "edu", "tv", "org", "us", "jp",
				"rec", "info" };
		String[] times = { "2000", "2001", "2002", "2005", "2007", "2010",
				"2011", "2012", "2013", "1998" };
		String[] value = { "1326", "1446", "1401", "1202", "1871", "2000",
				"122", "23000", "400", "240" };
		String[] validity = { "3", "5", "20", "100", "32", "12", "50", "1",
				"23", "45" };
		String[] seller = { "Huang", "Lina", "James", "Gale", "Kathryn",
				"Anze", "Green", "Facke", "Nina", "Litao" };

		// 写入中文字符时解决中文乱码问题
		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream(new File("d:\\domain.log"));
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
		OutputStreamWriter osw = null;
		try {
			osw = new OutputStreamWriter(fos, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		BufferedWriter bw = new BufferedWriter(osw);

		for (int i = 0; i < note_num; i++) {
			// 构造域名
			String net = "www." + net0[random.nextInt(10)] + "."
					+ net1[random.nextInt(10)];
			String records = net + "\t" + value[random.nextInt(10)] + "\t"
					+ times[random.nextInt(10)] + "\t"
					+ validity[random.nextInt(10)] + "\t"
					+ seller[random.nextInt(10)];
			try {
				bw.write(records);
				bw.newLine();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		// 注意关闭的先后顺序，先打开的后关闭，后打开的先关闭
		try {
			bw.close();
			osw.close();
			fos.close();
			System.out.println("write ok !");
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
