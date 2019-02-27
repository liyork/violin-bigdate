package com.wolf.test.storm.project.xml;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import com.wolf.test.storm.project.base.MacroDef;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;

/**
 * @author blogchong
 * @Blog   www.blogchong.com
 * @email  blogchong@gmail.com
 * @QQ_G   191321336
 * @version 2014年11月16日 上午19:54:29
 */

public class MetaXml {

	//xml路径
	private static String fd;
	//MetaBolt参数
	//!--MetaQ消息队列--
	public static String MetaTopic;
	//!--MetaQ服务地址--
	public static String MetaZkConnect;
	//!--MetaQ服务路径--
	public static String MetaZkRoot;


	@SuppressWarnings("static-access")
	public MetaXml(String str){
		this.fd = str;
	}

	@SuppressWarnings("static-access")
	public void read(){
		try {
			File file = new File(this.fd);
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document doc = db.parse(file);

			NodeList nl = doc.getElementsByTagName(MacroDef.Parameter);

			Element e = (Element)nl.item(0);

			MetaTopic = e.getElementsByTagName(MacroDef.MetaTopic).item(0).getFirstChild().getNodeValue();
			MetaZkConnect = e.getElementsByTagName(MacroDef.MetaZkConnect).item(0).getFirstChild().getNodeValue();
			MetaZkRoot = e.getElementsByTagName(MacroDef.MetaZkRoot).item(0).getFirstChild().getNodeValue();


		}catch (Exception e) {
			e.printStackTrace();
		}
	}

}
