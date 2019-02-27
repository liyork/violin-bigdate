package com.wolf.test.storm.project.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.wolf.test.storm.project.base.ConfCheck;
import com.wolf.test.storm.project.base.MacroDef;
import com.wolf.test.storm.project.xml.MonitorXml;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author blogchong
 * @Blog www.blogchong.com
 * @email blogchong@gmail.com
 * @QQ_G 191321336
 * @version 2014年11月9日 上午11:26:29
 */

@SuppressWarnings("serial")
public class MonitorBolt implements IRichBolt {

	private OutputCollector collector;

	//是否加载配置标志位
	private static boolean isNotModified = true;

	private long register = 0;

	//默认参数~~
	String monitorXml = "Monitor.xml";
	// 参数判空标志
	private boolean isNotNullXml = true;
	// 匹配条件间的逻辑关系
	String MatchLogic = "AND";
	// !--匹配类型列表
	String MatchType = "regular::range::routine0";
	// !--匹配字段列表-
	String MatchField = "1::2::5";
	// !--字段值列表-
	String FieldValue = ".*baidu.*::1000,2000::ina";

	public MonitorBolt(String MonitorXML) {

		if (MonitorXML == null) {
			isNotNullXml = false;
		} else {
			this.monitorXml = MonitorXML;
		}
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
						OutputCollector collector) {

		System.out.println("MonitorBolt	--	Start!");
		this.collector = collector;

		if (!this.isNotNullXml) {
			System.out
					.println("MetaSpout-- Erre: can't get the path of Spout.xml!");
		} else {
			// 调用定时检测线程，查看文件是否有变更
			new ConfCheck(this.monitorXml, MacroDef.HEART_BEAT,
					MacroDef.Thread_type_monitorbolt).start();
		}

	}

	@Override
	public void execute(Tuple input) {
		String str = input.getString(0);

		if (!this.isNotNullXml) {
			System.out
					.println("MonitorBolt-- Erre: can't get the path of Monitor.xml!");
		} else {

			// 配置文件发生更改则进行加载参数操作
			if (!isNotModified) {
				reLoading();
				if (register != 0) {
					System.out.println("MonitorBolt-- Conf Change: "
							+ this.monitorXml);
				} else {
					System.out.println("MonitorBolt-- Conf Loaded: "
							+ this.monitorXml);
				}
			}

			boolean isMatch = isMatch(str, this.MatchLogic, this.MatchType,
					this.MatchField, this.FieldValue);
			if (isMatch) {
				this.collector.emit(new Values(str));
			}
		}
	}

	//更改标志位
	public static void isNotModified() {
		isNotModified = false;
	}

	// 加载参数操作
	public void reLoading() {
		// 从conf中获取参数
		System.out.println("monitorXml:     " + this.monitorXml);

		new MonitorXml(this.monitorXml).read();
		this.MatchLogic = MonitorXml.MatchLogic;
		this.MatchType = MonitorXml.MatchType;
		this.MatchField = MonitorXml.MatchField;
		this.FieldValue = MonitorXml.FieldValue;

	}

	private boolean isMatch(String str, String logic, String type,
							String field, String value) {

		String[] types = type.split(MacroDef.FLAG_COLON);
		String[] fields = field.split(MacroDef.FLAG_COLON);
		String[] values = value.split(MacroDef.FLAG_COLON);

		int flag_init = types.length;
		int flag = 0;

		if (logic.equals(MacroDef.RULE_AND)) {
			for (int i = 0; i < flag_init; i++) {
				if (types[i].equals(MacroDef.RLUE_REGULAR)) {
					boolean regu = regular(str, fields[i], values[i]);
					if (regu == true) {
						flag++;
					}
				} else if (types[i].equals(MacroDef.RULE_RANGE)) {
					boolean ran = range(str, fields[i], values[i]);
					if (ran == true) {
						flag++;
					}
				} else if (types[i].equals(MacroDef.RULE_ROUTINE0)) {
					boolean rou0 = routine0(str, fields[i], values[i]);
					if (rou0 == true) {
						flag++;
					}
				} else if (types[i].equals(MacroDef.RULE_ROUTINE1)) {
					boolean rou1 = routine1(str, fields[i], values[i]);
					if (rou1 == true) {
						flag++;
					}
				}
			}

			if (flag == flag_init) {
				return true;
			} else {
				return false;
			}

		} else if (logic.equals(MacroDef.RULE_OR)) {

			for (int i = 0; i < flag_init; i++) {
				if (types[i].equals(MacroDef.RLUE_REGULAR)) {
					boolean regu = regular(str, fields[i], values[i]);
					if (regu == true) {
						flag++;
					}
				} else if (types[i].equals(MacroDef.RULE_RANGE)) {
					boolean ran = range(str, fields[i], values[i]);
					if (ran == true) {
						flag++;
					}
				} else if (types[i].equals(MacroDef.RULE_ROUTINE0)) {
					boolean rou0 = routine0(str, fields[i], values[i]);
					if (rou0 == true) {
						flag++;
					}
				} else if (types[i].equals(MacroDef.RULE_ROUTINE1)) {
					boolean rou1 = routine1(str, fields[i], values[i]);
					if (rou1 == true) {
						flag++;
					}
				}
			}
			if (flag != 0) {
				return true;
			} else {
				return false;
			}
		}
		return false;
	}

	// 正则匹配判断
	private boolean regular(String str, String field, String value) {
		String[] strs = str.split(MacroDef.FLAG_TABS);

		Pattern p = Pattern.compile(value);
		Matcher m = p.matcher(strs[Integer.parseInt(field) - 1]);
		boolean result = m.matches();

		if (result == true) {
			return true;
		} else {
			return false;
		}
	}

	// 范围匹配
	private boolean range(String str, String field, String value) {
		String[] strs = str.split(MacroDef.FLAG_TABS);
		String[] values = value.split(MacroDef.FLAG_COMMA);

		int strss = Integer.parseInt(strs[Integer.parseInt(field) - 1]);

		if (values.length == 1) {
			if (strss > Integer.parseInt(values[0])) {
				return true;
			} else {
				return false;
			}
		} else if (values.length == 2 && values[0].length() == 0) {
			if (strss < Integer.parseInt(values[1])) {
				return true;
			} else {
				return false;
			}
		} else if (values.length == 2 && values[0].length() != 0) {
			if (strss > Integer.parseInt(values[0])
					&& strss < Integer.parseInt(values[1])) {
				return true;
			} else {
				return false;
			}
		} else {
			return false;
		}
	}

	// 常规模糊匹配
	private boolean routine0(String str, String field, String value) {
		String[] strs = str.split(MacroDef.FLAG_TABS);
		String strss = strs[Integer.parseInt(field) - 1];

		if (strss.contains(value) && !strss.equals(value)) {
			return true;
		} else {
			return false;
		}
	}

	// 常规完全匹配
	private boolean routine1(String str, String field, String value) {
		String[] strs = str.split(MacroDef.FLAG_TABS);
		String strss = strs[Integer.parseInt(field) - 1];

		if (strss.equals(value)) {
			return true;
		} else {
			return false;
		}
	}

	@Override
	public void cleanup() {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("str"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
