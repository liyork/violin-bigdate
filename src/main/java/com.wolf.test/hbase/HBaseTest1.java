package com.wolf.test.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;

/**
 * Description:
 * <br/> Created on 2016/9/7 8:27
 *
 * @author 李超()
 * @since 1.0.0
 */
public class HBaseTest1 {

	// 声明静态配置
	static Configuration conf = null;

	static {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "localhost");
	}

	/*
	 * 创建表
	 *
	 * @tableName 表名
	 *
	 * @family 列族列表
	 */
	@Test
	public void creatTable() throws Exception {

		String tableName = "blog2";
		String[] family = {"article", "author"};

		HBaseAdmin admin = new HBaseAdmin(conf);

		HTableDescriptor desc = new HTableDescriptor(tableName);
		for (int i = 0; i < family.length; i++) {
			desc.addFamily(new HColumnDescriptor(family[i]));
		}
		if (admin.tableExists(tableName)) {
			System.out.println("table Exists!");
			System.exit(0);
		} else {
			admin.createTable(desc);
			System.out.println("create table Success!");
		}
	}

	@Test
	public void testAddData() throws IOException {
		// 为表添加数据

		String[] column1 = {"title", "content", "tag"};
		String[] value1 = {
				"Head First HBase",
				"HBase is the Hadoop database. Use it when you need random, realtime read/write access to your Big Data.",
				"Hadoop,HBase,NoSQL"};
		String[] column2 = {"name", "nickname"};
		String[] value2 = {"nicholas", "lee"};
		addData("rowkey1", "blog2", column1, value1, column2, value2);
		addData("rowkey2", "blog2", column1, value1, column2, value2);
		addData("rowkey3", "blog2", column1, value1, column2, value2);
	}

	/*
	 * 为表添加数据（适合知道有多少列族的固定表）
	 *
	 * @rowKey rowKey
	 *
	 * @tableName 表名
	 *
	 * @column1 第一个列族列表
	 *
	 * @value1 第一个列的值的列表
	 *
	 * @column2 第二个列族列表
	 *
	 * @value2 第二个列的值的列表
	 */
	private static void addData(String rowKey, String tableName,
								String[] column1, String[] value1, String[] column2, String[] value2)
			throws IOException {

		Put put = new Put(Bytes.toBytes(rowKey));// 设置rowkey
		HTable table = new HTable(conf, Bytes.toBytes(tableName));// HTabel负责跟记录相关的操作如增删改查等//
		// 获取表
		HColumnDescriptor[] columnFamilies = table.getTableDescriptor() // 获取所有的列族
				.getColumnFamilies();

		for (int i = 0; i < columnFamilies.length; i++) {
			String familyName = columnFamilies[i].getNameAsString(); // 获取列族名
			if (familyName.equals("article")) { // article列族put数据
				for (int j = 0; j < column1.length; j++) {
					put.add(Bytes.toBytes(familyName),
							Bytes.toBytes(column1[j]), Bytes.toBytes(value1[j]));
				}
			}
			if (familyName.equals("author")) { // author列族put数据
				for (int j = 0; j < column2.length; j++) {
					put.add(Bytes.toBytes(familyName),
							Bytes.toBytes(column2[j]), Bytes.toBytes(value2[j]));
				}
			}
		}
		table.put(put);
		System.out.println("add data Success!");
	}

	/*
	 * 根据rwokey查询
	 *
	 * @rowKey rowKey
	 *
	 * @tableName 表名
	 */
	@Test
	public void getResult() throws IOException {
		// 查询
		String tableName = "blog2";
		String rowKey = "rowkey1";

		Get get = new Get(Bytes.toBytes(rowKey));
		HTable table = new HTable(conf, Bytes.toBytes(tableName));// 获取表
		Result result = table.get(get);
		for (KeyValue kv : result.list()) {
			System.out.println("family:" + Bytes.toString(kv.getFamily()));
			System.out.println("qualifier:" + Bytes.toString(kv.getQualifier()));
			System.out.println("value:" + Bytes.toString(kv.getValue()));
			System.out.println("Timestamp:" + kv.getTimestamp());
			System.out.println("-------------------------------------------");
		}
	}

	/*
	 * 遍历查询hbase表
	 *
	 * @tableName 表名
	 */
	@Test
	public void getResultScann() throws IOException {
		String tableName = "blog2";

		Scan scan = new Scan();
		ResultScanner rs = null;
		HTable table = new HTable(conf, Bytes.toBytes(tableName));
		try {
			rs = table.getScanner(scan);
			for (Result r : rs) {
				for (KeyValue kv : r.list()) {
					System.out.println("row:" + Bytes.toString(kv.getRow()));
					System.out.println("family:" + Bytes.toString(kv.getFamily()));
					System.out.println("qualifier:" + Bytes.toString(kv.getQualifier()));
					System.out.println("value:" + Bytes.toString(kv.getValue()));
					System.out.println("timestamp:" + kv.getTimestamp());
					System.out.println("-------------------------------------------");
				}
			}
		} finally {
			rs.close();
		}
	}

	@Test
	public void getResultScan() throws IOException {
		// 遍历查询
		getResultScann("blog2", "rowkey4", "rowkey5");
	}

	/*
	 * 遍历查询hbase表
	 *
	 * @tableName 表名
	 */
	public static void getResultScann(String tableName, String start_rowkey,
									  String stop_rowkey) throws IOException {
		Scan scan = new Scan();
		scan.setStartRow(Bytes.toBytes(start_rowkey));
		scan.setStopRow(Bytes.toBytes(stop_rowkey));
		ResultScanner rs = null;
		HTable table = new HTable(conf, Bytes.toBytes(tableName));
		try {
			rs = table.getScanner(scan);
			for (Result r : rs) {
				for (KeyValue kv : r.list()) {
					System.out.println("row:" + Bytes.toString(kv.getRow()));
					System.out.println("family:"
							+ Bytes.toString(kv.getFamily()));
					System.out.println("qualifier:"
							+ Bytes.toString(kv.getQualifier()));
					System.out
							.println("value:" + Bytes.toString(kv.getValue()));
					System.out.println("timestamp:" + kv.getTimestamp());
					System.out
							.println("-------------------------------------------");
				}
			}
		} finally {
			rs.close();
		}
	}

	/*
	 * 查询表中的某一列
	 *
	 * @tableName 表名
	 *
	 * @rowKey rowKey
	 */
	@Test
	public void getResultByColumn() throws IOException {
		String tableName = "blog2";
		String rowKey = "rowkey1";
		String familyName = "author";
		String columnName = "name";

		HTable table = new HTable(conf, Bytes.toBytes(tableName));
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName)); // 获取指定列族和列修饰符对应的列
		Result result = table.get(get);
		for (KeyValue kv : result.list()) {
			System.out.println("family:" + Bytes.toString(kv.getFamily()));
			System.out.println("qualifier:" + Bytes.toString(kv.getQualifier()));
			System.out.println("value:" + Bytes.toString(kv.getValue()));
			System.out.println("Timestamp:" + kv.getTimestamp());
			System.out.println("-------------------------------------------");
		}
	}

	/*
	 * 更新表中的某一列
	 *
	 * @tableName 表名
	 *
	 * @rowKey rowKey
	 *
	 * @familyName 列族名
	 *
	 * @columnName 列名
	 *
	 * @value 更新后的值
	 */
	@Test
	public void updateTable() throws IOException {

		String tableName = "blog2";
		String rowKey = "rowkey1";
		String familyName = "author";
		String columnName = "name";
		String value = "bin1";


		HTable table = new HTable(conf, Bytes.toBytes(tableName));
		Put put = new Put(Bytes.toBytes(rowKey));
		put.add(Bytes.toBytes(familyName), Bytes.toBytes(columnName),
				Bytes.toBytes(value));
		table.put(put);
		System.out.println("update table Success!");
	}

	/*
	 * 查询某列数据的多个版本
	 *
	 * @tableName 表名
	 *
	 * @rowKey rowKey
	 *
	 * @familyName 列族名
	 *
	 * @columnName 列名
	 */
	@Test
	public void getResultByVersion() throws IOException {

		String tableName = "blog2";
		String rowKey = "rowkey1";
		String familyName = "author";
		String columnName = "name";

		HTable table = new HTable(conf, Bytes.toBytes(tableName));
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
		get.setMaxVersions(5);
		Result result = table.get(get);
		for (KeyValue kv : result.list()) {
			System.out.println("family:" + Bytes.toString(kv.getFamily()));
			System.out.println("qualifier:" + Bytes.toString(kv.getQualifier()));
			System.out.println("value:" + Bytes.toString(kv.getValue()));
			System.out.println("Timestamp:" + kv.getTimestamp());
			System.out.println("-------------------------------------------");
		}
	}

	/*
	 * 删除指定的列
	 *
	 * @tableName 表名
	 *
	 * @rowKey rowKey
	 *
	 * @familyName 列族名
	 *
	 * @columnName 列名
	 */
	@Test
	public void deleteColumn() throws IOException {

		String tableName = "blog2";
		String rowKey = "rowkey1";
		String falilyName = "author";
		String columnName = "nickname";

		HTable table = new HTable(conf, Bytes.toBytes(tableName));
		Delete deleteColumn = new Delete(Bytes.toBytes(rowKey));
		deleteColumn.deleteColumns(Bytes.toBytes(falilyName),
				Bytes.toBytes(columnName));
		table.delete(deleteColumn);
		System.out.println(falilyName + ":" + columnName + "is deleted!");
	}

	/*
	 * 删除指定的列
	 *
	 * @tableName 表名
	 *
	 * @rowKey rowKey
	 */
	@Test
	public void deleteAllColumn() throws IOException {

		String tableName = "blog2";
		String rowKey = "rowkey1";

		HTable table = new HTable(conf, Bytes.toBytes(tableName));
		Delete deleteAll = new Delete(Bytes.toBytes(rowKey));
		table.delete(deleteAll);
		System.out.println("all columns are deleted!");
	}

	/*
	 * 删除表
	 *
	 * @tableName 表名
	 */
	@Test
	public void deleteTable() throws IOException {

		String tableName = "blog2";

		HBaseAdmin admin = new HBaseAdmin(conf);
		admin.disableTable(tableName);
		admin.deleteTable(tableName);
		System.out.println(tableName + "is deleted!");
	}

}
