package com.wolf.test.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Description:
 * <br/> Created on 2016/9/6 17:16
 *
 * @author 李超()
 * @since 1.0.0
 */
public class HBaseTest2 {

	public static Configuration configuration;

	static {
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		configuration.set("hbase.zookeeper.quorum", "127.0.0.1");
		configuration.set("hbase.master", "127.0.0.1:600000");
	}

	@Test
	public void createTable() {
		String tableName = "testtablbe1";
		System.out.println("start create table ......");
		try {
			HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
			if (hBaseAdmin.tableExists(tableName)) {// 如果存在要创建的表，那么先删除，再创建
				hBaseAdmin.disableTable(tableName);
				hBaseAdmin.deleteTable(tableName);
				System.out.println(tableName + " is exist,detele....");
			}
			HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
			tableDescriptor.addFamily(new HColumnDescriptor("column1"));
			tableDescriptor.addFamily(new HColumnDescriptor("column2"));
			tableDescriptor.addFamily(new HColumnDescriptor("column3"));
			hBaseAdmin.createTable(tableDescriptor);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("end create table ......");
	}

	@Test
	public void insertData() {
		String tableName = "testtablbe1";
		System.out.println("start insert data ......");
		HTablePool pool = new HTablePool(configuration, 1000);
		HTableInterface table = pool.getTable(tableName);
		Put put = new Put("2222bbbcccc".getBytes());// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值
		put.add("column1".getBytes(), "a".getBytes(), "aaa2".getBytes());// 本行数据的第一列
		put.add("column2".getBytes(), "b".getBytes(), "bbb2".getBytes());// 本行数据的第三列
		put.add("column3".getBytes(), "c".getBytes(), "ccc2".getBytes());// 本行数据的第三列
		try {
			table.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("end insert data ......");
	}

	@Test
	public void dropTable() {
		String tableName = "testtablbe1";
		try {
			HBaseAdmin admin = new HBaseAdmin(configuration);
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Test
	public void deleteRow() {
		String tableName = "testtablbe1";
		String rowkey = "112233bbbcccc";
		try {
			HTable table = new HTable(configuration, tableName);
			List list = new ArrayList();
			Delete d1 = new Delete(rowkey.getBytes());
			list.add(d1);

			table.delete(list);
			System.out.println("删除行成功!");

		} catch (IOException e) {
			e.printStackTrace();
		}


	}

	@Test
	public void queryAll() {
		String tableName = "testtablbe1";
		HTablePool pool = new HTablePool(configuration, 1000);
		HTableInterface table = pool.getTable(tableName);
		try {
			ResultScanner rs = table.getScanner(new Scan());
			for (Result r : rs) {
				System.out.println("获得到rowkey:" + new String(r.getRow()));
				for (KeyValue keyValue : r.raw()) {
					System.out.println("列：" + new String(keyValue.getFamily())
							+ "====值:" + new String(keyValue.getValue()));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void queryByCondition1() {

		String tableName = "testtablbe1";
		HTablePool pool = new HTablePool(configuration, 1000);
		HTableInterface table = pool.getTable(tableName);
		try {
			Get scan = new Get("112233bbbcccc".getBytes());// 根据rowkey查询
			Result r = table.get(scan);
			System.out.println("获得到rowkey:" + new String(r.getRow()));
			for (KeyValue keyValue : r.raw()) {
				System.out.println("列：" + new String(keyValue.getFamily())
						+ "====值:" + new String(keyValue.getValue()));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void queryByCondition2() {

		String tableName = "testtablbe1";
		try {
			HTablePool pool = new HTablePool(configuration, 1000);
			HTableInterface table = pool.getTable(tableName);
			Filter filter = new SingleColumnValueFilter(Bytes
					.toBytes("column1"), null, CompareFilter.CompareOp.EQUAL, Bytes
					.toBytes("aaa2")); // 当列column1的值为aaa时进行查询
			Scan s = new Scan();
			s.setFilter(filter);
			ResultScanner rs = table.getScanner(s);
			for (Result r : rs) {
				System.out.println("获得到rowkey:" + new String(r.getRow()));
				for (KeyValue keyValue : r.raw()) {
					System.out.println("列：" + new String(keyValue.getFamily())
							+ "====值:" + new String(keyValue.getValue()));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Test
	public void queryByCondition3() {

		String tableName = "testtablbe1";
		try {
			HTablePool pool = new HTablePool(configuration, 1000);
			HTableInterface table = pool.getTable(tableName);

			List<Filter> filters = new ArrayList<Filter>();

			Filter filter1 = new SingleColumnValueFilter(Bytes
					.toBytes("column1"), null, CompareFilter.CompareOp.EQUAL, Bytes
					.toBytes("aaa"));
			filters.add(filter1);

			Filter filter2 = new SingleColumnValueFilter(Bytes
					.toBytes("column2"), null, CompareFilter.CompareOp.EQUAL, Bytes
					.toBytes("bbb"));
			filters.add(filter2);

			Filter filter3 = new SingleColumnValueFilter(Bytes
					.toBytes("column3"), null, CompareFilter.CompareOp.EQUAL, Bytes
					.toBytes("ccc"));
			filters.add(filter3);

			FilterList filterList1 = new FilterList(filters);

			Scan scan = new Scan();
			scan.setFilter(filterList1);
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				System.out.println("获得到rowkey:" + new String(r.getRow()));
				for (KeyValue keyValue : r.raw()) {
					System.out.println("列：" + new String(keyValue.getFamily())
							+ "====值:" + new String(keyValue.getValue()));
				}
			}
			rs.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}


	@Test
	public void selectRowKey() throws IOException {

		String tableName = "testtablbe1";
		String rowKey = "2222bbbcccc";

		HTable table = new HTable(configuration, tableName);
		Get g = new Get(rowKey.getBytes());
		Result rs = table.get(g);

		for (KeyValue kv : rs.raw()) {
			System.out.println("--------------------" + new String(kv.getRow()) + "----------------------------");
			System.out.println("Column Family: " + new String(kv.getFamily()));
			System.out.println("Column       :" + new String(kv.getQualifier()));
			System.out.println("value        : " + new String(kv.getValue()));
		}
	}

	@Test
	public void selectRowKeyFamily() throws IOException {
		String tableName = "testtablbe1";
		String rowKey = "2222bbbcccc";
		String family = "column1";

		HTable table = new HTable(configuration, tableName);
		Get g = new Get(rowKey.getBytes());
		g.addFamily(Bytes.toBytes(family));
		Result rs = table.get(g);
		for (KeyValue kv : rs.raw()) {
			System.out.println("--------------------" + new String(kv.getRow()) + "----------------------------");
			System.out.println("Column Family: " + new String(kv.getFamily()));
			System.out.println("Column       :" + new String(kv.getQualifier()));
			System.out.println("value        : " + new String(kv.getValue()));
		}
	}

	@Test
	public void selectRowKeyFamilyColumn()
			throws IOException {

		String tableName = "testtablbe1";
		String rowKey = "2222bbbcccc";
		String family = "column1";
		String column = "a";

		HTable table = new HTable(configuration, tableName);
		Get g = new Get(rowKey.getBytes());
		g.addColumn(family.getBytes(), column.getBytes());

		Result rs = table.get(g);

		for (KeyValue kv : rs.raw()) {
			System.out.println("--------------------" + new String(kv.getRow()) + "----------------------------");
			System.out.println("Column Family: " + new String(kv.getFamily()));
			System.out.println("Column       :" + new String(kv.getQualifier()));
			System.out.println("value        : " + new String(kv.getValue()));
		}
	}

	@Test
	public void selectFilter() throws IOException {

		String tableName = "testtablbe1";
		List<String> arr = new ArrayList<String>();
		arr.add("column1,a,aaa2");
		arr.add("column2,b,bbb2");

		HTable table = new HTable(configuration, tableName);
		Scan scan = new Scan();// 实例化一个遍历器
		FilterList filterList = new FilterList(); // 过滤器List

		for (String v : arr) { // 下标0为列簇，1为列名，3为条件
			String[] wheres = v.split(",");

			// 各个条件之间是" and "的关系
			filterList.addFilter(new SingleColumnValueFilter(// 过滤器
					wheres[0].getBytes(), wheres[1].getBytes(),
					CompareFilter.CompareOp.EQUAL,
					wheres[2].getBytes()));
		}
		scan.setFilter(filterList);
		ResultScanner ResultScannerFilterList = table.getScanner(scan);
		for (Result rs = ResultScannerFilterList.next(); rs != null; rs = ResultScannerFilterList.next()) {
			for (KeyValue kv : rs.list()) {
				System.out.println("--------------------" + new String(kv.getRow()) + "----------------------------");
				System.out.println("Column Family: " + new String(kv.getFamily()));
				System.out.println("Column       :" + new String(kv.getQualifier()));
				System.out.println("value        : " + new String(kv.getValue()));
			}
		}
	}


}
