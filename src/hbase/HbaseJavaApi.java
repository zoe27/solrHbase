package hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseJavaApi {
	
	public static HConnection connection;
	
	/*static {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "10.100.35.37");
		try {
			connection = HConnectionManager.createConnection(conf);
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}*/

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
	    
	    Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.100.35.37");
		
		long time = System.currentTimeMillis();
		//crateTable(conf, "Second");
		//addData(null, "Second");
		long time2 = System.currentTimeMillis();
		System.out.println(time2 - time);
		
		//getData(conf);
		long time3 = System.currentTimeMillis();
		System.out.println("================"+(time3 - time2) );
		
		//batchAddData(conf);
	    getDataByFilter(conf);
		//getDataByCloumFilter(conf);
		//getDataMultiplyFilter(conf);
		//getDataByRowCloumFilter(conf);
		System.out.println(System.currentTimeMillis() - time3);
		//getDataByTime(conf);
	}
	
	/**
	 * 插入数据
	 * @param conf
	 * @throws IOException
	 */
	public static void addData(Configuration conf, String tableName) throws IOException {
		//HTable table = new HTable(conf, tableName);

		HTableInterface table = connection.getTable(tableName);
		Put put = new Put(Bytes.toBytes("4"));

		put.add(Bytes.toBytes("info"), Bytes.toBytes("qual1"),
				Bytes.toBytes("val1"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("qual2"),
				Bytes.toBytes("val2"));
		
		put.add(Bytes.toBytes("info"), Bytes.toBytes("age"),
				Bytes.toBytes("27")); 
		put.add(Bytes.toBytes("info"), Bytes.toBytes("name"),
				Bytes.toBytes("tina	"));
		
		put.add(Bytes.toBytes("info"), Bytes.toBytes("sex"),
				Bytes.toBytes("man"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("tel"),
				Bytes.toBytes("13688888888"));
		
		put.add(Bytes.toBytes("info"), Bytes.toBytes("company"),
				Bytes.toBytes("google"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("country"),
				Bytes.toBytes("China")); 
		
		put.add(Bytes.toBytes("info"), Bytes.toBytes("addtime"),
				Bytes.toBytes("2015-09-12 09:09:09"));

		table.put(put);
	}
	

	
	/**
	 * 批量插入数据
	 * @param conf
	 * @throws IOException
	 */
	public static void batchAddData(Configuration conf) throws IOException{
		HTable table = new HTable(conf, "user");
		
		for (int i = 11351750; i < 30000000; i++) {
			Put put = new Put(Bytes.toBytes("row"+i));

			put.add(Bytes.toBytes("info"), Bytes.toBytes("qual1"),
					Bytes.toBytes("val1"+i));
			put.add(Bytes.toBytes("info"), Bytes.toBytes("qual2"),
					Bytes.toBytes("val2"+i));
			
			put.add(Bytes.toBytes("info"), Bytes.toBytes("age"),
					Bytes.toBytes("27"+i));
			put.add(Bytes.toBytes("info"), Bytes.toBytes("name"),
					Bytes.toBytes("zoe"+i));
			
			put.add(Bytes.toBytes("info"), Bytes.toBytes("sex"),
					Bytes.toBytes("man"+i));
			put.add(Bytes.toBytes("info"), Bytes.toBytes("tel"),
					Bytes.toBytes("13688888888"+i));
			
			put.add(Bytes.toBytes("info"), Bytes.toBytes("company"),
					Bytes.toBytes("google"+i));
			put.add(Bytes.toBytes("info"), Bytes.toBytes("country"),
					Bytes.toBytes("China"+i)); 
			
			put.add(Bytes.toBytes("info"), Bytes.toBytes("addtime"),
					Bytes.toBytes("2015-09-12 09:"+ i%60 +":09"));

			table.put(put);
			System.out.println("insert data =========== " + i);
		}
	}
	
	
	/**
	 * 读取数据
	 * @param conf
	 * @throws IOException
	 */
	public static void getData(Configuration conf) throws IOException{
		HTable table = new HTable(conf, "table");
		Get get = new Get("row1515151".getBytes()); 
		List<Get> list = new ArrayList<Get>();
		for (int i = 0; i <4000; i++) {
			Get get2 = new Get(("row11"+ new Random().nextInt(400000)).getBytes());
			list.add(get2);
		}
		///get.addFamily(Bytes.toBytes("info"));
		/*Filter filter1 = new RowFilter(CompareOp.EQUAL,
				new SubstringComparator("row1515151"));
		get.setFilter(filter1);*/
		long t = System.currentTimeMillis();
		Result[] result = table.get(list);
		System.out.println("--------:" + (System.currentTimeMillis() - t));
		
		int size = result.length;
 
		System.out.println(size);
		//byte[] cloum = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"));
		System.out.println(result.toString());
		//System.out.println(Bytes.toString(cloum));
	}
	
	/**
	 * 创建表
	 * @param conf
	 */
	public static void crateTable(Configuration conf, String tableName){
		 try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			if (admin.tableExists(tableName)) {
				System.out.println("table exists, trying recreate table!");
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
				// return;
			}
			HTableDescriptor htd = new HTableDescriptor(tableName);
			HColumnDescriptor col = new HColumnDescriptor("info");
			htd.addFamily(col);
			admin.createTable(htd);
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/**
	 * 条件查询, 行过滤， 二进制比较
	 * @param conf
	 * @throws IOException 
	 */
	public static void getDataByFilter(Configuration conf) throws IOException{
		HTable table = new HTable(conf, "calllist0");
		/*// 扫描器
		Scan scan = new Scan();
		// 过滤器
		Filter filter = new RowFilter(CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes("row15")));
		scan.setFilter(filter);*/
		
		Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("caller"));
        scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("callee"));
		
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.out.println(result);
		}
		scanner.close();
	}
	
	/**
	 * 条件查询，列簇
	 * @param conf
	 * @throws IOException
	 */
	public static void getDataByCloumFilter(Configuration conf) throws IOException{
		HTable table = new HTable(conf, "user");
		// 扫描器
		Scan scan = new Scan();
		// 过滤器
		Filter filter = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("info")));
	 
		scan.setFilter(filter);	 
 
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.out.println(result);
		}
		scanner.close();
	}
	
	
	/**
	 * 条件查询，某一行某一列簇
	 * @param conf
	 * @throws IOException
	 */
	public static void getDataByRowCloumFilter(Configuration conf) throws IOException{
		HTable table = new HTable(conf, "user");
		// 扫描器
		Scan scan = new Scan();
		// 过滤器
		Filter filter = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("info")));
		Filter filter1 = new RowFilter(CompareOp.EQUAL,
				new SubstringComparator("row1515151"));
		
		List<Filter> list = new ArrayList<Filter>();
		list.add(filter1);
		list.add(filter);
		
		FilterList filterList = new FilterList(list);
		scan.setFilter(filterList);
		
		long begin = System.currentTimeMillis();
		 
		ResultScanner scanner = table.getScanner(scan);
		long endTime = System.currentTimeMillis();
		
		System.out.println("查询时间： "+ (endTime - begin));
		int i = 0;  
		
		for (Result result : scanner) {
			System.out.println(result);
			System.out.println(); 
			i++;
			long getOneTime = System.currentTimeMillis();
			System.out.println(getOneTime - endTime);
			endTime = getOneTime;
			
		}
		long getOneTime = System.currentTimeMillis();
		System.out.println(getOneTime - begin);
		System.out.println("共有数据: "+i);
 
		scanner.close();
	}
	
	public static void getDataMultiplyFilter(Configuration conf) throws IOException{
		HTable table = new HTable(conf, "user");
		// 扫描器
		Scan scan = new Scan();
		
		SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("name"), CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("zoe")));
		filter1.setFilterIfMissing(true);
		SingleColumnValueFilter filter2 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("company"), CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("google")));
		filter2.setFilterIfMissing(true);
		List<Filter> list = new ArrayList<Filter>();
		list.add(filter1);
		list.add(filter2);
		
		FilterList filterList = new FilterList(list);
		scan.setFilter(filterList);
 
		ResultScanner resultScanner = table.getScanner(scan);
		 
		long t = System.currentTimeMillis();
		for (Result result : resultScanner) {
			for (KeyValue kv : result.raw()) {
				byte[] cloum = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"));
				System.out.print("KV: " + kv + "    value: " + Bytes.toString(kv.getValue()));			
			}
			System.out.println();
		}
		System.out.println("多条件查询耗时："+(System.currentTimeMillis() - t));
 
		resultScanner.close();
	}
	
	
	public static void getDataByTime(Configuration conf) throws IOException{
		HTable table = new HTable(conf, "user");
		// 扫描器
		Scan scan = new Scan();
		// 2015-09-12 09:38:09
		/*SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("name"), CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("zoe11351750")));
		filter1.setFilterIfMissing(true);*/
		SingleColumnValueFilter filter2 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("name"), CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("zoe11351740")));
		filter2.setFilterIfMissing(true);
		List<Filter> list = new ArrayList<Filter>();
		//list.add(filter1);
		list.add(filter2);
		
		FilterList filterList = new FilterList(list);
		scan.setFilter(filterList);
		
		long begin = System.currentTimeMillis();
 
		ResultScanner resultScanner = table.getScanner(scan);
		long endTime = System.currentTimeMillis();
		
		System.out.println("查询时间： "+ (endTime - begin));
		int i = 0;
		 
		 
		for (Result result : resultScanner) {
			for (KeyValue kv : result.raw()) {
				// byte[] cloum = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"));
				System.out.print("KV: " + kv + "    value: " + Bytes.toString(kv.getValue()));			
			}
			System.out.println(); 
			i++;
			long getOneTime = System.currentTimeMillis();
			System.out.println(getOneTime - endTime);
			endTime = getOneTime;
		}
		long getOneTime = System.currentTimeMillis();
		System.out.println(getOneTime - begin);
		System.out.println("共有数据: "+i);
		resultScanner.close();
	}

}
