package solr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

public class SolrHbase {

	private static final String DEFAULT_URL = "http://10.100.35.37:8082/solr/";
	private static final String TABLE = "solr";

	/**
	 * @param args
	 * @throws SolrServerException
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException,
			SolrServerException {
		// TODO Auto-generated method stub

		//addDate();
		//getHsolr();
		//getDataMultiplyFilter(null);
		//addBathData();
		SolrHbase sHbase = new SolrHbase();
		sHbase.new ThreadInner(50000, 550000).start();
		sHbase.new ThreadInner(550000, 1050000).start();
		sHbase.new ThreadInner(2000000, 2500000).start();
		sHbase.new ThreadInner(2500000, 3000000).start();
		sHbase.new ThreadInner(3000000, 3500000).start();
		sHbase.new ThreadInner(3500000, 4000000).start();
		
		sHbase.new ThreadInner(4500000, 5000000).start();
		sHbase.new ThreadInner(5500000, 6000000).start();
		sHbase.new ThreadInner(6500000, 7000000).start();
		sHbase.new ThreadInner(7500000, 8000000).start();
	}
	
	class ThreadInner extends Thread {
		private int start;
		private int end;

		public ThreadInner(int start, int end) {
			this.start = start;
			this.end = end;
		}

		public void run() {
			final Configuration conf;
			HttpSolrServer solrServer = new HttpSolrServer(DEFAULT_URL); // 因为服务端是用的Solr自带的jetty容器，默认端口号是8983

			conf = HBaseConfiguration.create();
			conf.set("hbase.zookeeper.quorum", "10.100.35.37");
			HTable table = null;
			try {
				table = new HTable(conf, TABLE);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} // 这里指定HBase表名称
			for (int i = start; i < end; i++) {
				Put put = new Put(Bytes.toBytes("row" + i));

				put.add(Bytes.toBytes("hsolr"), Bytes.toBytes("id"),
						Bytes.toBytes("111" + i));
				put.add(Bytes.toBytes("hsolr"), Bytes.toBytes("manu"),
						Bytes.toBytes("val2" + i));

				put.add(Bytes.toBytes("hsolr"), Bytes.toBytes("features"),
						Bytes.toBytes("27" + i));
				put.add(Bytes.toBytes("hsolr"), Bytes.toBytes("name"),
						Bytes.toBytes("zoe" + i));

				put.add(Bytes.toBytes("hsolr"), Bytes.toBytes("includes"),
						Bytes.toBytes("man" + i));
				put.add(Bytes.toBytes("hsolr"), Bytes.toBytes("price"),
						Bytes.toBytes("13688888888" + i));

				put.add(Bytes.toBytes("hsolr"), Bytes.toBytes("cat"),
						Bytes.toBytes("google" + i));
				put.add(Bytes.toBytes("hsolr"), Bytes.toBytes("inStock"),
						Bytes.toBytes("China" + i));

				put.add(Bytes.toBytes("hsolr"), Bytes.toBytes("keywords"),
						Bytes.toBytes("2015-09-12 09:" + (i % 60) + ":09"));

				try {
					table.put(put);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				SolrInputDocument solrDoc = new SolrInputDocument();
				solrDoc.addField("id", new String("row" + i));
				solrDoc.addField("manu", "val2" + i);
				solrDoc.addField("includes", "man" + i);
				solrDoc.addField("name", "zoe" + i);
				solrDoc.addField("price", "13688888888" + i);
				solrDoc.addField("cat", "google" + i);
				solrDoc.addField("inStock", "China" + i);
				solrDoc.addField("keywords", "2015-09-12 09:" + (i % 60)
						+ ":09");
				solrDoc.addField("features", "27" + i);
				try {
					solrServer.add(solrDoc);
					solrServer.commit(true, true, true);
				} catch (SolrServerException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println("已经处理第 " + i + " 条记录");
			}
			try {
				table.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	/**
	 * 插入大量数据
	 * @return 
	 * @throws IOException 
	 * @throws SolrServerException 
	 */
	public static void addBathData() throws IOException, SolrServerException {
		final Configuration conf;
		HttpSolrServer solrServer = new HttpSolrServer(DEFAULT_URL); // 因为服务端是用的Solr自带的jetty容器，默认端口号是8983

		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "10.100.35.37");
		HTable table = new HTable(conf, TABLE); // 这里指定HBase表名称

		for (int i = 0; i < 5000000; i++) {
			Put put = new Put(Bytes.toBytes("row" + i));

			put.add(Bytes.toBytes("hsolr"), Bytes.toBytes("id"),
					Bytes.toBytes("111" + i));
			put.add(Bytes.toBytes("hsolr"), Bytes.toBytes("manu"),
					Bytes.toBytes("val2" + i));

			put.add(Bytes.toBytes("hsolr"), Bytes.toBytes("features"),
					Bytes.toBytes("27" + i));
			put.add(Bytes.toBytes("hsolr"), Bytes.toBytes("name"),
					Bytes.toBytes("zoe" + i));

			put.add(Bytes.toBytes("hsolr"), Bytes.toBytes("includes"),
					Bytes.toBytes("man" + i));
			put.add(Bytes.toBytes("hsolr"), Bytes.toBytes("price"),
					Bytes.toBytes("13688888888" + i));

			put.add(Bytes.toBytes("hsolr"), Bytes.toBytes("cat"),
					Bytes.toBytes("google" + i));
			put.add(Bytes.toBytes("hsolr"), Bytes.toBytes("inStock"),
					Bytes.toBytes("China" + i));

			put.add(Bytes.toBytes("hsolr"), Bytes.toBytes("keywords"),
					Bytes.toBytes("2015-09-12 09:" + (i % 60) + ":09"));

			table.put(put);

			Scan scan = new Scan();
			Filter filter = new RowFilter(CompareOp.EQUAL,
					new BinaryComparator(Bytes.toBytes("row" + i)));
			scan.setFilter(filter);
			ResultScanner ss = table.getScanner(scan);

			System.out.println("start ...");

			for (Result r : ss) {
				SolrInputDocument solrDoc = new SolrInputDocument();
				solrDoc.addField("id", new String(r.getRow()));
				for (KeyValue kv : r.raw()) {
					String fieldName = new String(kv.getQualifier());
					String fieldValue = new String(kv.getValue());
					if (fieldName.equalsIgnoreCase("manu")
							|| fieldName.equalsIgnoreCase("includes")
							|| fieldName.equalsIgnoreCase("name")
							|| fieldName.equalsIgnoreCase("price")
							|| fieldName.equalsIgnoreCase("cat")
							|| fieldName.equalsIgnoreCase("inStock")
							|| fieldName.equalsIgnoreCase("keywords")
							|| fieldName.equalsIgnoreCase("features")) {
						solrDoc.addField(fieldName, fieldValue);
					}
				}
				solrServer.add(solrDoc);
				solrServer.commit(true, true, true);
			}

			System.out.println("已经成功处理 " + i + " 条数据");
			ss.close();
		}
		table.close();
		System.out.println("done !");
	}

	public static void addDate() throws IOException, SolrServerException {
		final Configuration conf;
		HttpSolrServer solrServer = new HttpSolrServer(DEFAULT_URL); // 因为服务端是用的Solr自带的jetty容器，默认端口号是8983

		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "10.100.35.37");
		HTable table = new HTable(conf, TABLE); // 这里指定HBase表名称

		Put put = new Put(Bytes.toBytes("111"));
		put.add(Bytes.toBytes("hsolr"), Bytes.toBytes("id"),
				Bytes.toBytes("111"));
		put.add(Bytes.toBytes("hsolr"), Bytes.toBytes("manu"),
				Bytes.toBytes("val2"));

		put.add(Bytes.toBytes("hsolr"), Bytes.toBytes("features"),
				Bytes.toBytes("27"));
		put.add(Bytes.toBytes("hsolr"), Bytes.toBytes("name"),
				Bytes.toBytes("zoe"));

		put.add(Bytes.toBytes("hsolr"), Bytes.toBytes("includes"),
				Bytes.toBytes("man"));
		put.add(Bytes.toBytes("hsolr"), Bytes.toBytes("price"),
				Bytes.toBytes("13688888888"));

		put.add(Bytes.toBytes("hsolr"), Bytes.toBytes("cat"),
				Bytes.toBytes("google"));
		put.add(Bytes.toBytes("hsolr"), Bytes.toBytes("inStock"),
				Bytes.toBytes("China"));

		put.add(Bytes.toBytes("hsolr"), Bytes.toBytes("keywords"),
				Bytes.toBytes("2015-09-12 09:09:09"));

		table.put(put);

		Scan scan = new Scan();
		Filter filter = new RowFilter(CompareOp.EQUAL, new BinaryComparator(
				Bytes.toBytes("111")));
		scan.setFilter(filter);
		ResultScanner ss = table.getScanner(scan);

		System.out.println("start ...");
		int i = 0;
		try {
			for (Result r : ss) {
				SolrInputDocument solrDoc = new SolrInputDocument();
				solrDoc.addField("id", new String(r.getRow()));
				for (KeyValue kv : r.raw()) {
					String fieldName = new String(kv.getQualifier());
					String fieldValue = new String(kv.getValue());
					if (fieldName.equalsIgnoreCase("manu")
							|| fieldName.equalsIgnoreCase("includes")
							|| fieldName.equalsIgnoreCase("name")
							|| fieldName.equalsIgnoreCase("price")
							|| fieldName.equalsIgnoreCase("cat")
							|| fieldName.equalsIgnoreCase("inStock")
							|| fieldName.equalsIgnoreCase("keywords")
							|| fieldName.equalsIgnoreCase("features")) {
						solrDoc.addField(fieldName, fieldValue);
					}
				}
				solrServer.add(solrDoc);
				solrServer.commit(true, true, true);
				i = i + 1;
				System.out.println("已经成功处理 " + i + " 条数据");
			}
			ss.close();
			table.close();
			System.out.println("done !");
		} catch (IOException e) {
		} finally {
			ss.close();
			table.close();
			System.out.println("erro !");
		}
	}

	/*
	 * 从hbase查询数据
	 * 备注： 5W数据量，条件查询一条， hbase 耗时为:141
	 *                 hbase+solr查询： 4
	 */
	public static void getHsolr() throws IOException, SolrServerException {
		final Configuration conf;
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "10.100.35.37");
		HTable table = new HTable(conf, TABLE);
		Get get = null;
		List<Get> list = new ArrayList<Get>();

		HttpSolrServer server = new HttpSolrServer(DEFAULT_URL);
		SolrQuery query = new SolrQuery("inStock:China AND cat:google");
		query.setStart(0); // 数据起始行，分页用
		query.setRows(10); // 返回记录数，分页用
		QueryResponse response = server.query(query);
		SolrDocumentList docs = response.getResults();
		System.out.println("文档个数：" + docs.getNumFound()); // 数据总条数也可轻易获取
		System.out.println("查询时间：" + response.getQTime());
		for (SolrDocument doc : docs) {
			get = new Get(Bytes.toBytes((String) doc.getFieldValue("id")));
			list.add(get);
		}

		long t = System.currentTimeMillis();
		Result[] res = table.get(list);

		byte[] bt1 = null;
		byte[] bt2 = null;
		byte[] bt3 = null;
		byte[] bt4 = null;
		String str1 = null;
		String str2 = null;
		String str3 = null;
		String str4 = null;
		for (Result rs : res) {
			bt1 = rs.getValue("hslor".getBytes(), "inStock".getBytes());
			bt2 = rs.getValue("hslor".getBytes(), "cat".getBytes());
			bt3 = rs.getValue("hslor".getBytes(), "price".getBytes());
			bt4 = rs.getValue("hslor".getBytes(), "features".getBytes());
			if (bt1 != null && bt1.length > 0) {
				str1 = new String(bt1);
			} else {
				str1 = "无数据";
			} // 对空值进行new String的话会抛出异常
			if (bt2 != null && bt2.length > 0) {
				str2 = new String(bt2);
			} else {
				str2 = "无数据";
			}
			if (bt3 != null && bt3.length > 0) {
				str3 = new String(bt3);
			} else {
				str3 = "无数据";
			}
			if (bt4 != null && bt4.length > 0) {
				str4 = new String(bt4);
			} else {
				str4 = "无数据";
			}
			System.out.print(new String(rs.getRow()) + " ");
			System.out.print(str1 + "|");
			System.out.print(str2 + "|");
			System.out.print(str3 + "|");
			System.out.println(str4 + "|");
		}
		System.out.println("处理时间为： " + (System.currentTimeMillis() - t));
		table.close();
	}
	
	
	public static void getDataMultiplyFilter(Configuration conf) throws IOException{
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "10.100.35.37");
		HTable table = new HTable(conf, "solr");
		// 扫描器
		Scan scan = new Scan();
		
		long t = System.currentTimeMillis();
		SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes("hsolr"), Bytes.toBytes("inStock"), CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("China")));
		filter1.setFilterIfMissing(true);
		SingleColumnValueFilter filter2 = new SingleColumnValueFilter(Bytes.toBytes("hsolr"), Bytes.toBytes("cat"), CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("google")));
		filter2.setFilterIfMissing(true);
		List<Filter> list = new ArrayList<Filter>();
		list.add(filter1);
		list.add(filter2);
		
		FilterList filterList = new FilterList(list);
		scan.setFilter(filterList);
 
		ResultScanner resultScanner = table.getScanner(scan);
		 
		for (Result result : resultScanner) {
			for (KeyValue kv : result.raw()) {
				byte[] cloum = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"));
				System.out.print("KV: " + kv + "    value: " + Bytes.toString(kv.getValue()));			
			}
			System.out.println();
		}
 
		resultScanner.close();
		System.out.println("hbase 耗时为:" + (System.currentTimeMillis() - t));
	}

}
