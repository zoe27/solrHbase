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

import com.sun.org.apache.bcel.internal.generic.LSTORE;

public class SolrHbaseQuery {

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

	 
		getHsolr();
		//getDataMultiplyFilter(null);
		//getDateByRow();
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
		SolrQuery query = new SolrQuery("manu:[val22222 TO val22224]");
		//query.setFields("id");
		query.setStart(0); // 数据起始行，分页用
		query.setRows(Integer.MAX_VALUE); // 返回记录数，分页用
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
		
		System.out.println("hbase 查询数据耗时 " + (System.currentTimeMillis() - t));

		byte[] bt1 = null;
		byte[] bt2 = null;
		byte[] bt3 = null;
		byte[] bt4 = null;
		String str1 = null;
		String str2 = null;
		String str3 = null;
		String str4 = null;
		/*for (Result rs : res) {
			for (KeyValue kv : rs.raw()) {
				//byte[] cloum = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"));
				System.out.print("  KV: " + kv + "    value: " + Bytes.toString(kv.getValue()));			
			}
			bt1 = rs.getValue(Bytes.toBytes("hslor"), Bytes.toBytes("inStock"));
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
		}*/
		long time = System.currentTimeMillis() - t;
		System.out.println("处理时间为： " + time);
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
				System.out.print("  KV: " + kv + "    value: " + Bytes.toString(kv.getValue()));			
			}
			System.out.println();
		}
 
		resultScanner.close();
		long time = System.currentTimeMillis() - t;
		System.out.println("hbase 耗时为:" + time);
		System.out.println("");
	}
	
	public static void getDateByRow() throws IOException{
		final Configuration conf;
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "10.100.35.37");
		HTable table = new HTable(conf, TABLE);
		
		List<Get> list = new ArrayList<Get>();
		
		Get get = new Get(Bytes.toBytes("row22"));
		Get get1 = new Get(Bytes.toBytes("row23"));
		list.add(get);
		list.add(get1);
		Result[] result = table.get(list);
		for (Result result2 : result) {
			for (KeyValue kv : result2.raw()) {
				//byte[] cloum = result2.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"));
				System.out.print("  KV: " + kv + "    value: " + Bytes.toString(kv.getValue()));			
			}
			System.out.println();
			
		}
	}

}
