package solr;

import java.io.IOException;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

public class SolrIndex {

	private static HttpSolrServer server;
	
	private static final String DEFAULT_URL = "http://10.100.35.37:8082/solr/";
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
        getSolr();
        addDoc();
	}
	
	public static void getSolr(){
		server = new HttpSolrServer(DEFAULT_URL);
	}
	
	 /**
     * 添加文档
     */
    public static void addDoc() {
 
        SolrInputDocument doc = new SolrInputDocument();
 
        doc.addField("id", "1111");
        doc.addField("title", "小说");
        doc.addField("description", "this is my document !!");
 
        try {
 
            UpdateResponse response = server.add(doc);
            // 提交
            server.commit();
 
            System.out.println("########## Query Time :" + response.getQTime());
            System.out.println("########## Elapsed Time :" + response.getElapsedTime());
            System.out.println("########## Status :" + response.getStatus());
 
        } catch ( IOException e) {
        	System.out.println( e.getStackTrace());
        } catch (SolrServerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

}
