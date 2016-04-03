package hbase;

import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
 

public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		/*String  string = "hhuui";
		byte[] b = string.getBytes();
		for (byte c : b) {
			System.out.println(c);
		}
		System.out.println(b);
		
		String[] strs = new String[]{"abfds1", "advesd2", "dasfdsa3", "cdsaew1", "abbdsa3", "abbdsa2", "abbdsa"};  
		Arrays.sort(strs);  
		for(String str : strs) {  
		    System.out.println(str);  
		}  
		
		List<String> list = new ArrayList<String>();
		System.out.println(list.size());
*/
       /* String s = "fffffffffffffffffffffffffffffff";
        long t = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            s.replace("ffffffffffffffffffffffffffff", "xxxxxxxxxxxxxxxxxxxxxxxx");
        }
        
        System.out.println(System.currentTimeMillis() - t);*/
	    System.out.println("19876".hashCode());
	    
	    String prefix = String.format("%03d", "19876".hashCode() % 1000);
	    System.out.println(prefix);

	    String matchRex = "[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}";
	    String time = "2015-12-12 12012";
	    System.out.println(time.matches(matchRex));

        
        
	}

}
