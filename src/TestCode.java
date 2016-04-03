import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.hbase.generated.master.table_jsp;

/**
 * ≤‚ ‘cod¥˙¬Î
 * 
 * @author Administrator
 * 
 */
public class TestCode {
    
    private String str = "ff";

    public static void main(String[] args) {
        /*Map<String, String> tMap = new HashMap<String, String>();
        setValue(tMap);
        System.out.println(tMap);
        
        String t = "tt";
        name(t);
        System.out.println(t);*/
        // test(null);
        new TestCode().set(null);
    }
    
    public void set( String str){
        this.str = str;
        if (null == str) {
            System.out.println("================");
        }
        System.out.println(str);
        System.out.println(this.str);
    }
    
    public static void test(String[] args){
        if (null != args && args.length != 2) {
            System.out.println("ffff");
            return;
        }
        
        System.out.println("ffff");
    }

    private static long pow(long base, int power) {
        if (power == 0) {
            return 1;
        } else {
            if (power % 2 == 0) { // even
                return pow(base * base, power / 2);
            } else { // odd
                return pow(base * base, (power - 1) / 2) * base;
            }
        }
    }
    
    public static void setValue(Map<String, String> t){
        t.put("1", "1");
        t.put("2", "1");
        t.put("3", "1");
        t.put("4", "1");
    }
    
    public  static void  name(String t) {
        t = "";
    }

}
