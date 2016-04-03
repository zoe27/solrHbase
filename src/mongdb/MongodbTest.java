package mongdb;

import com.mongodb.Mongo;

public class MongodbTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		testMongDb();
	}
	
	public static void testMongDb(){
		Mongo mg = new Mongo("10.100.35.37", 27017);  
		
	}

}
