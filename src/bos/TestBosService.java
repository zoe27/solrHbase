package bos;

import java.io.File;
import java.io.FileNotFoundException;

import com.baidubce.auth.DefaultBceCredentials;
import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.BosClientConfiguration;
import com.baidubce.services.bos.model.PutObjectResponse;


public class TestBosService {
    public static void main(String[] args) {
        String ACCESS_KEY_ID = "1ce7f0bbc27742afa9231a3e98c3e4ca"; // 用户的Access Key ID
        String SECRET_ACCESS_KEY = "9b61c9ec68d04684a3295f7dfaab6d7d"; // 用户的Secret Access Key
        String ENDPOINT = "http://bos.qasandbox.bcetest.baidu.com";

        
        // 初始化一个BosClient
        BosClientConfiguration config = new BosClientConfiguration();
        config.setCredentials(new DefaultBceCredentials(ACCESS_KEY_ID, SECRET_ACCESS_KEY));
        config.setEndpoint(ENDPOINT);
        
        BosClient client = new BosClient(config);
        
        // createBucket("test-zoe", client);
        
        try {
            PutObject(client, "test-lxb", "2.png", null, null);
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    public static void createBucket(String bucketName, BosClient client){
        // 新建client
        boolean exist = client.doesBucketExist(bucketName);
        System.out.println("============" + exist + "==================");
        if (exist) {
            System.out.println("bucket is exist");
        }else {
            System.out.println("create a new bucket");
            client.createBucket(bucketName);
            
        }
    }
    
    public static void PutObject(BosClient client, String bucketName, String objectKey, byte[] byte1, String string1) throws FileNotFoundException{
        // 获取指定文件
        File file = new File("D:/145.png");
        // 获取数据流
        //InputStream inputStream = new FileInputStream("D:/145.png");

        // 以文件形式上传Object
        PutObjectResponse putObjectFromFileResponse = client.putObject(bucketName, objectKey, file);
        // 以数据流形式上传Object
        //PutObjectResponse putObjectResponseFromInputStream = client.putObject(bucketName, objectKey, inputStream);
        // 以二进制串上传Object
        //PutObjectResponse putObjectResponseFromByte = client.putObject(bucketName, objectKey, byte1);
        // 以字符串上传Object
        //PutObjectResponse putObjectResponseFromString = client.putObject(bucketName, objectKey, string1);

        // 打印ETag
        System.out.println(putObjectFromFileResponse.getETag());
    }
}
