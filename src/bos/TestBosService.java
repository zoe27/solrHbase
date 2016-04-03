package bos;

import java.io.File;
import java.io.FileNotFoundException;

import com.baidubce.auth.DefaultBceCredentials;
import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.BosClientConfiguration;
import com.baidubce.services.bos.model.PutObjectResponse;


public class TestBosService {
    public static void main(String[] args) {
        String ACCESS_KEY_ID = "1ce7f0bbc27742afa9231a3e98c3e4ca"; // �û���Access Key ID
        String SECRET_ACCESS_KEY = "9b61c9ec68d04684a3295f7dfaab6d7d"; // �û���Secret Access Key
        String ENDPOINT = "http://bos.qasandbox.bcetest.baidu.com";

        
        // ��ʼ��һ��BosClient
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
        // �½�client
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
        // ��ȡָ���ļ�
        File file = new File("D:/145.png");
        // ��ȡ������
        //InputStream inputStream = new FileInputStream("D:/145.png");

        // ���ļ���ʽ�ϴ�Object
        PutObjectResponse putObjectFromFileResponse = client.putObject(bucketName, objectKey, file);
        // ����������ʽ�ϴ�Object
        //PutObjectResponse putObjectResponseFromInputStream = client.putObject(bucketName, objectKey, inputStream);
        // �Զ����ƴ��ϴ�Object
        //PutObjectResponse putObjectResponseFromByte = client.putObject(bucketName, objectKey, byte1);
        // ���ַ����ϴ�Object
        //PutObjectResponse putObjectResponseFromString = client.putObject(bucketName, objectKey, string1);

        // ��ӡETag
        System.out.println(putObjectFromFileResponse.getETag());
    }
}
