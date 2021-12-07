import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.*;
import java.nio.file.Files;

public class S3 {
    private String identifier;
    private S3Client s3Client;
    public S3(String identifier) {
        this.identifier = identifier;
        Region region = Region.US_EAST_1;
        s3Client = S3Client.builder().region(region).build();
    }

    public BufferedReader download(String URL) {
        String bucketName = identifier;
        String[] URLsplit = URL.split("/");
        String fileKey = URLsplit[3];
        bucketName = URLsplit[2];
        System.out.println("Downloading a file on S3 from: " + URL);
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(fileKey)
                .build();
        ResponseInputStream<GetObjectResponse> obj = s3Client.getObject(getObjectRequest);
        BufferedReader reader = new BufferedReader(new InputStreamReader(obj));
        return reader;
    }

    public String upload(String path){
        String bucketName = identifier;
        String key = path.substring(path.lastIndexOf('/')+1);
        int keyPtIdx = key.lastIndexOf('.');
        key = key.substring(0, keyPtIdx)+ "-" + System.currentTimeMillis() + key.substring(keyPtIdx);
        System.out.println("S3 upload file key is: "+key);
        PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();
        try{
            File file = new File(path);
            FileInputStream fileInputStream = new FileInputStream(file);
            byte[] bytesToWrite = Files.readAllBytes(file.toPath());
            ByteArrayInputStream inputStream = new ByteArrayInputStream(bytesToWrite);
            s3Client.putObject(objectRequest, RequestBody.fromInputStream(inputStream, bytesToWrite.length));
            System.out.println("Upload complete");
        }catch(IOException e){
            System.out.println(e);
            if(e.getMessage().startsWith("Your socket connection to the server"))
                return "too long";
            return "";
        }
        return "s://"+identifier+"/"+key;
    }
}