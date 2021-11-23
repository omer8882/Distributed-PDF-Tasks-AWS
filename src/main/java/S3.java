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
        //omer
        String fileKey = URL; // TODO: How to get key from URL
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
        // omer
        String fileKeyName = path.substring(path.lastIndexOf('\\')); //TODO: Choose a unique name from path

        PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(fileKeyName)
                .build();
        try{
            FileInputStream fileInputStream = new FileInputStream(new File(path));
            byte[] bytesToWrite = fileInputStream.readAllBytes();
            ByteArrayInputStream inputStream = new ByteArrayInputStream(bytesToWrite);
            s3Client.putObject(objectRequest, RequestBody.fromInputStream(inputStream, bytesToWrite.length));
            System.out.println("Upload complete");
        }catch(IOException e){
            System.out.println(e);
        }
        return "URL-of-uploaded-file.";
    }
}