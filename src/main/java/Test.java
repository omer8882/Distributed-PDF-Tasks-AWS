import java.io.*;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

public class Test {

    public static void main(String[] args) throws IOException {
        //System.out.println(System.getenv());
        //System.out.println(cmd("https://www.star-k.org/articles/wp-content/uploads/Starbucks_Recommendations_July2019.pdf"));
        //System.out.println(downloadPDF("http://www.africau.edu/images/default/sample.pdf"));
        Ec2Client ec2 = Ec2Client.builder().build();
        //String newEc2Instance = createInstance(ec2);
        //startInstance(ec2, newEc2Instance);
        //describeInstanceTags(ec2, "i-07a5985dcb365cd14");
        //stopInstance(ec2, "i-07a5985dcb365cd14");
        //describeInstanceTags(ec2);
        //describeInstances(ec2);
        //stopInstance(ec2, "i-07a5985dcb365cd14");
        terminateInstance(ec2, "i-00d4e2e5a42266dd8");
        System.out.println("Num of running workers: "+getNumOfRunningWorkers(ec2));
    }

    private static String cmd(String url) {
        ProcessBuilder processBuilder = new ProcessBuilder();
        String wgetCmdLine = "wget + " + url;
        String localPath = "";
        // Windows option just for testing
        if (System.getProperty("os.name").startsWith("Windows")) {
            processBuilder.command("cmd.exe", "/c", wgetCmdLine);
        } else {
            processBuilder.command("bash", "-c", wgetCmdLine);
        }
        try {
            Process process = processBuilder.start();
            //To read the output list
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
            int exitCode = process.waitFor();
            System.out.println("\nExited command line with error code: " + exitCode);
            localPath = System.getProperty("user.dir") + url.substring(url.lastIndexOf('/'));
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        return localPath;
    }

    private static String downloadPDF(String url) {
        String wgetCmdLine = "wget " + url;
        String localPath = "";
        try {
            Process process;
            // Windows option just for testing
            if (System.getProperty("os.name").startsWith("Windows")) {
                System.out.println("OS is Windows...");
                process = Runtime.getRuntime().exec("cmd /c " + wgetCmdLine);
            } else {
                process = Runtime.getRuntime().exec(wgetCmdLine);
            }
            StringBuilder output = new StringBuilder();
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line + "\n");
            }
            int exitVal = process.waitFor();
            if (exitVal == 0) {
                System.out.println("Success!");
                System.out.println(output);
                System.exit(0);
            } else {
                System.out.println("Abnormal Error... " + exitVal);
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        return localPath;
    }

    private static String createInstance(Ec2Client ec2) {
        String amiID = "ami-04902260ca3d33422";
        // ami-04ad2567c9e3d7893 (T1_MICRO) without java
        // ami-00e95a9222311e8ed (T2_MICRO) with java - works but version 52.0
        //ami-061fe2315dce4d761
        // ami-04902260ca3d33422 - amazon linux - WORKS!!!! (T2_MICRO) without java
        String userDataString = "#!/bin/bash\n" +
                "set -x\n" + "echo Hello, World!1\n" +
                "sudo amazon-linux-extras install java-openjdk11\n" +
                "export AWS_ACCESS_KEY_ID=ASIAQYDTG66XIRC7SQDD\n" +
                "export AWS_SECRET_ACCESS_KEY=NlmZgI9B9/+T1r4LS0qfNokg6HpQtNjI721czPxA\n" +
                "export AWS_SESSION_TOKEN=FwoGZXIvYXdzEFAaDH5DenXY9kOZOL+MEyLIAYTe2peYn0EgVKedKuhPXZFN3LYPlEn4ap5mKG7aAnJ8PhJmP8gjuqZfWXCHiNUkeGfnvnHNPOoqjkGsn/rHSBDm7o3RX05F3+IzB35xgeo9bzJdAWuWU2Iba2mnUkz9dhaTJ3QWU2buvQKmhG//YtztoWwjR+bbMDqMmQypgQcUsU7wkUwg1T+/xRqUZXNNC+ZuE2VuAd0uuueWEnylCIwDAn5gxK3+XJZ9cHjWpiTM6I0d+Jwus9ZzXP6hc+fA4FmODHgQVV+MKNuUno0GMi1H3MuLFD9X9yyhak2KMIPIqCSdxDFuIyRfJ/pJCHCXn1+mvf62uLfb2cVc9dE=\n" +
                "export AWS_DEFAULT_REGION=us-east-1\n" +
                "aws s3 cp s3://bucket1637048833333/Main.jar Main.jar\n" +
                "java -jar Main.jar";
        String userData = Base64.getEncoder().encodeToString((userDataString).getBytes());
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(amiID)
                .instanceType(InstanceType.T2_MICRO)
                .maxCount(1)
                .minCount(1)
                .userData(userData)
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);
        String instanceId = response.instances().get(0).instanceId();
        Tag tag = Tag.builder()
                .key("Type")
                .value("Worker")
                .build();
        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();

        try {
            ec2.createTags(tagRequest);
            System.out.println("Successfully created EC2 Instance " + instanceId);
            System.out.println("All instances: " + response.instances());
            return instanceId;
        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return "";
    }

    private static void startInstance(Ec2Client ec2, String instanceId) {
        StartInstancesRequest request = StartInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();

        ec2.startInstances(request);
        System.out.println("Started instance " + instanceId);
    }

    private static int getNumOfRunningWorkers(Ec2Client ec2Client) {
        DescribeTagsResponse describeTagsResponse = ec2Client.describeTags(DescribeTagsRequest.builder().build());
        List<TagDescription> tags = describeTagsResponse.tags();
        Filter filter = Filter.builder().name("tag:Type").values("Worker").build();
        DescribeInstancesRequest request = DescribeInstancesRequest.builder().filters(filter).build();
        DescribeInstancesResponse response = ec2Client.describeInstances(request);
        int count=0, running=0;
        for (Reservation reservation : response.reservations()) {
            for (Instance instance : reservation.instances()) {
                System.out.println("Worker "+(++count)+" state is " + instance.state().name());
                if (instance.state().name()==InstanceStateName.RUNNING)
                    running++;
            }
        }
        return running;
    }

    public static void describeInstanceTags(Ec2Client ec2, String resourceId) {
        try {
            DescribeTagsResponse describeTagsResponse = ec2.describeTags(DescribeTagsRequest.builder().build());
            List<TagDescription> tags = describeTagsResponse.tags();
            for (TagDescription tag : tags) {
                System.out.println("For Instance "+tag.resourceId());
                System.out.print(" key is: " + tag.key());
                System.out.println(", value is: " + tag.value());
            }
        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    private static void describeInstances(Ec2Client ec2) {
//        DescribeTagsResponse describeTagsResponse = ec2.describeTags(DescribeTagsRequest.builder().build());
//        List<TagDescription> tags = describeTagsResponse.tags();
//        for (TagDescription tag : tags) {
//            if (tag.key().equals("Type") && tag.value().equals("Worker")) {
//                System.out.println("Found Worker with id "+tag.resourceId());
                //Filter filter = Filter.builder().name("resource-id").values(tag.resourceId()).build();
                Filter filter = Filter.builder().name("tag:Type").values("Worker").build();
                DescribeInstancesRequest request = DescribeInstancesRequest.builder().filters(filter).build();
                DescribeInstancesResponse response = ec2.describeInstances(request);
                for (Reservation reservation : response.reservations()) {
                    System.out.println("Reservation: " + reservation.toString());
                    for (Instance instance : reservation.instances()) {
                        System.out.println("Instance Id is " + instance.instanceId());
                        System.out.println("Image id is " + instance.imageId());
                        System.out.println("Instance type is " + instance.instanceType());
                        System.out.println("Instance state name is " + instance.state().name());
                        System.out.println("monitoring information is " + instance.monitoring().state());
                    }
                }
//            }
//        }
    }

    public static void stopInstance(Ec2Client ec2, String instanceId) {
        StopInstancesRequest request = StopInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();

        ec2.stopInstances(request);
        System.out.println("Successfully stopped instance "+instanceId);
    }

    private static void terminateInstance(Ec2Client ec2, String instanceID) {
        try{
            List<String> instances = new ArrayList<>();
            instances.add("i-02f6c374b57f88787"); instances.add("i-0e2cb7407ef5bf5f8");
            TerminateInstancesRequest ti = TerminateInstancesRequest.builder()
                    .instanceIds(instances)
                    .build();
            TerminateInstancesResponse response = ec2.terminateInstances(ti);
            List<InstanceStateChange> list = response.terminatingInstances();
            for (InstanceStateChange sc : list) {
                System.out.println("The ID of the terminated instance is " + sc.instanceId());
            }
        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }


    /************************** S3 **********************/

    private void testS3() throws IOException {
        Region region = Region.US_EAST_1;
        S3Client s3 = S3Client.builder().region(region).build();

        String bucket = "bucket1637048833333"; // + System.currentTimeMillis();
        String key = "key";

        //createNewBucket(s3, bucket, region);

        //System.out.println("Uploading object...");

        //addObject(s3, bucket, region, key);

        getObject(s3, bucket, region, key);

        //cleanUp(s3, bucket, key);

        System.out.println("Closing the connection to {S3}");
        s3.close();
        System.out.println("Connection closed");
        System.out.println("Exiting...");
    }

    public static void createNewBucket(S3Client s3Client, String bucketName, Region region) {
        try {
            s3Client.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(bucketName)
                    .createBucketConfiguration(
                            CreateBucketConfiguration.builder()
                                    .build())
                    .build());
            System.out.println("Creating bucket: " + bucketName);
            s3Client.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
            System.out.println(bucketName + " is ready.");
            System.out.printf("%n");
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    public static void addObject(S3Client s3Client, String bucketName, Region region, String key) {
        PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();
        // Create content
        ObjectMapper mapper = new ObjectMapper();
        WorkerRequestMsg message = new WorkerRequestMsg(PDFConverter.ToImage, "www.wtf.com/pdf.pdf", "resultSqsId");
        String fileName = "testRequest.txt";
        File file = new File(fileName);
        try {
            mapper.writeValue(file, message);
            byte[] bytesToWrite = mapper.writeValueAsBytes(message);
            ByteArrayInputStream messageInputStream = new ByteArrayInputStream(bytesToWrite);
            s3Client.putObject(objectRequest, RequestBody.fromInputStream(messageInputStream, bytesToWrite.length));
            System.out.println("Upload complete");
        } catch (IOException e) {
            System.out.println(e);
        }
    }

    public static void getObject(S3Client s3Client, String bucketName, Region region, String key) throws IOException {
        System.out.println("Getting object...");
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();

        ResponseInputStream<GetObjectResponse> obj = s3Client.getObject(getObjectRequest);
        BufferedReader reader = new BufferedReader(new InputStreamReader(obj));

        String line;
        System.out.println("File from S3 bucket:");
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }
    }

    public static void cleanUp(S3Client s3Client, String bucketName, String keyName) {
        System.out.println("Cleaning up...");
        try {
            System.out.println("Deleting object: " + keyName);
            DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder().bucket(bucketName).key(keyName).build();
            s3Client.deleteObject(deleteObjectRequest);
            System.out.println(keyName + " has been deleted.");
            System.out.println("Deleting bucket: " + bucketName);
            DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucketName).build();
            s3Client.deleteBucket(deleteBucketRequest);
            System.out.println(bucketName + " has been deleted.");
            System.out.printf("%n");
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        System.out.println("Cleanup complete");
        System.out.printf("%n");
    }


}