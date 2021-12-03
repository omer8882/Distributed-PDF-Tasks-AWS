import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.Ec2ClientBuilder;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Base64;
import java.util.List;

public class LocalApplication {
    private Sqs inputSqs;
    private Sqs resultSqs;
    private Manager manager;
    private S3 s3;
    private Ec2Client ec2Client;

    public void run(String inputFilePath, int n, boolean terminateMode) {
        System.out.println("Local application is running!");
        ec2Client = Ec2Client.builder().build();

        String resultSqsId = "LocalAppResultSqs" + System.currentTimeMillis() + ".fifo";
        // String s3Id = "S3Id" + System.currentTimeMillis();
        String s3Id = "mybucket920463236";
        resultSqs = new Sqs(resultSqsId);
        s3 = new S3(s3Id);

        activateManager(s3Id, n);
        uploadFileToS3(inputFilePath);
        inputSqs.write("inputFile: " + inputFilePath + " " + resultSqsId, "inputFiles");
        Message resultMessage = getResultPath();
        createResultFile(resultMessage.body());
        handleMode(terminateMode);
        resultSqs.delete();
        inputSqs.delete(); // todo remove that !
    }

    private void handleMode(boolean terminateMode) {
        if (terminateMode) {
            sendTerminationToManager();
        }
    }

    private void sendTerminationToManager() {
        System.out.println("Terminating the manager");
        inputSqs.write("terminate", "terminations");
    }

    private void createResultFile(String resultPath) {
        System.out.println("Preparing HTML file.");
        System.out.println("Downloading the results from s3 at: " + resultPath);
        BufferedReader resultFileBuffer = s3.download(resultPath);
        try {
            String line;
            while ((line = resultFileBuffer.readLine()) != null) {
                System.out.println(line);
                // niv
                // TODO: Add each line to the HTML summary file.
            }
        } catch (IOException e) {
            System.out.println(e);
        }
    }

    private Message getResultPath() {
        return resultSqs.readBlocking();
    }

    private void uploadFileToS3(String path) {
        System.out.println("Uploading " + path + " to s3");
       s3.upload(path);
    }

    private void activateManager(String s3Identifier, int n) {
        String inputSqsPrefix = "LocalAppInputSqs.fifo";
        Instance managerInstance = getManager(); // TODO: Edge Case: Return array of Managers
        if (managerInstance == null || managerInstance.state().name() == InstanceStateName.TERMINATED || managerInstance.state().name() == InstanceStateName.STOPPING || managerInstance.state().name() == InstanceStateName.STOPPED) {
            String inputSqsIdentifier = inputSqsPrefix + System.currentTimeMillis() + ".fifo";
            String managerInstanceId = createManager(inputSqsIdentifier, s3Identifier, n);
            startManager(managerInstanceId);
            //new Thread(() -> manager.run()).start();
        }
        inputSqs = new Sqs(inputSqsPrefix);
    }

    private String createManager(String sqsIdentifier, String s3Identifier, int n) {
        System.out.println("Creating manager instance!");
        String amiID = "ami-04902260ca3d33422"; //Amazon Linus without Java
        String userDataString = "#!/bin/bash\n" +
                "set -x\n" + "echo Hello, World!1\n" +
                "sudo amazon-linux-extras install java-openjdk11\n" +
                "export AWS_ACCESS_KEY_ID=ASIAQYDTG66XIRC7SQDD\n" +
                "export AWS_SECRET_ACCESS_KEY=NlmZgI9B9/+T1r4LS0qfNokg6HpQtNjI721czPxA\n" +
                "export AWS_SESSION_TOKEN=FwoGZXIvYXdzEFAaDH5DenXY9kOZOL+MEyLIAYTe2peYn0EgVKedKuhPXZFN3LYPlEn4ap5mKG7aAnJ8PhJmP8gjuqZfWXCHiNUkeGfnvnHNPOoqjkGsn/rHSBDm7o3RX05F3+IzB35xgeo9bzJdAWuWU2Iba2mnUkz9dhaTJ3QWU2buvQKmhG//YtztoWwjR+bbMDqMmQypgQcUsU7wkUwg1T+/xRqUZXNNC+ZuE2VuAd0uuueWEnylCIwDAn5gxK3+XJZ9cHjWpiTM6I0d+Jwus9ZzXP6hc+fA4FmODHgQVV+MKNuUno0GMi1H3MuLFD9X9yyhak2KMIPIqCSdxDFuIyRfJ/pJCHCXn1+mvf62uLfb2cVc9dE=\n" +
                "export AWS_DEFAULT_REGION=us-east-1\n" +
                "aws s3 cp s3://"+s3Identifier+"/Main.jar Main.jar "+sqsIdentifier+"\n" +
                "java -jar Main.jar";
        String userData = Base64.getEncoder().encodeToString((userDataString).getBytes());
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(amiID)
                .instanceType(InstanceType.T2_MICRO)
                .maxCount(1)
                .minCount(1)
                .userData(userData)
                .build();
        RunInstancesResponse response = ec2Client.runInstances(runRequest);
        String instanceId = response.instances().get(0).instanceId();
        Tag tag = Tag.builder().key("Type").value("Manager").build();
        CreateTagsRequest tagRequest = CreateTagsRequest.builder().resources(instanceId).tags(tag).build();
        ec2Client.createTags(tagRequest);
        System.out.println("Successfully created EC2 Instance " + instanceId + " as Manager");
        return instanceId;
    }

    private void startManager(String instanceId) {
        System.out.println("Staring manager!");
        //this.manager = new Manager(sqsIdentifier, s3Identifier, n);
        StartInstancesRequest request = StartInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();

        ec2Client.startInstances(request);
        System.out.printf("Started manager instance %s", instanceId);
    }

    private Instance getManager() {
        Filter filter = Filter.builder().name("tag:Type").values("Manager").build();
        DescribeInstancesRequest request = DescribeInstancesRequest.builder().filters(filter).build();
        DescribeInstancesResponse response = ec2Client.describeInstances(request);
        for (Reservation reservation : response.reservations()) {
            for (Instance instance : reservation.instances()) {
                System.out.println("Instance state name is " + instance.state().name());
                return instance;
            }
        }
        return null;
    }
}
