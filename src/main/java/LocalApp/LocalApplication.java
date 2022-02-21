package LocalApp;

import SharedResources.S3;
import SharedResources.Sqs;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Base64;

public class LocalApplication {
    private Sqs inputSqs;
    private Sqs resultSqs;
    private S3 s3;
    private Ec2Client ec2Client;

    private String inputFileName;
    private String outputFileName;

    private int n;
    private boolean terminateMode = false;

    // Command args: [input file] [output file] [num of files per worker] [terminate]
    public LocalApplication(String[] args){
        inputFileName = System.getProperty("user.dir");
        if (System.getProperty("os.name").startsWith("Windows")) {
            inputFileName += '\\'+args[0];
        } else {
            inputFileName += '/'+args[0];
        }
        outputFileName = args[1];
        n = Integer.parseInt(args[2]);
        terminateMode = args.length > 3;
        run();
    }

    private void run() {
        System.out.println("Local application is running!");
        ec2Client = Ec2Client.builder().build();

        String resultSqsId = "LocalAppResultSqs" + System.currentTimeMillis() + ".fifo";
        resultSqs = new Sqs(resultSqsId);
        String s3Id = "*** Bucket Name ***";
        s3 = new S3(s3Id);
        activateManager(s3Id);

        String s3Path = uploadFileToS3(inputFileName);
        inputSqs.write("inputFile: " + s3Path + " " + resultSqsId + " " + n, "");
        Message resultMessage = getResultPath();
        createResultFile(resultMessage.body());

        if (terminateMode) {
            sendTerminationToManager();
        }
        resultSqs.delete();
        ec2Client.close();
    }

    private void sendTerminationToManager() {
        System.out.println("Terminating the manager");
        inputSqs.write("terminate", "terminations");
    }

    /* createResultFile
     * Blocking method that waits for the all its request from the manager
     * and writes the HTML output
     */
    private void createResultFile(String resultPath) {
        System.out.println("Preparing HTML file.");
        System.out.println("Downloading the results from s3 at: " + resultPath);
        BufferedReader resultFileBuffer = s3.download(resultPath);
        String fileName = System.getProperty("user.dir") + "\\"+outputFileName+".html";
        String htmlContent = "<!DOCTYPE html>\n"
                +"<html>\n" +
                "   <head>\n" +
                "     <title>Result File</title>\n" +
                "   </head>\n" +
                "   <body>\n";
        try(PrintWriter out = new PrintWriter(fileName)) {
            String line;
            while ((line = resultFileBuffer.readLine()) != null) {
                System.out.println(line);
                htmlContent += "<p>"+line+"<p>";
            }
            htmlContent += "   </body>\n" +
                    " </html>";
            out.println(htmlContent);
        } catch (IOException e) {
            System.out.println(e);
        }
    }

    private Message getResultPath() {
        return resultSqs.readBlocking();
    }

    private String uploadFileToS3(String path) {
        System.out.println("Uploading " + path + " to s3");
        return s3.upload(path);
    }

    private void activateManager(String s3Identifier) {
        String inputSqsIdentifier = "*** inputSqsIdentifier ***";
        Instance managerInstance = getManager();
        if (managerInstance == null) {
            String managerInstanceId = createManager(inputSqsIdentifier, s3Identifier);
            startManager(managerInstanceId);
        }
        inputSqs = new Sqs(inputSqsIdentifier);
    }

    private String createManager(String sqsIdentifier, String s3Identifier) {
        System.out.println("Creating manager instance!");
        String amiID = "ami-00e95a9222311e8ed"; //Amazon Linux with Java 8
        String userDataString = "#!/bin/bash\n" +
                "set -x\n" + "echo Hello, World!1\n" +
                "aws s3 cp s3://"+s3Identifier+"/Manager.jar Manager.jar\n" +
                "java -jar Manager.jar "+sqsIdentifier+" "+s3Identifier;
        String userData = Base64.getEncoder().encodeToString((userDataString).getBytes());
        IamInstanceProfileSpecification role = IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build();
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(amiID)
                .instanceType(InstanceType.T2_MICRO)
                .maxCount(1)
                .minCount(1)
                .userData(userData)
                .iamInstanceProfile(role)
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
        StartInstancesRequest request = StartInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();

        ec2Client.startInstances(request);
        System.out.printf("Started manager instance %s", instanceId);
    }

    /* returns Manager Instance if currently exists
    * if not returns null */
    private Instance getManager() {
        Filter filter = Filter.builder().name("tag:Type").values("Manager").build();
        DescribeInstancesRequest request = DescribeInstancesRequest.builder().filters(filter).build();
        DescribeInstancesResponse response = ec2Client.describeInstances(request);
        for (Reservation reservation : response.reservations()) {
            for (Instance instance : reservation.instances()) {
                System.out.println("Manager instance state is " + instance.state().name());
                if (instance.state().name() == InstanceStateName.RUNNING || instance.state().name() == InstanceStateName.PENDING)
                return instance;
            }
        }
        return null;
    }
}
