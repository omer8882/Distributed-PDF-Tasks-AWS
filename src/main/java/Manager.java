import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public class Manager {
    Sqs localAppInputSqs;
    S3 s3;
    Sqs workersSQS;
    Ec2Client ec2Client;
    int currentNumOfWorkers;
    int newNumOfWorkers;
    int n;
    boolean terminateReceived;

    //should be singleton
    public Manager(String localAppSqsId, String s3Identifier, int n) {
        this.localAppInputSqs = new Sqs(localAppSqsId);
        this.s3 = new S3(s3Identifier);
        String workerSqsId = "workerSqs" + System.currentTimeMillis() + ".fifo";
        this.workersSQS = new Sqs(workerSqsId);
        this.n = n;
        this.terminateReceived = false;
        this.currentNumOfWorkers = 0;
        this.newNumOfWorkers = 0;
        ec2Client = Ec2Client.builder().build();
    }

    public void run() {
        Object lock = new Object();
        List<Thread> threads = new ArrayList<>();
        Message msg;
        int i = 0;
        while (!terminateReceived) {
            msg = localAppInputSqs.readBlocking();
            localAppInputSqs.deleteMessage(msg);
            if (msg.body().startsWith("inputFile: ")) {
                Message finalMsg = msg;
                Thread t = new Thread("" + i) {
                    public void run() {
                        handleTask(finalMsg, lock);
                    }
                };
                t.start();
                threads.add(t);
                i++;
            } else if (msg.body() == "terminate") {
                this.terminate();
            } else {
                System.out.println("Invalid sqs msg");
            }
            createWorkers(lock);
        }
        //wait for all thread ids
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        //terminate all workers
        terminateWorkers();
        //terminate itself
    }

    private void handleTask(Message msg, Object lock) {
        String[] splited = msg.body().split("\\s+"); // [0] InputFile: , [1] path, [2] result sqs
        BufferedReader s3Content = s3.download(splited[1]);
        String workerResultsSqsId = "workerResultsSqsId" + System.currentTimeMillis(); // maybe splited[2]
        Sqs workerResultsSqs = new Sqs(workerResultsSqsId);
        int workerItemsCount = 0;
        try {
            String line;
            while ((line = s3Content.readLine()) != null) {
                String operationString = line.substring(0, line.indexOf(' '));
                short operation = operationString.equals("ToImage") ? PDFConverter.ToImage :
                        operationString.equals("ToHTML") ? PDFConverter.ToHTML :
                                operationString.equals("ToText") ? PDFConverter.ToText : 0;
                if (operation == 0)
                    throw new IOException("Operation on message is illegal!\n Got: " + operationString);
                ObjectMapper mapper = new ObjectMapper();
                String msgString = mapper.writeValueAsString(
                    new WorkerRequestMsg(operation, "http://www.website.com/file.pdf", workerResultsSqsId)); //maybe splited[2]
                workersSQS.write(msgString, "thinkAboutThat");
                workerItemsCount++;
            }
        } catch (IOException e) {
            System.out.println(e);
        }

        int m = (int) Math.min(Math.ceil(1.0 * workerItemsCount / n), 15);
        synchronized (lock) {
            newNumOfWorkers = Math.max(m, newNumOfWorkers);
        }

        // Read result
        String resultString = "";
        for (int i = 0; i < workerItemsCount; i++) {
            Message workerResultMsg = workerResultsSqs.readBlocking();
            // aggregate result
            resultString += getWorkerCompleteMessage(workerResultMsg.body()) + "\n";
        }
        String fileName = "result.txt";
        try (PrintWriter out = new PrintWriter(fileName)) {
            out.println(resultString);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        //        upload result to s3
        String url = s3.upload(fileName);
        //      write the path to the results to local app sqs
        Sqs localAppResultSqs = new Sqs(splited[2]); // need to trim the message
        localAppResultSqs.write(url, "results");
    }

    private void createWorkers(Object lock) {
        int tmp;
        synchronized (lock) {
            tmp = newNumOfWorkers;
        }
        // Assert number of current running workers
        int assertRunning = getNumOfRunningWorkers();
        if (assertRunning < currentNumOfWorkers) {
            System.out.println("Warning: Manager and AWS show different running workers. A worker error must've occurred.");
            currentNumOfWorkers=assertRunning;
        }
        for (int i = currentNumOfWorkers; i < tmp; i++) {
            createNewWorker(i);
        }
        currentNumOfWorkers = tmp;
    }

    private int getNumOfRunningWorkers() {
        DescribeTagsResponse describeTagsResponse = ec2Client.describeTags(DescribeTagsRequest.builder().build());
        List<TagDescription> tags = describeTagsResponse.tags();
        Filter filter = Filter.builder().name("tag:Type").values("Worker").build();
        DescribeInstancesRequest request = DescribeInstancesRequest.builder().filters(filter).build();
        DescribeInstancesResponse response = ec2Client.describeInstances(request);
        int count=0, running=0;
        for (Reservation reservation : response.reservations()) {
            for (Instance instance : reservation.instances()) {
                System.out.println("Worker "+(++count)+" is " + instance.state().name());
                if (instance.state().name()==InstanceStateName.RUNNING || instance.state().name()==InstanceStateName.PENDING)
                    running++;
            }
        }
        return running;
    }

    private void createNewWorker(int i) {
        System.out.println("Creating one new worker");
        String instanceId = createWorkerInstance();
        startInstance(instanceId);
    }

    private String createWorkerInstance(){
        String amiID = "ami-04902260ca3d33422"; // amazon linux(T2_MICRO) without java
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

        RunInstancesResponse response = ec2Client.runInstances(runRequest);
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
            ec2Client.createTags(tagRequest);
            System.out.println("Successfully created EC2 Instance " + instanceId + "as worker.");
            return instanceId;
        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
        return "";
    }

    private void startInstance(String instanceId) {
        StartInstancesRequest request = StartInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();

        ec2Client.startInstances(request);
        System.out.println("Started instance " + instanceId);
    }


    private void terminate() {
        System.out.println("Manager terminating");
        this.terminateReceived = true;
    }

    private void terminateWorkers() {
        System.out.println("Terminating all workers.");
        // Getting all worker IDs
        DescribeTagsResponse describeTagsResponse = ec2Client.describeTags(DescribeTagsRequest.builder().build());
        Filter filter = Filter.builder().name("tag:Type").values("Worker").build();
        DescribeInstancesRequest request = DescribeInstancesRequest.builder().filters(filter).build();
        DescribeInstancesResponse response = ec2Client.describeInstances(request);
        List<String> workers = new ArrayList<>();
        for (Reservation reservation : response.reservations()) {
            for (Instance instance : reservation.instances()) {
                if (instance.state().name()!=InstanceStateName.TERMINATED){
                    workers.add(instance.instanceId());
                }
            }
        }

        // Termination
        try{
            TerminateInstancesRequest ti = TerminateInstancesRequest.builder()
                    .instanceIds(workers)
                    .build();
            TerminateInstancesResponse responseTer = ec2Client.terminateInstances(ti);
            List<InstanceStateChange> list = responseTer.terminatingInstances();
            for (int i = 0; i < list.size(); i++) {
                InstanceStateChange sc = (list.get(i));
                System.out.println("Terminated Worker Instance "+sc.instanceId());
            }
        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    private String getWorkerCompleteMessage(String msgString) {
        ObjectMapper mapper = new ObjectMapper();
        WorkerCompleteMsg msg = null;
        try {
            msg = mapper.readValue(msgString, WorkerCompleteMsg.class);
        } catch (IOException e) {
            System.out.println("ERROR: Couldn't read workers complete message properly.\n" + e);
        }
        String operation = msg.getOperation() == PDFConverter.ToImage ? "ToImage" :
                msg.getOperation() == PDFConverter.ToHTML ? "ToHTML" :
                        msg.getOperation() == PDFConverter.ToText ? "ToText" : "ERROR";
        String summaryLine = msg.getFileURL() + " " + operation + " " + msg.getS3URL();
        return summaryLine;
    }
}
