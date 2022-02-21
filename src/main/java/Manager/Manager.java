package Manager;

import SharedResources.PDFConverter;
import SharedResources.S3;
import SharedResources.Sqs;
import Worker.WorkerCompleteMsg;
import Worker.WorkerRequestMsg;
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
    private static Sqs localAppInputSqs;
    private static S3 s3;
    private static Sqs workersSQS;
    private static Ec2Client ec2Client;
    private static int currentNumOfWorkers;
    private static int newNumOfWorkers;
    private static boolean terminateReceived;
    private static final int MAXNUMOFWORKERS = 8;

    public static void main(String[] args) {
        setupManager(args[0], args[1]);
    }

    private static void setupManager(String localAppSqsId, String s3Identifier) {
        localAppInputSqs = new Sqs(localAppSqsId);
        s3 = new S3(s3Identifier);
        String workerSqsId = "workerSqs" + System.currentTimeMillis() + ".fifo";
        workersSQS = new Sqs(workerSqsId);
        terminateReceived = false;
        currentNumOfWorkers = 0;
        newNumOfWorkers = 0;
        ec2Client = Ec2Client.builder().build();
        run();
    }

    /**
     * Receives and operates all tasks
     * Opens thread per task
     */
    private static void run() {
        Object lock = new Object();
        List<Thread> threads = new ArrayList<>();
        Message msg;
        int i = 0; // Num of running threads
        while (!terminateReceived) {
            msg = localAppInputSqs.tryReadFromSQS();
            if (msg == null) { // Waiting 1 sec if no new message
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else if (msg.body().startsWith("inputFile: ")) {
                // Opening a new thread for new input file
                Message finalMsg = msg;
                Thread t = new Thread("" + i) {
                    public void run() {
                        handleTask(finalMsg, lock);
                    }
                };
                t.start();
                threads.add(t);
                i++;
            } else if (msg.body().equals("terminate")) {
                terminateReceived = true;
            } else {
                System.out.println("Invalid sqs msg");
            }
            // Updates num of running workers, after each try to read new message.
            createWorkers(lock);
        }

        // Starting termination process

        // Wait for all thread ids
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // Cleanup
        terminateWorkers();
        localAppInputSqs.delete();
        workersSQS.delete();
        terminateSelf();
    }

    private static void handleTask(Message msg, Object lock) {
        // Reading local app request
        String[] splitMessage = msg.body().split("\\s+"); // [0] InputFile: , [1] path, [2] result sqs, [3] n
        BufferedReader s3Content = s3.download(splitMessage[1]);
        String resultSQSId = splitMessage[2];
        int n = Integer.parseInt(splitMessage[3]);
        String TAG = "Job "+resultSQSId+": ";

        // Setting up worker tasks
        String workerResultsSqsId = "workerResultsSqsId" + System.currentTimeMillis() + ".fifo";
        Sqs workerResultsSqs = new Sqs(workerResultsSqsId);
        int workerItemsCount = 0;
        int requestedNumOfWorkers = 0; // For this local app task
        try {
            String line;
            // Sending each file in request as a separate task
            while ((line = s3Content.readLine()) != null) {
                String operationString = line.substring(0, line.indexOf('\t'));
                String fileURL = line.substring(line.indexOf('\t') + 1);
                short operation = operationString.equals("ToImage") ? PDFConverter.ToImage :
                        operationString.equals("ToHTML") ? PDFConverter.ToHTML :
                                operationString.equals("ToText") ? PDFConverter.ToText : 0;
                if (operation == 0)
                    throw new IOException("Operation on message is illegal!\n Got: " + operationString);
                ObjectMapper mapper = new ObjectMapper();
                String msgString = mapper.writeValueAsString(
                        new WorkerRequestMsg(operation, fileURL, workerResultsSqsId));
                workersSQS.write(msgString, "");
                workerItemsCount++;
                // Live updating of number of workers to activate
                if (workerItemsCount % n == 0) {
                    requestedNumOfWorkers++;
                    synchronized (lock) {
                        newNumOfWorkers = Math.min(MAXNUMOFWORKERS, Math.max(requestedNumOfWorkers, newNumOfWorkers));
                    }
                }
            }
            System.out.println(TAG+ "Sent all sqs messages to workers.");
        } catch (IOException e) {
            System.out.println(e);
        }

        // Aggregating workers Results
        String resultString = "";
        for (int i = 0; i < workerItemsCount; i++) {
            Message workerResultMsg = workerResultsSqs.readBlocking();
            resultString += getWorkerCompleteMessage(workerResultMsg.body()) + "\n";
        }
        System.out.println(TAG+"Received all results.");

        // Creating a file for all th received workers results
        String fileName = System.getProperty("user.dir") + "/result.txt";
        try (PrintWriter out = new PrintWriter(fileName)) {
            out.println(resultString);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        // Upload result to s3
        System.out.println(TAG+"Uploading results file to SharedResources.S3");
        String url = s3.upload(fileName);

        // Write the results path to local app sqs
        Sqs localAppResultSqs = new Sqs(resultSQSId); // need to trim the message
        localAppResultSqs.write(url, "");

        // Cleanup
        localAppInputSqs.deleteMessage(msg);
        workerResultsSqs.delete();
        System.out.println(TAG+"Finished.");
    }

    private static void createWorkers(Object lock) {
        int tmp;
        synchronized (lock) {
            tmp = newNumOfWorkers;
        }
        // Assert number of current running workers
        int assertRunning = getNumOfRunningWorkers();
        if (assertRunning < currentNumOfWorkers) {
            System.out.println("Warning: Manager and AWS show different running workers. A worker error must've occurred.");
            currentNumOfWorkers = assertRunning;
        }
        // Creates the difference of workers needed
        for (int i = currentNumOfWorkers; i < tmp; i++) {
            createNewWorker(i);
        }
        currentNumOfWorkers = tmp;
    }

    private static int getNumOfRunningWorkers() {
        DescribeTagsResponse describeTagsResponse = ec2Client.describeTags(DescribeTagsRequest.builder().build());
        List<TagDescription> tags = describeTagsResponse.tags();
        Filter filter = Filter.builder().name("tag:Type").values("Worker").build();
        DescribeInstancesRequest request = DescribeInstancesRequest.builder().filters(filter).build();
        DescribeInstancesResponse response = ec2Client.describeInstances(request);
        int count = 0, running = 0;
        for (Reservation reservation : response.reservations()) {
            for (Instance instance : reservation.instances()) {
                if (instance.state().name() == InstanceStateName.RUNNING || instance.state().name() == InstanceStateName.PENDING)
                    running++;
            }
        }
        return running;
    }

    private static void createNewWorker(int i) {
        String instanceId = createWorkerInstance();
        if (!instanceId.equals(""))
            startInstance(instanceId);
    }

    private static String createWorkerInstance() {
        String amiID = "ami-00e95a9222311e8ed"; // Linux T2_MICRO with Java 8
        String userDataString = "#!/bin/bash\n" +
                "set -x\n" + "echo Hello, World!1\n" +
                "aws s3 cp s3://"+s3.getIdentifier()+"/Worker.jar Worker.jar\n" +
                "java -jar Worker.jar";
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

    private static void startInstance(String instanceId) {
        StartInstancesRequest request = StartInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();

        ec2Client.startInstances(request);
        System.out.println("Started instance " + instanceId);
    }


    private static void terminateSelf() {
        System.out.println("Manager terminating itself.");
        Instance myself = null;
        Filter filter = Filter.builder().name("tag:Type").values("Manager").build();
        DescribeInstancesRequest request = DescribeInstancesRequest.builder().filters(filter).build();
        DescribeInstancesResponse response = ec2Client.describeInstances(request);
        for (Reservation reservation : response.reservations()) {
            for (Instance instance : reservation.instances()) {
                System.out.println("Manager instance state is " + instance.state().name());
                if (instance.state().name() == InstanceStateName.RUNNING)
                    myself = instance;
            }
        }

        try {
            TerminateInstancesRequest ti = TerminateInstancesRequest.builder()
                    .instanceIds(myself.instanceId())
                    .build();
            TerminateInstancesResponse responseTer = ec2Client.terminateInstances(ti);
            List<InstanceStateChange> list = responseTer.terminatingInstances();
            for (int i = 0; i < list.size(); i++) {
                InstanceStateChange sc = (list.get(i));
            }
        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    private static void terminateWorkers() {
        System.out.println("Terminating all workers.");
        // Getting all worker IDs
        Filter filter = Filter.builder().name("tag:Type").values("Worker").build();
        DescribeInstancesRequest request = DescribeInstancesRequest.builder().filters(filter).build();
        DescribeInstancesResponse response = ec2Client.describeInstances(request);
        List<String> workers = new ArrayList<>();
        for (Reservation reservation : response.reservations()) {
            for (Instance instance : reservation.instances()) {
                if (instance.state().name() != InstanceStateName.TERMINATED) {
                    workers.add(instance.instanceId());
                }
            }
        }

        // Terminating all workers
        try {
            TerminateInstancesRequest ti = TerminateInstancesRequest.builder()
                    .instanceIds(workers)
                    .build();
            TerminateInstancesResponse responseTer = ec2Client.terminateInstances(ti);
            List<InstanceStateChange> list = responseTer.terminatingInstances();
            for (int i = 0; i < list.size(); i++) {
                InstanceStateChange sc = (list.get(i));
                System.out.println("Terminated Worker Instance " + sc.instanceId());
            }
        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    private static String getWorkerCompleteMessage(String msgString) {
        ObjectMapper mapper = new ObjectMapper();
        WorkerCompleteMsg msg = null;
        String summaryLine;
        try {
            msg = mapper.readValue(msgString, WorkerCompleteMsg.class);
            String operation = msg.getOperation() == PDFConverter.ToImage ? "ToImage" :
                    msg.getOperation() == PDFConverter.ToHTML ? "ToHTML" :
                            msg.getOperation() == PDFConverter.ToText ? "ToText" : "ERROR";
            summaryLine = operation + " " + msg.getFileURL() + " " + msg.getS3URL();
        } catch (IOException e) {
            System.out.println("ERROR: Couldn't read workers complete message properly.\n" + e);
            summaryLine = "Error in transference of a message occurred.";
        }
        return summaryLine;
    }
}
