import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Manager {
    Sqs localAppInputSqs;
    S3 s3Manager;
    Sqs workersSQS;
    int currentNumOfWorkers;
    int newNumOfWorkers;
    int n;
    boolean terminateReceived;

    //should be singleton
    public Manager(String localAppSqsId, String s3Identifier, int n) {
        this.localAppInputSqs = new Sqs(localAppSqsId);
        this.s3Manager = new S3(s3Identifier);
        String workerSqsId = "workerSqs" + System.currentTimeMillis() + ".fifo";
        this.workersSQS = new Sqs(workerSqsId);
        this.n = n;
        this.terminateReceived = false;
        this.currentNumOfWorkers = 0;
        this.newNumOfWorkers = 0;
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

    private void terminateWorkers() {

    }

    private void handleTask(Message msg, Object lock) {
        String[] splited = msg.body().split("\\s+"); // [0] InputFile: , [1] path, [2] result sqs
        BufferedReader s3Content = s3Manager.download(splited[1]);
        Sqs workerResultsSqs = new Sqs(splited[2]);
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
                String msgString = mapper.writeValueAsString(new WorkerRequestMsg(operation, "http://www.website.com/file.pdf", splited[2]));
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
        //TODO
        //niv
//        upload result to s3
//        write the path to the results to local app sqs
        Sqs resultSqs = new Sqs(splited[2]); // need to trim the message
        resultSqs.write("result bla bla", "results");
    }

    private void createWorkers(Object lock) {
        int tmp;
        synchronized (lock) {
            tmp = newNumOfWorkers;
        }
        for (int i = currentNumOfWorkers; i < tmp; i++) {
            createNewWorker(i);
        }
        currentNumOfWorkers = tmp;

    }

    private void createNewWorker(int i) {
        System.out.println("Creating new worker");
    }


    private void terminate() {
        System.out.println("Manager terminating");
        this.terminateReceived = true;
    }

    private String getWorkerCompleteMessage(String msgString){
        ObjectMapper mapper = new ObjectMapper();
        WorkerCompleteMsg msg = null;
        try{
            msg = mapper.readValue(msgString, WorkerCompleteMsg.class);
        }catch(IOException e){
            System.out.println("ERROR: Couldn't read workers complete message properly.\n"+e);
        }
        String operation = msg.getOperation()==PDFConverter.ToImage ? "ToImage":
                msg.getOperation()==PDFConverter.ToHTML ? "ToHTML":
                        msg.getOperation()==PDFConverter.ToText ? "ToText": "ERROR";
        String summaryLine = msg.getFileURL()+" "+operation+" "+msg.getS3URL();
        return summaryLine;
    }
}
