import software.amazon.awssdk.services.sqs.model.Message;

import java.io.BufferedReader;
import java.io.IOException;

public class LocalApplication {
    private Sqs inputSqs;
    private Sqs resultSqs;
    private Manager manager;
    private S3 s3;

    public void run(String inputFilePath, int n, boolean terminateMode){
        System.out.println("Local application is running!");

        String resultSqsId = "LocalAppResultSqs" + System.currentTimeMillis() + ".fifo";
        String s3Id = "S3Id" + System.currentTimeMillis();

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
        if(terminateMode){
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
        try{
            String line;
            while ((line = resultFileBuffer.readLine()) != null) {
                System.out.println(line);
                // niv
                // TODO: Add each line to the HTML summary file.
            }
        }catch(IOException e){
            System.out.println(e);
        }
    }

    private Message getResultPath() {
        return resultSqs.readBlocking();
    }

    private void uploadFileToS3(String path) {
        System.out.println("Uploading "+path+" to s3");
    }

    private void activateManager(String s3Identifier, int n) {
        String inputSqsPrefix = "LocalAppInputSqs";
        if (!managerIsActive()){
            String inputSqsIdentifier = inputSqsPrefix  + System.currentTimeMillis() + ".fifo";
            startManager(inputSqsIdentifier, s3Identifier, n);
            new Thread(()->manager.run()).start();
        }
        inputSqs = new Sqs(inputSqsPrefix);

    }

    private void startManager(String sqsIdentifier,String s3Identifier, int n) {
        System.out.println("Staring manager!");
        this.manager = new Manager(sqsIdentifier, s3Identifier, n);
    }

    private boolean managerIsActive() {
        //TODO
        return false;
    }
    //TODO
    //this class should not hold a manager as field, in addition the check if manager is active should check
    // ec2 instance with that name.
}
