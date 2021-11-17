public class LocalApplication {
    private static SQSManager sqs;
    private Manager manager;
    public void run(String path, boolean terminateMode, int n){
        System.out.println("Local application is running!");

        String sqsIdentifier = "LocalAppQueue" + System.currentTimeMillis() + ".fifo";

        String s3Identifier = "S3Identifier";
        sqs = new SQSManager(sqsIdentifier);

        activateManager(sqs, s3Identifier, n);
        uploadFileToS3(path);
        sendSQSMessage("input file : " + path);
        String resultPath = getResultPath();
        createResultFile(resultPath);
        handleMode(terminateMode);
        //sqs.delete();
    }

    private static void handleMode(boolean terminateMode) {
        if(terminateMode){
            sendTerminationToManager();
        }
    }

    private static void sendTerminationToManager() {
        System.out.println("Terminating the manager");
    }

    private static void createResultFile(String resultPath) {
        System.out.println("Downloading the results from s3 at: " + resultPath);
        System.out.println("Creating HTML representing the results");
    }

    private static String getResultPath() {
        return sqs.readBlocking();
    }

    private static void sendSQSMessage(String str) {
        sqs.write(str);
    }

    private static void uploadFileToS3(String path) {
        System.out.println("Uploading "+path+" to s3");
    }

    private void activateManager(SQSManager sqs, String s3Identifier, int n) {
        if (!managerIsActive()){
            startManager(sqs, s3Identifier, n);
        }
    }

    private void startManager(SQSManager sqs,String s3Identifier, int n) {
        System.out.println("Staring manager!");
        this.manager = new Manager(sqs, s3Identifier, n);
        manager.run();
    }

    private static boolean managerIsActive() {
        return false;
    }
}
