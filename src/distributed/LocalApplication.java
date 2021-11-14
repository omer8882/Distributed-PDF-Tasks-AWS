package distributed;

public class LocalApplication {
    private static SQSManager sqs;
    public static void run(String path, boolean terminateMode, int n){
        String sqsIdentifier = "SQSIdentifier";
        String s3Identifier = "S3Identifier";
        sqs = new SQSManager(sqsIdentifier);
        System.out.println("Local application is running!");
        activateManager(sqsIdentifier, s3Identifier, n);
        uploadFileToS3(path);
        sendSQSMessage("input file : " + path);
        String resultPath = getResultPath();
        createResultFile(resultPath);
        handleMode(terminateMode);
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

    private static void activateManager(String sqsIdentifier, String s3Identifier, int n) {
        if (!managerIsActive()){
            startManager(sqsIdentifier, s3Identifier, n);
        }
    }

    private static void startManager(String sqsIdentifier,String s3Identifier, int n) {
        System.out.println("Staring manager!");
        Manager manager = new Manager(sqsIdentifier, s3Identifier, n);
    }

    private static boolean managerIsActive() {
        return false;
    }
}
