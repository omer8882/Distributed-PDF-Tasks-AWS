package distributed;

public class LocalApplication {
    public static void run(String path, boolean terminateMode){
        System.out.println("Local application is running!");
        activateManager();
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
        String resultPathS3 = "";
        do {
            resultPathS3 = tryReadFromSQS();
        }
        while (resultPathS3 == "");
        return resultPathS3;
    }

    private static String tryReadFromSQS() {
        return "pathToResultFileS3";
    }

    private static void sendSQSMessage(String str) {
        System.out.println("sending SQS message : " + str);
    }

    private static void uploadFileToS3(String path) {
        System.out.println("Uploading "+path+" to s3");
    }

    private static void activateManager() {
        if (!managerIsActive()){
            startManager();
        }
    }

    private static void startManager() {
        System.out.println("Staring manager!");
    }

    private static boolean managerIsActive() {
        return true;
    }
}
