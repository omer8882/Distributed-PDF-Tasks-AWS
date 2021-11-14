package distributed;

public class Manager {
    SQSManager localAppSqs;
    S3Manager s3Manager;
    SQSManager workersSQS;
    int n;
    public Manager(String sqsIdentifier, String s3Identifier, int n) {
        this.localAppSqs = new SQSManager(sqsIdentifier);
        this.s3Manager = new S3Manager(s3Identifier);
        this.workersSQS = new SQSManager("workers sqs");
        this.n = n;
    }

    public void run(){
        String msg = localAppSqs.readBlocking();
        if(msg == "new task"){
            handleTask(msg);
        }else if(msg == "terminate"){
            this.terminate();
        }else{
            System.out.println("Invalid sqs msg");
        }

    }

    private void handleTask(String msg) {
        String urls[] = new String[20];
        String s3Content = downloadInputFromS3(msg);
        for (String url : urls) {
            workersSQS.write("operation x on url :+ " + url);
        }
        int sqsCount = workersSQS.size();
        int m = Math.min(sqsCount/n,19);
        int k = getNumOfActiveWorkers();
        for( int i = 0;i < m-k; i++){
            createNewWorker();
        }
    }

    private void createNewWorker() {
        System.out.println("Creating new worker");
    }

    private int getNumOfActiveWorkers() {
        return 3;
    }

    private String downloadInputFromS3(String path) {
        return s3Manager.download(path);
    }

    private void terminate() {
        System.out.println("Manager terminating");
    }
}
