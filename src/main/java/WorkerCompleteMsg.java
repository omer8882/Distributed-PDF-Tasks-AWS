public class WorkerCompleteMsg {
    private short operation;
    private String fileURL;
    private String s3URL;

    public WorkerCompleteMsg(short operation, String fileURL, String s3URL) {
        this.operation = operation;
        this.fileURL = fileURL;
        this.s3URL = s3URL;
    }

    public WorkerCompleteMsg() {
        this.operation = 0;
        this.fileURL = "";
        this.s3URL = "";
    }

    public short getOperation() { return operation; }

    public void setOperation(short operation) {
        this.operation = operation;
    }

    public String getFileURL() {
        return fileURL;
    }

    public void setFileURL(String fileURL) {
        this.fileURL = fileURL;
    }

    public String getS3URL() {
        return s3URL;
    }

    public void setS3URL(String s3URL) {
        this.s3URL = s3URL;
    }
}