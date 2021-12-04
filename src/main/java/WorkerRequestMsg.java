public class WorkerRequestMsg {
    private short operation;
    private String fileURL;
    private String resultSqsId;

    public WorkerRequestMsg(short operation, String fileURL, String resultSqsId) {
        this.operation = operation;
        this.fileURL = fileURL;
        this.resultSqsId = resultSqsId;
    }
    public WorkerRequestMsg() {
        this.operation = 0;
        this.fileURL = "";
        this.resultSqsId = "";
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

    public String getResultSqsId() {
        return resultSqsId;
    }

    public void setResultSqsId(String resultSqsId) {
        this.resultSqsId = resultSqsId;
    }
}