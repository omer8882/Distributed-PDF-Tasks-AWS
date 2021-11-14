package distributed;

public class SQSManager {
    private String identifier;
    public SQSManager(String identifier) {
        this.identifier = identifier;
    }

    public String readBlocking(){
        String msg = "";
        do {
            msg = tryReadFromSQS();
        }
        while (msg == "");
        return msg;

    }
    public void write (String msg){
        System.out.println("Writing to sqs: " + msg);
    }

    private static String tryReadFromSQS() {
        return "sqsMsg";
    }
    public int size(){
        return 90;
    }

}
