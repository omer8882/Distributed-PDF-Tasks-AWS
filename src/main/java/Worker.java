import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.IOException;

public class Worker {

    PDFConverter converter = new PDFConverter();
    Sqs incomingSqs;
    S3 s3 = new S3("* Bucket Name *");

    public void main(String[] args) throws IOException {
        String sqsId = args[1];
        incomingSqs = new Sqs(sqsId);
    }

    public void run(String sqsURL){
        while(true){
            try{
                WorkerRequestMsg message = getMessage(sqsURL);
                String localPath = downloadPDF(message.getFileURL());
                String outputPath = converter.convert(message.getOperation(), localPath);
                String s3URL = s3.upload(outputPath);
                sendSQSCompleteMessage(message, s3URL);
            }catch(IOException e){
                System.out.println("ERROR!\n"+e);
            }
        }
    }

    private WorkerRequestMsg getMessage(String sqsURL) throws IOException {
        Message message = incomingSqs.readBlocking();
        ObjectMapper mapper = new ObjectMapper();
        WorkerRequestMsg msg = null;
        try{
            msg = mapper.readValue(message.body(), WorkerRequestMsg.class);
        }catch(IOException e){
            System.out.println("ERROR: Couldn't read workers complete message properly.\n"+e);
        }
        return msg;
    }

    private String downloadPDF(String fileURL) {
        // omer
        //TODO: Download PDF document from the internet.
        return "local path of file";
    }

    private void sendSQSCompleteMessage(WorkerRequestMsg request, String s3URL) {
        WorkerCompleteMsg msgObject = new WorkerCompleteMsg(request.getOperation(), request.getFileURL(), s3URL);
        ObjectMapper mapper = new ObjectMapper();
        try {
            String msgString = mapper.writeValueAsString(msgObject);
            Sqs resultSqs = new Sqs(request.getResultSqsId());
            resultSqs.write(msgString, "blabla");
            System.out.println("Complete message sent.");
        } catch (IOException e) {
            System.out.println(e);
        }
    }
}