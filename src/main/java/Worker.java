import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

public class Worker {

    static PDFConverter converter = new PDFConverter();
    static Sqs incomingSqs;
    static S3 s3 = new S3("mybucket920463236");

    public static void main(String[] args) throws IOException {
//        String sqsId = args[1]; // todo return to that
        String sqsId = "workerSqs1638641462773.fifo";
        incomingSqs = new Sqs(sqsId);
        run();
    }

    public static void run() {
        while (true) {
            String localPath="", outputPath="";
            WorkerRequestMsg message = null;
            Message msgObject = getMessage();
            try {
                message = convertMsg(msgObject);
                if(message==null) throw new IOException("Message isn't formatted right");
                localPath = downloadPDF(message.getFileURL());
                outputPath = converter.convert(message.getOperation(), localPath);
                String s3URL = s3.upload(outputPath);
                sendSQSCompleteMessage(message, s3URL);
            } catch (IOException e) {
                System.out.println("Error: Failed to complete task.\n"+e);
                if(message == null) continue;
                sendSQSFailedMessage(message);
            } finally {
                incomingSqs.deleteMessage(msgObject);
                deleteLocalFiles(localPath, outputPath);
            }
        }
    }

    private static WorkerRequestMsg convertMsg(Message message) {
        ObjectMapper mapper = new ObjectMapper();
        WorkerRequestMsg msg = null;
        try {
            msg = mapper.readValue(message.body(), WorkerRequestMsg.class);
        } catch (JsonProcessingException e) {
            System.out.println("Failed to convert message: "+e);
            return null;
        }
        return msg;
    }

    private static Message getMessage(){
        return incomingSqs.readBlocking();
    }

    private static String downloadPDF(String url) throws IOException {
        ProcessBuilder processBuilder = new ProcessBuilder();
        String wgetCmdLine = "wget + " + url;
        String localPath = "";
        // Windows option just for testing
        if (System.getProperty("os.name").startsWith("Windows")) {
            processBuilder.command("cmd.exe", "/c", wgetCmdLine);
        } else {
            processBuilder.command("bash", "-c", wgetCmdLine);
        }
            processBuilder.redirectErrorStream(true);
            Process process = processBuilder.start();
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            boolean downloaded=false;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
                if(line.startsWith("Downloaded: 1")) downloaded = true;
            }
            localPath = System.getProperty("user.dir") + url.substring(url.lastIndexOf('/'));
            if(!downloaded) throw new IOException("Couldn't download PDF file. Aborting task.");
        return localPath;
    }

    private static void sendSQSCompleteMessage(WorkerRequestMsg request, String s3URL) {
        WorkerCompleteMsg msgObject = new WorkerCompleteMsg(request.getOperation(), request.getFileURL(), s3URL);
        ObjectMapper mapper = new ObjectMapper();
        try {
            String msgString = mapper.writeValueAsString(msgObject);
            Sqs resultSqs = new Sqs(request.getResultSqsId());
            resultSqs.write(msgString, "");
            System.out.println("Complete message sent.");
        } catch (IOException e) {
            System.out.println(e);
        }
    }

    private static void sendSQSFailedMessage(WorkerRequestMsg request) {
        WorkerCompleteMsg msgObject = new WorkerCompleteMsg(request.getOperation(), request.getFileURL(), "ERROR: Couldn't Complete task.");
        ObjectMapper mapper = new ObjectMapper();
        try {
            String msgString = mapper.writeValueAsString(msgObject);
            Sqs resultSqs = new Sqs(request.getResultSqsId());
            resultSqs.write(msgString, "");
            System.out.println("Failure message sent.");
        } catch (IOException e) {
            System.out.println(e);
        }
    }

    private static void deleteLocalFiles(String localPath, String outputPath) {
        File f = new File(localPath);
        if(f.delete()) {
            System.out.println("Deleted local pdf file.");
        }
        f = new File(outputPath);
        if(f.delete()) {
            System.out.println("Deleted local converted file.");
        }
    }
}