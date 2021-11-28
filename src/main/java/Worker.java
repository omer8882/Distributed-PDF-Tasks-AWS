import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Worker {

    PDFConverter converter = new PDFConverter();
    Sqs incomingSqs;
    S3 s3 = new S3("* Bucket Name *");

    public void main(String[] args) throws IOException {
        String sqsId = args[1];
        incomingSqs = new Sqs(sqsId);
    }

    public void run(String sqsURL) {
        while (true) {
            try {
                WorkerRequestMsg message = getMessage(sqsURL);
                String localPath = downloadPDF(message.getFileURL());
                String outputPath = converter.convert(message.getOperation(), localPath);
                String s3URL = s3.upload(outputPath);
                sendSQSCompleteMessage(message, s3URL);
            } catch (IOException e) {
                System.out.println("ERROR!\n" + e);
            }
        }
    }

    private WorkerRequestMsg getMessage(String sqsURL) throws IOException {
        Message message = incomingSqs.readBlocking();
        ObjectMapper mapper = new ObjectMapper();
        WorkerRequestMsg msg = null;
        try {
            msg = mapper.readValue(message.body(), WorkerRequestMsg.class);
        } catch (IOException e) {
            System.out.println("ERROR: Couldn't read workers complete message properly.\n" + e);
        }
        return msg;
    }

    private String downloadPDF(String url) {
        ProcessBuilder processBuilder = new ProcessBuilder();
        String wgetCmdLine = "wget + " + url;
        String localPath = "";
        // Windows option just for testing
        if (System.getProperty("os.name").startsWith("Windows")) {
            processBuilder.command("cmd.exe", "/c", wgetCmdLine);
        } else {
            processBuilder.command("bash", "-c", wgetCmdLine);
        } try {
            Process process = processBuilder.start();
            //To read the output list
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
            int exitCode = process.waitFor();
            System.out.println("\nCmd with exit code: " + exitCode);
            localPath = System.getProperty("user.dir") + url.substring(url.lastIndexOf('/'));
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        return localPath;
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