import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.BufferedReader;
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
            try {
                Message msgObject= getMessage();
                WorkerRequestMsg message = convertMsg(msgObject);
                String localPath = downloadPDF(message.getFileURL());
                String outputPath = converter.convert(message.getOperation(), localPath);
                String s3URL = s3.upload(outputPath);
                sendSQSCompleteMessage(message, s3URL, msgObject);
            } catch (IOException e) {
                System.out.println("ERROR!\n" + e);
            }
        }
    }

    private static WorkerRequestMsg convertMsg(Message message) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        WorkerRequestMsg msg = null;
        try {
            msg = mapper.readValue(message.body(), WorkerRequestMsg.class);
        } catch (IOException e) {
            System.out.println("ERROR: Couldn't read workers complete message properly.\n" + e);
        }
        return msg;
    }

    private static Message getMessage() throws IOException {
        return incomingSqs.readBlocking();
    }

    private static String downloadPDF(String url) {
        ProcessBuilder processBuilder = new ProcessBuilder();
        String wgetCmdLine = "wget + " + url;
        String localPath = "";
        // Windows option just for testing
        if (System.getProperty("os.name").startsWith("Windows")) {
            processBuilder.command("cmd.exe", "/c", wgetCmdLine);
        } else {
            processBuilder.command("bash", "-c", wgetCmdLine);
        } try {
            Process process = processBuilder.start();//todo if cannot download return error to result sqs
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

    private static void sendSQSCompleteMessage(WorkerRequestMsg request, String s3URL, Message message) {
        WorkerCompleteMsg msgObject = new WorkerCompleteMsg(request.getOperation(), request.getFileURL(), s3URL);
        ObjectMapper mapper = new ObjectMapper();
        try {
            String msgString = mapper.writeValueAsString(msgObject);
            Sqs resultSqs = new Sqs(request.getResultSqsId());
            resultSqs.write(msgString, "");
            incomingSqs.deleteMessage(message);
            System.out.println("Complete message sent.");//todo delete the created files (download and converted)
        } catch (IOException e) {
            System.out.println(e);
        }
    }
}