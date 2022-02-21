package Worker;

import SharedResources.PDFConverter;
import SharedResources.S3;
import SharedResources.Sqs;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.sqs.model.Message;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

public class Worker {

    static PDFConverter converter = new PDFConverter();
    static Sqs incomingSqs;
    static S3 s3 = new S3("*** S3 Identifier ***");

    public static void main(String[] args){
        System.out.println("Worker started");
        String sqsId = "*** workerSqs ***";
        incomingSqs = new Sqs(sqsId);
        run();
    }

    /**
     * Loops until terminated by Manager
     * Each loops handles a new worker request
     */
    private static void run() {
        while (true) {
            String localPath="", outputPath="";
            WorkerRequestMsg message = null;
            Message msgObject = getMessage();
            try {
                message = convertMsg(msgObject);
                if (message==null) throw new IOException("Error: Message isn't formatted right");
                localPath = downloadPDF(message.getFileURL());
                outputPath = converter.convert(message.getOperation(), localPath);
                String s3URL = "";
                // Max 3 tries for upload failure
                for(int i = 0; true; i++){
                    s3URL = s3.upload(outputPath);
                    if(!s3URL.equals("") && !s3URL.equals("too long"))
                        break;
                    if(i==2) throw new IOException("Error: Failed to upload result to SharedResources.S3.");
                }
                sendSQSCompleteMessage(message, s3URL);
            } catch (IOException e) {
                System.out.println("Error: Failed to complete task.\n"+e);
                if(message == null) continue;
                sendSQSFailedMessage(message, e.getMessage());
            } finally {
                // Deletes message when finished handling it
                incomingSqs.deleteMessage(msgObject);
                deleteLocalFiles(localPath, outputPath);
            }
        }
    }

    private static WorkerRequestMsg convertMsg(Message message){
        ObjectMapper mapper = new ObjectMapper();
        WorkerRequestMsg msg = null;
        try {
            msg = mapper.readValue(message.body(), WorkerRequestMsg.class);
        } catch (IOException e) {
            System.out.println("Failed to convert message: "+e);
            e.printStackTrace();
            return null;
        }
        return msg;
    }

    private static Message getMessage(){
        return incomingSqs.readBlocking();
    }

    private static String downloadPDF(String url) throws IOException {
        ProcessBuilder processBuilder = new ProcessBuilder();
        String wgetCmdLine = "wget " + url;
        String localPath = "";
        if (System.getProperty("os.name").startsWith("Windows")) {
            processBuilder.command("cmd.exe", "/c", wgetCmdLine);
        } else {
            processBuilder.command("bash", "-c", wgetCmdLine);
        }
            processBuilder.redirectErrorStream(true);
            Process process = processBuilder.start();
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            boolean downloaded=false;

            //Checks if file was downloaded successfully
            String line;
            while ((line = reader.readLine()) != null) {
                if(line.contains("saved")) downloaded = true;
            }
            localPath = System.getProperty("user.dir") + url.substring(url.lastIndexOf('/')+1);
            if(!downloaded) throw new IOException("Couldn't download PDF file.");
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

    private static void sendSQSFailedMessage(WorkerRequestMsg request, String errMsg) {
        WorkerCompleteMsg msgObject = new WorkerCompleteMsg(request.getOperation(), request.getFileURL(), errMsg);
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