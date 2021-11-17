import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SQSManager {
    private String queueURL;
    private SqsClient sqsClient;

    public SQSManager(String identifier) {
        Region region = Region.US_EAST_1;
        this.sqsClient = SqsClient.builder().region(region).build();

        String prefix = identifier;

        try {
//            ListQueuesRequest listQueuesRequest = ListQueuesRequest.builder().queueNamePrefix(prefix).build();
//            ListQueuesResponse listQueuesResponse = sqsClient.listQueues(listQueuesRequest);
//            boolean found = false;
//            for (String url : listQueuesResponse.queueUrls()) {
//                this.queueURL = url;
//                found = true;
//                System.out.println(url);
//            }
//            if (! found){
                this.queueURL = createQueue(sqsClient, identifier);
//            }

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }


    }

    // snippet-start:[sqs.java2.sqs_example.main]
    public static String createQueue(SqsClient sqsClient,String queueName ) {

        try {
            System.out.println("\nCreate Queue");
            // snippet-start:[sqs.java2.sqs_example.create_queue]

            Map<QueueAttributeName, String> attributes = new HashMap<>();
            attributes.put(QueueAttributeName.FIFO_QUEUE, Boolean.TRUE.toString());

            CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                    .queueName(queueName)
                    .attributes(attributes)
                    .build();

            sqsClient.createQueue(createQueueRequest);
            // snippet-end:[sqs.java2.sqs_example.create_queue]

            System.out.println("\nGet queue url");

            // snippet-start:[sqs.java2.sqs_example.get_queue]
            GetQueueUrlResponse getQueueUrlResponse =
                    sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
            String queueUrl = getQueueUrlResponse.queueUrl();
            return queueUrl;

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return "";
        // snippet-end:[sqs.java2.sqs_example.get_queue]
    }

    public String readBlocking() {
        String msg = "";
        do {
            msg = tryReadFromSQS();
        }
        while (msg == "");
        return msg;

    }

    public void write(String msg) {
        System.out.println("Writing to sqs: " + msg);
        sqsClient.sendMessage(SendMessageRequest.builder()
                        .queueUrl(queueURL)
                        .messageBody("Hello world!")
                        .messageGroupId("ABC")
                        .messageDeduplicationId("aaa")
                        .build());
    }

    private String tryReadFromSQS() {
        /*
    }
        try {
            for (Message message : messages) {
                DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                        .queueUrl(queueURL)
                        .receiptHandle(message.receiptHandle())
                        .build();
                sqsClient.deleteMessage(deleteMessageRequest);
            }
        return "sqsMsg";
    }

    public void delete(){
        try {
            DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder()
                    .queueUrl(queueURL)
                    .build();

            sqsClient.deleteQueue(deleteQueueRequest);

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    public int size() {
        return 90;
    }
         */
        return "bla bla";
    }

    public int size() {
        return -1;
    }
}
