import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Sqs {
    private String queueURL;
    private SqsClient sqsClient;

    public Sqs(String identifier) {
        Region region = Region.US_EAST_1;
        Ec2Client  ec2Client = Ec2Client.builder().region(region).build();
        this.sqsClient = SqsClient.builder().region(region).build();

        String prefix = identifier;

        try {
            ListQueuesRequest listQueuesRequest = ListQueuesRequest.builder().queueNamePrefix(prefix).build();
            ListQueuesResponse listQueuesResponse = sqsClient.listQueues(listQueuesRequest);
            boolean found = false;
            for (String url : listQueuesResponse.queueUrls()) {
                this.queueURL = url;
                found = true;
                System.out.println(url);
            }
            if (!found) {
                this.queueURL = createQueue(sqsClient, identifier);
            }

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    public static String createQueue(SqsClient sqsClient, String queueName) {

        try {
            System.out.println("\nCreate Queue");
            Map<QueueAttributeName, String> attributes = new HashMap<>();
            attributes.put(QueueAttributeName.FIFO_QUEUE, Boolean.TRUE.toString());

            CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                    .queueName(queueName)
                    .attributes(attributes)
                    .build();

            sqsClient.createQueue(createQueueRequest);

            System.out.println("\nGet queue url");

            GetQueueUrlResponse getQueueUrlResponse =
                    sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
            return getQueueUrlResponse.queueUrl();

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return "";
    }

    public Message readBlocking() {
        Message msg;
        do {
            msg = tryReadFromSQS();
        }
        while (msg == null);
        return msg;

    }

    public void write(String msg, String groupId) {
        System.out.println("Writing to sqs: " + msg);
        sqsClient.sendMessage(SendMessageRequest.builder()
                .queueUrl(queueURL)
                .messageBody(msg)
                .messageGroupId("ABC")
                .messageDeduplicationId(String.valueOf(System.currentTimeMillis()))
                .build());
    }

    public Message tryReadFromSQS() {
        try {
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueURL)
                    .maxNumberOfMessages(1)
                    .visibilityTimeout(60)
                    .build();
            List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).messages();
            if (messages == null || messages.size() == 0) {
                return null;
            }
            return messages.get(0);
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return null;
    }

    public boolean deleteMessage(Message message) {
        try {
            DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueURL)
                    .receiptHandle(message.receiptHandle())
                    .build();
            sqsClient.deleteMessage(deleteMessageRequest);
            return true;

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            return false;
        }
    }

        public void delete () {
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

//        public int size () {
//            try {
//                ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
//                        .queueUrl(queueURL)
//                        .build();
//                List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).messages();
//                if (messages == null) {
//                    return 0;
//                }
//                return messages.size();
//            } catch (SqsException e) {
//                System.err.println(e.awsErrorDetails().errorMessage());
//                System.exit(1);
//            }
//            return -1;
//        }
    }
