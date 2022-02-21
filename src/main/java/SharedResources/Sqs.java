package SharedResources;

import software.amazon.awssdk.regions.Region;
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
        this.sqsClient = SqsClient.builder().region(region).build();

        // Checks if queue with this prefix already exists, if not, creates new queue
        try {
            if (!queueExists(identifier)) {
                this.queueURL = createQueue(identifier);
            }
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    // Return true if queue with this prefix exists, else returns false
    private boolean queueExists(String prefix){
        ListQueuesRequest listQueuesRequest = ListQueuesRequest.builder().queueNamePrefix(prefix).build();
        ListQueuesResponse listQueuesResponse = sqsClient.listQueues(listQueuesRequest);
        boolean found = false;
        for (String url : listQueuesResponse.queueUrls()) {
            this.queueURL = url;
            found = true;
            System.out.println(url);
        }
        return found;
    }

    private String createQueue(String queueName) {
        try {
            System.out.println("Creating Queue " + queueName);
            Map<QueueAttributeName, String> attributes = new HashMap<>();
            attributes.put(QueueAttributeName.FIFO_QUEUE, Boolean.TRUE.toString());

            CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                    .queueName(queueName)
                    .attributes(attributes)
                    .build();

            sqsClient.createQueue(createQueueRequest);

            GetQueueUrlResponse getQueueUrlResponse =
                    sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
            return getQueueUrlResponse.queueUrl();

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return "";
    }

    // Blocking until there is a new message to return from queue
    public Message readBlocking() {
        Message msg;
        do {
            msg = tryReadFromSQS();
        }
        while (msg == null);
        return msg;

    }

    public void write(String msg, String groupId) {
        if (groupId == "") {
            groupId = String.valueOf(System.currentTimeMillis());
        }
        sqsClient.sendMessage(SendMessageRequest.builder()
                .queueUrl(queueURL)
                .messageBody(msg)
                .messageGroupId(groupId)
                .messageDeduplicationId(String.valueOf(System.currentTimeMillis()))
                .build());
    }

    /*
    * Returns new message from queue if exists
    * else returns null
    * */
    public Message tryReadFromSQS() {
        try {
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueURL)
                    .maxNumberOfMessages(1)
                    .visibilityTimeout(75 * 60)
                    .waitTimeSeconds(3)
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

    // Deletes this queue
    public void delete() {
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
}
