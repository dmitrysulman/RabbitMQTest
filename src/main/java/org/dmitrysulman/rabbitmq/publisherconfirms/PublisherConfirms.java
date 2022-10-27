package org.dmitrysulman.rabbitmq.publisherconfirms;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmCallback;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class PublisherConfirms {

    static final int MESSAGE_COUNT = 50_000;

    private static Connection createConnection() throws Exception {
        ConnectionFactory cf = new ConnectionFactory();
        cf.setHost("localhost");
//        cf.setUsername("guest");
//        cf.setPassword("guest");
        return cf.newConnection();
    }


    public static void main(String[] args) throws Exception {
        handlePublishConfirmsAsynchronously();
    }

    static void handlePublishConfirmsAsynchronously() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = createConnection();
             Channel channel = connection.createChannel()) {

            String queue = UUID.randomUUID().toString();

            channel.queueDeclare(queue, false, false, true, null);

            channel.confirmSelect();

            ConcurrentNavigableMap<Long, String> outstandingConfirms = new ConcurrentSkipListMap<>();

            ConfirmCallback cleanOutstandingConfirms = (sequenceNumber, multiple) -> {
                if (multiple) {
                    ConcurrentNavigableMap<Long, String> confirmed = outstandingConfirms.headMap(sequenceNumber, true);
                    confirmed.clear();
                } else {
                    outstandingConfirms.remove(sequenceNumber);
                }
            };

            channel.addConfirmListener(cleanOutstandingConfirms, (sequenceNumber, multiple) -> {
                String body = outstandingConfirms.get(sequenceNumber);

                System.err.format(
                        "Message with body %s has been nack-ed. Sequence number: %d, multiple: %b%n",
                        body, sequenceNumber, multiple
                );

                cleanOutstandingConfirms.handle(sequenceNumber, multiple);
            });

            for (int i = 0; i < MESSAGE_COUNT; i++) {
                String body = String.valueOf(i);
                outstandingConfirms.put(channel.getNextPublishSeqNo(), body);
                channel.basicPublish(queue, "", null, body.getBytes());
            }
        }
    }
}
