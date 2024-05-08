import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class ClientReaderV2 {
    public static void main(String[] args) throws InterruptedException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try {
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();
            channel.queueDeclare(Config.REPLY_QUEUE_NAME, false, false, false, null);

            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder().replyTo(Config.REPLY_QUEUE_NAME).build();

            Map<String, Integer> counts = new HashMap<>();

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                if (!"END_OF_FILE".equals(message)) {
                    counts.put(message, counts.getOrDefault(message, 0) + 1);
                }

                if (message.equals("END_OF_FILE")) {
                    System.out.println("Réplica a terminé d'envoyer ses lignes.");
                }
            };

            channel.basicConsume(Config.REPLY_QUEUE_NAME, true, deliverCallback, consumerTag -> {
            });

            channel.exchangeDeclare(Config.REQUEST_EXCHANGE_NAME, BuiltinExchangeType.FANOUT);
            channel.basicPublish(Config.REQUEST_EXCHANGE_NAME, "", props, Config.READ_ALL_COMMAND.getBytes(StandardCharsets.UTF_8));
            System.out.println("Requête 'Read All' envoyée.");

            // Une pause pour s'assurer de recevoir toutes les données avant de procéder.
            Thread.sleep(1000);

            // Afficher les lignes reçues de la majorité des réplicas
            counts.forEach((line, count) -> {
                if (count >= 2) { // Majorité, si on a 3 réplicas
                    System.out.println(line);
                }
            });
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
    }
}