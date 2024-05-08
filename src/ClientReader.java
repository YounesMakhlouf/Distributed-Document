import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class ClientReader {
    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {

            channel.queueDeclare(Config.REPLY_QUEUE_NAME, false, false, false, null);

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                System.out.println("Réponse reçue : " + message);
            };
            channel.basicConsume(Config.REPLY_QUEUE_NAME, true, deliverCallback, consumerTag -> {
            });

            // Envoie une requête 'Read Last' aux réplicas
            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder().replyTo(Config.REPLY_QUEUE_NAME).build();
            channel.exchangeDeclare(Config.REQUEST_EXCHANGE_NAME, BuiltinExchangeType.FANOUT);

            channel.basicPublish(Config.REQUEST_EXCHANGE_NAME, "", props, Config.READ_LAST_COMMAND.getBytes(StandardCharsets.UTF_8));
            System.out.println("Requête 'Read Last' envoyée.");

            // Attendre un peu pour recevoir des réponses (ceci est une simplification; pour un usage réel, envisagez d'implémenter une attente plus robuste)
            Thread.sleep(10000);
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
    }
}