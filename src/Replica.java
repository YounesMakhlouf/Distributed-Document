import com.rabbitmq.client.*;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Replica {
    private static final String EXCHANGE_NAME = "writer";
    private final static String QUEUE_NAME_PREFIX = "file_text_queue_";
    private static String replicaDirectory;

    public static void main(String[] argv) {
        if (argv.length != 1) {
            System.out.println("Usage: Java Replica <replica_number>");
            System.exit(1);
        }

        int replicaNumber = Integer.parseInt(argv[0]);
        String queueName = QUEUE_NAME_PREFIX + replicaNumber;
        replicaDirectory = "replica_" + replicaNumber;

        // Vérifier et créer le répertoire si nécessaire
        try {
            Files.createDirectories(Paths.get(replicaDirectory));
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);
            channel.queueDeclare(queueName, false, false, false, null);
            channel.queueBind(queueName, EXCHANGE_NAME, "");

            System.out.println(" [*] Waiting for messages on replica " + replicaNumber + ". To exit press CTRL+C");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                System.out.println(" [x] Received '" + message + "'");
                // Écrire le message dans un fichier
                try (FileWriter writer = new FileWriter(replicaDirectory + "/fichier.txt", true)) {
                    writer.write(message + "\n");
                    System.out.println(" [x] Message written to file: '" + message + "'");
                } catch (IOException e) {
                    System.err.println("Error writing to file: " + e.getMessage());
                }
            };
            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
            });
        } catch (Exception e) {
            System.err.println("Failed to set up the replica: " + e.getMessage());
            e.printStackTrace();
        }
    }
}