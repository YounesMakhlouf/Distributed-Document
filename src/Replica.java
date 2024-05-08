import com.rabbitmq.client.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Replica {
    private final static String QUEUE_NAME_PREFIX = "file_text_queue_";
    // Queue ou Exchange de réponse où envoyer les dernières lignes lues
    private static String replicaDirectory;

    // Méthode pour écouter les requêtes de lecture
    private static void listenForReadRequests(Channel channel, int replicaNumber) throws IOException {
        channel.exchangeDeclare(Config.REQUEST_EXCHANGE_NAME, BuiltinExchangeType.FANOUT);
        String queueNameRequest = Config.QUEUE_NAME_REQUEST_PREFIX + replicaNumber;
        channel.queueDeclare(queueNameRequest, false, false, false, null);
        channel.queueBind(queueNameRequest, Config.REQUEST_EXCHANGE_NAME, "");


        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            if (Config.READ_LAST_COMMAND.equals(message)) {
                channel.queueDeclare(Config.REPLY_QUEUE_NAME, false, false, false, null);
                // Lire la dernière ligne du fichier et envoyer la réponse
                String lastLine = readLastLine();
                channel.basicPublish("", Config.REPLY_QUEUE_NAME, null, lastLine.getBytes(StandardCharsets.UTF_8));
            }
            if (Config.READ_ALL_COMMAND.equals(message)) {
                try (BufferedReader reader = new BufferedReader(new FileReader(replicaDirectory + "/fichier.txt"))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        channel.basicPublish("", Config.REPLY_QUEUE_NAME, null, (line + "\n").getBytes(StandardCharsets.UTF_8));
                    }
                    System.out.println(" [x] rEAD ALL");

                    // Indiquer la fin de l'envoi pour ce réplica
                    channel.basicPublish("", Config.REPLY_QUEUE_NAME, null, "END_OF_FILE".getBytes(StandardCharsets.UTF_8));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
        channel.basicConsume(queueNameRequest, true, deliverCallback, consumerTag -> {
        });
    }

    // Méthode pour lire la dernière ligne du fichier local
    private static String readLastLine() {
        String lastLine = "";
        try (BufferedReader reader = new BufferedReader(new FileReader(replicaDirectory + "/fichier.txt"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                lastLine = line;
            }
        } catch (IOException e) {
            System.err.println("Erreur lors de la lecture du fichier : " + e.getMessage());
        }
        return lastLine;
    }

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
            channel.exchangeDeclare(Config.WRITER_EXCHANGE_NAME, BuiltinExchangeType.FANOUT);
            channel.queueDeclare(queueName, false, false, false, null);
            channel.queueBind(queueName, Config.WRITER_EXCHANGE_NAME, "");

            listenForReadRequests(channel, replicaNumber);

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