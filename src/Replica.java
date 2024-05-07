import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.File;
import java.io.IOException;

public class Replica {
    private static final String EXCHANGE_WRITE = "writer";
    private static final String EXCHANGE_READ = "reader";
    private final static String QUEUE_NAME_PREFIX = "file_text_queue_";

    private static final String filepath = "fichier.txt";
    private static String replicaDirectory;

    public static void main(String[] argv) throws Exception {
        if (argv.length != 1) {
            System.out.println("Usage: Java Replica <replica_number>");
            System.exit(1);
        }

        int replicaNumber = Integer.parseInt(argv[0]);
        String queueName = QUEUE_NAME_PREFIX + replicaNumber;
        replicaDirectory = "replica_" + replicaNumber;

        // Vérifier et créer le répertoire si nécessaire
        File directory = new File(replicaDirectory);
        if (!directory.exists()) {
            directory.mkdir();
        }
        try {
            // Établir une connexion à RabbitMQ
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();
            channel.queueDeclare(queueName, false, false, false, null);
            setupQueueAndExchange(channel, queueName, EXCHANGE_READ, replicaNumber);
            setupQueueAndExchange(channel, queueName, EXCHANGE_WRITE, replicaNumber);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void setupQueueAndExchange(Channel channel, String queueName, String exchangeName, int replicaNumber) throws IOException {
        channel.queueDeclare(queueName, false, false, false, null);

        channel.exchangeDeclare(exchangeName, BuiltinExchangeType.FANOUT);
        channel.queueBind(queueName, EXCHANGE_WRITE, "");

        System.out.println(" [*] Waiting for messages on replica " + replicaNumber + ". To exit press CTRL+C");
    }
}

