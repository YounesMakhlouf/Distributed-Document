import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class ClientWrite {
    private static final String EXCHANGE_NAME = Config.WRITER_EXCHANGE_NAME;

    public static void sendMessage(String message) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(Config.RABBITMQ_HOST);
        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);
            channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes(StandardCharsets.UTF_8));
            System.out.println(" [x] Sent '" + message + "'");
        } catch (IOException | TimeoutException e) {
            System.err.println("Failed to send message: " + e.getMessage());
        }
    }

    public static void main(String[] argv) {
        if (argv.length < 1) {
            System.err.println("Usage: ClientWrite <message>");
            System.exit(1);
        }
        String message = String.join(" ", argv);
        sendMessage(message);
    }
}