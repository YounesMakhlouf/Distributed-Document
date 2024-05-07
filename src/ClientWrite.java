import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class ClientWrite {
    private static final String EXCHANGE_NAME = "writer";

    public static void sendMessage(String message) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);
            channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes((StandardCharsets.UTF_8)));
            System.out.println(" [x] Sent '" + message + "'");
        }
    }

    public static void main(String[] argv) throws Exception {
        String message = getMessage(argv);
        sendMessage(message);
    }


    private static String getMessage(String[] args) throws Exception {
        if (args.length < 1) {
            throw new Exception("No message provided.");
        }

        return String.join(" ", args);
    }
}