public abstract class Config {
    public static final String RABBITMQ_HOST = "localhost";
    public static final String WRITER_EXCHANGE_NAME = "writer";
    public static final String READER_SEND_EXCHANGE_NAME = "reader_send";
    public static final String READER_RECEIVE_EXCHANGE_NAME = "reader_receive";
    public static final String READ_LAST_COMMAND = "Read Last";
    public static final String READ_ALL_COMMAND = "Read All";
    public final static String QUEUE_NAME_PREFIX = "file_text_queue_";
    public final static String QUEUE_NAME_REQUEST_PREFIX = "request_queue_";
    // Queue ou Exchange de réponse où envoyer les dernières lignes lues
    public final static String REPLY_QUEUE_NAME = "read_replies";
    public static final String REQUEST_EXCHANGE_NAME = "read_request";
}