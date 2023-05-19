package constants;

public interface KafkaConstants {

  public static String KAFKA_BROKER = "localhost:29092";

  public static Integer MESSAGE_COUNT=5;

  public static String CLIENT_ID="client1";

  public static String TOPIC_NAME="demo_topic_1";

  public static String GROUP_ID_CONFIG="consumerGroup1";

  public static Integer MAX_NO_MESSAGE_FOUND_COUNT=5;

  public static String OFFSET_RESET_LATEST="latest";

  public static String OFFSET_RESET_EARLIER="earliest";

  public static Integer MAX_POLL_RECORDS=1;

}
