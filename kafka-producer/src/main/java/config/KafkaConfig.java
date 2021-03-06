package config;

public class KafkaConfig {

  public static final String BOOTSTRAPSERVER = "localhost:9092";
  public static final String ACKS_CONFIG = "all";
  public static final String MAX_IN_FLIGHT_CONN = "5";
  public static final String COMPRESSION_TYPE = "snappy";
  public static final String RETRIES_CONFIG = Integer.toString(Integer.MAX_VALUE);
  public static final String LINGER_CONFIG = "20";
  public static final String THIRTY_TWO_KB_BATCH_SIZE = Integer.toString(32 * 1024);


}
