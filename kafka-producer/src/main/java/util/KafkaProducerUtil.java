package util;

import config.KafkaConfig;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaProducerUtil {

  private static final KafkaProducer<String, String> PRODUCER;

  static {
    // Create util properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAPSERVER);
    properties
        .setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());

    // create safe Producer
    properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    properties.setProperty(ProducerConfig.ACKS_CONFIG, KafkaConfig.ACKS_CONFIG);
    properties.setProperty(ProducerConfig.RETRIES_CONFIG, KafkaConfig.RETRIES_CONFIG);
    properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
        KafkaConfig.MAX_IN_FLIGHT_CONN);

    // Additional settings for high throughput util
    properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, KafkaConfig.COMPRESSION_TYPE);
    properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, KafkaConfig.LINGER_CONFIG);
    properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, KafkaConfig.THIRTY_TWO_KB_BATCH_SIZE);

    PRODUCER = new KafkaProducer<>(properties);

  }

  /**
   * Prevent utility to instantiate
   */
  private KafkaProducerUtil() {
  }

  public static KafkaProducer<String, String> getKafkaProducer() {
    return PRODUCER;
  }


}
