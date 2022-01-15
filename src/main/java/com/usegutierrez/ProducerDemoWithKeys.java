package com.usegutierrez;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.impl.SimpleLoggerFactory;

public class ProducerDemoWithKeys {

  private static Logger LOGGER =
      new SimpleLoggerFactory().getLogger(ProducerDemoWithKeys.class.getName());

  public static void main(String[] args) {
    System.out.println("Hello");

    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties
        .setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());

    //safe util
    properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
    properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
    properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");


    //high throughput util
    properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
    properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
    properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); //32KB


    for (int index = 0; index < 5; index++) {

      String topic = "first_topic";
      String value = "hello world " + index;
      String key = "id_" + index;

      LOGGER.info("Key : " + key);

      KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

      final ProducerRecord<String, String> record =
          new ProducerRecord<>(topic, key, value);

      producer.send(record, (recordMetadata, e) -> {
        if (recordMetadata != null) {
          LOGGER.info("Received metadata");
          LOGGER.info("Topic :" + recordMetadata.topic());
          LOGGER.info("Partition : " + recordMetadata.partition());
          LOGGER.info("TimeStamp : " + recordMetadata.timestamp());

        } else {
          LOGGER.error("Error ", e);
        }
      });
      producer.close();
    }

  }

}
