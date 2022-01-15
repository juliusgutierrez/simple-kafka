package com.usegutierrez;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerDemo.class.getName());


  public static void main(String[] args) {

    Properties properties = new Properties();

    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my_fourth_application_2");
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

    consumer.subscribe(Arrays.asList("first_topic"));

    while (true) {
      ConsumerRecords<String, String> records =
          consumer.poll(Duration.ofMillis(100));
      records.forEach(ConsumerDemo::log);
    }

  }

  private static void log(ConsumerRecord record) {
    LOGGER.info("Key : " + record.key());
    LOGGER.info("Value : " + record.value());
    LOGGER.info("Partition : " + record.partition());
    LOGGER.info("Offset : " + record.offset());
  }


}
