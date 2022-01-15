package com.usegutierrez;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {

  public static void main(String[] args) {
    System.out.println("Hello");

    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties
        .setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());

    int n = 0;
    while (true) {
      KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

      ProducerRecord<String, String> record =
          new ProducerRecord<>("first_topic", "Hello World " + n);
      producer.send(record);
      producer.close();
      n++;
    }

  }

}
