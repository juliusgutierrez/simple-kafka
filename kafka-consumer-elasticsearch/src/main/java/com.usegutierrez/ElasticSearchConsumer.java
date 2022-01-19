package com.usegutierrez;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchConsumer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

  private static String hostname = "simple-kafka-6215019002.us-east-1.bonsaisearch.net";
  private static String username = "7NUsD4qtwv";
  private static String password = "kWwtmUQZD4A6JvfgNyexG893";

  public static RestHighLevelClient createClient() {

    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY,
        new UsernamePasswordCredentials(username, password));

    return new RestHighLevelClient(
        RestClient.builder(new HttpHost(hostname, 443, "https"))
            .setHttpClientConfigCallback(httpAsyncClientBuilder ->
                httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                    .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));
  }

  public static KafkaConsumer<String, String> createConsumer(String topic) {
    Properties properties = new Properties();

    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "kafka-demo-elasticsearch");
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(Arrays.asList(topic));

    return consumer;
  }


  public static void main(String[] args) throws IOException {

    RestHighLevelClient client = createClient();
    try {

      var consumer = createConsumer("PSE-Kafka");

      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

        records.forEach(record -> {
          IndexResponse response = null;
          try {

            IndexRequest indexRequest = new IndexRequest("pse", "_doc", "pse-stock-1")
                .source(record.value(), XContentType.JSON);

            response = client.index(indexRequest, RequestOptions.DEFAULT);
            LOGGER.info(response.getId());
            client.close();

          } catch (IOException e1) {
            e1.printStackTrace();

          }

        });
      }


    } catch (Exception e) {
      e.printStackTrace();
      System.exit(0);
    }
  }

  private static void log(ConsumerRecord record) {
    LOGGER.info("Key : " + record.key());
    LOGGER.info("Value : " + record.value());
    LOGGER.info("Partition : " + record.partition());
    LOGGER.info("Offset : " + record.offset());
  }


}
