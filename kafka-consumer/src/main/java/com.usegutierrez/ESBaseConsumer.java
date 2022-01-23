package com.usegutierrez;

import com.usegutierrez.config.BonsaiConfig;
import com.usegutierrez.exception.ESConsumerException;
import com.usegutierrez.util.KafkaConsumerUtil;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import org.apache.commons.lang3.Validate;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ESBaseConsumer {

  private String topic;

  protected ESBaseConsumer(String topic) {
    Validate.notNull(topic, "Topic should not be null");
    this.topic = topic;
  }

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PSEElasticSearchConsumer.class.getName());


  public static RestHighLevelClient createClient() {

    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY,
        new UsernamePasswordCredentials(BonsaiConfig.USERNAME, BonsaiConfig.PASSWORD));

    return new RestHighLevelClient(
        RestClient.builder(new HttpHost(BonsaiConfig.HOSTNAME, 443, "https"))
            .setHttpClientConfigCallback(httpAsyncClientBuilder ->
                httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                    .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));
  }

  public void run() {
    RestHighLevelClient client = createClient();
    try {

      var consumer = KafkaConsumerUtil.getKafkaConsumer();
      consumer.subscribe(Arrays.asList(this.topic));

      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

        records.forEach(record -> {
          IndexResponse response = null;

          try (client) {
            IndexRequest indexRequest = createIndexRequest(record);
            if (null != indexRequest) {
              response = client.index(indexRequest, RequestOptions.DEFAULT);
              LOGGER.info(response.getId());
            }

          } catch (IOException error) {
            throw new ESConsumerException(error);
          }

        });
      }


    } catch (Exception e) {
      e.printStackTrace();
      System.exit(0);
    }
  }

  protected abstract IndexRequest createIndexRequest(ConsumerRecord record);

}
