package com.usegutierrez;

import com.google.gson.JsonParser;
import com.usegutierrez.config.TwitterConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TwitterElasticSearchConsumer extends ESBaseConsumer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(TwitterElasticSearchConsumer.class.getName());

  protected TwitterElasticSearchConsumer() {
    super(TwitterConsumerConfig.TOPIC);
  }

  public static void main(String[] args) {
    new TwitterElasticSearchConsumer().run();
  }

  @Override
  protected IndexRequest createIndexRequest(ConsumerRecord record) {
    String id = null;
    try {
      id = getId(record);
    } catch (IllegalStateException e) {
      LOGGER.warn("Error encountered: ", e);
      return null;
    }

    if (null != id && null != record.value()) {
      return new IndexRequest(TwitterConsumerConfig.INDEX, "_doc", id)
          .source(record.value().toString(), XContentType.JSON);
    } else {
      LOGGER.warn("Data has no id, Skipping...");
    }
    return null;
  }

  private String getId(ConsumerRecord record) {
    return JsonParser.parseString(record.value().toString())
        .getAsJsonObject()
        .get("id_str")
        .getAsString();
  }
}
