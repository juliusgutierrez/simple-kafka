package com.usegutierrez;

import com.usegutierrez.config.PSEConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PSEElasticSearchConsumer extends ESBaseConsumer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PSEElasticSearchConsumer.class.getName());

  public PSEElasticSearchConsumer() {
    super(PSEConsumerConfig.TOPIC);
  }

  public static void main(String[] args) {
    new PSEElasticSearchConsumer().run();
  }

  @Override
  public IndexRequest createIndexRequest(ConsumerRecord record) {
    return new IndexRequest(PSEConsumerConfig.INDEX, "_doc", "pse-stock-1")
        .source(record.value(), XContentType.JSON);
  }

}
