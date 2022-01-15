import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import config.KafkaConfig;
import config.TwitterConfig;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.KafkaProducerUtil;

public class TwitterProducer {


  final Logger logger = LoggerFactory
      .getLogger(TwitterProducer.class);

  private Client client;
  private KafkaProducer<String, String> producer;
  private BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);
  private List<String> trackTerms = Lists.newArrayList("kafka");

  public static void main(String[] args) {
    new TwitterProducer().run();
  }


  // Twitter Client
  public Client createTwitterClient(BlockingQueue<String> msgQueue) {

    /** Setting up a connection   */
    Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
    StatusesFilterEndpoint hbEndpoint = new StatusesFilterEndpoint();

    // Term that I want to search on Twitter
    hbEndpoint.trackTerms(trackTerms);

    // Twitter API and tokens
    Authentication hosebirdAuth = new OAuth1(TwitterConfig.CONSUMER_KEYS,
        TwitterConfig.CONSUMER_SECRETS,
        TwitterConfig.ACCESS_TOKEN,
        TwitterConfig.ACCESS_TOKEN_SECRET);

    /** Creating a client   */
    ClientBuilder builder = new ClientBuilder()
        .name("Hosebird-Client-01")
        .hosts(hosebirdHosts)
        .authentication(hosebirdAuth)
        .endpoint(hbEndpoint)
        .processor(new StringDelimitedProcessor(msgQueue));

    Client hbClient = builder.build();
    return hbClient;
  }


  //Kafka Producer
  private KafkaProducer<String, String> createKafkaProducer() {
    return KafkaProducerUtil.getKafkaProducer();

  }


  // connect to kafka and twitter client and then start producing to kafka topic
  private void run() {
    logger.info("Setting up");

    // 1. Call the Twitter Client
    client = createTwitterClient(msgQueue);
    client.connect();

    // 2. Create Kafka Producer
    producer = createKafkaProducer();

    // Shutdown Hook
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      logger.info("Application is not stopping!");
      client.stop();
      logger.info("Closing Producer");
      producer.close();
      logger.info("Finished closing");
    }));

    int counter = 0;
    String key = "id_" + counter;

    // 3. Send Tweets to Kafka
    while (!client.isDone()) {
      String msg = null;
      try {
        msg = msgQueue.poll(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
        client.stop();
      }
      if (msg != null) {
        logger.info(msg);
        producer.send(
            new ProducerRecord<>(KafkaConfig.TOPIC, key, msg), (recordMetadata, e) -> {
              if (e != null) {
                logger.error("Some error OR something bad happened", e);
              }
            }
        );
      }
      counter++;
    }
    logger.info("\n Application End");
  }

}
