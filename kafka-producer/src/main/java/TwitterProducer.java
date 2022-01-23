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
import config.TwitterConfig;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.KafkaProducerUtil;

public class TwitterProducer extends BaseProducer {


  final Logger logger = LoggerFactory
      .getLogger(TwitterProducer.class);

  private List<String> trackTerms = Lists.newArrayList("kafka");
  private static String TOPIC = "Twitter-Kafka";

  public TwitterProducer() {
    super(TOPIC);
  }

  public static void main(String[] args) throws Exception {
    new TwitterProducer().run();
  }


  // Twitter Client
  public Client createTwitterClient(BlockingQueue<String> msgQueue) {

    /** Setting up a connection*/
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

  @Override
  protected void makeAPICall() {
    Client client = createTwitterClient(getMsgQueue());
    client.connect();
  }

}
