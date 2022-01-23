import config.KafkaConfig;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.KafkaProducerUtil;

public class PhStockMarketProducer extends BaseProducer {

  private static final Logger logger = LoggerFactory.getLogger(PhStockMarketProducer.class);
  private static final String URL = "http://pseapi.com/api/Sector/03-15-2018";
  private static final String TOPIC = "PSE-Kafka";

  public PhStockMarketProducer() {
    super(TOPIC, URL);
  }

  public static void main(String[] args) {
    new PhStockMarketProducer().run();
  }

}
