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

public class PhStockMarketProducer {

  private static final Logger logger = LoggerFactory.getLogger(PhStockMarketProducer.class);
  private static final String URL = "http://pseapi.com/api/Sector/03-15-2018";
  private BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);

  private KafkaProducer<String, String> producer;

  public static void main(String[] args) throws Exception {
    new PhStockMarketProducer().run();
  }


  public void run() throws Exception {
    logger.info("Setting up!");

    //call producer
    setUpProducer();

    //call stock ph
    makeStockPhRequest();

    //send phstock market to producer
    sendToProducer();
  }

  private void setUpProducer() {
    producer = KafkaProducerUtil.getKafkaProducer();
  }

  private void makeStockPhRequest()
      throws URISyntaxException, java.io.IOException, InterruptedException {

    var request = HttpRequest.newBuilder()
        .uri(new URI(URL))
        .GET()
        .build();

    var response = HttpClient.newBuilder()
        .build()
        .send(request, BodyHandlers.ofString());

    msgQueue.add(response.body());
  }

  private void sendToProducer() {
    String msg = null;
    while (true) {
      try {
        msg = msgQueue.poll(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      if (msg != null) {
        logger.info(msg);
        producer.send(
            new ProducerRecord<>(KafkaConfig.TOPIC, msg), (recordMetadata, e) -> {
              if (e != null) {
                logger.error("Some error OR something bad happened", e);
              }
            }
        );
      }
    }
  }

}
