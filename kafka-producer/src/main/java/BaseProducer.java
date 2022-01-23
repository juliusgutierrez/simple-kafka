import exception.APIException;
import java.io.IOException;
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

public abstract class BaseProducer {

  private static final Logger logger = LoggerFactory.getLogger(BaseProducer.class);

  private KafkaProducer<String, String> producer;
  private BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);
  private String topic;
  private String URL;

  public BaseProducer(String topic) {
    this.topic = topic;
  }

  public BaseProducer(String topic, String URL) {
    this.topic = topic;
    this.URL = URL;
  }

  protected void run() {
    logger.info("Setting up!");

    //call producer
    setUpProducer();

    logger.info("making api call");

    //make api call
    makeAPICall();

    logger.info("sending to producer :" +  topic);
    //send to producer
    sendToProducer();
  }

  protected void setUpProducer() {
    producer = KafkaProducerUtil.getKafkaProducer();
  }

  protected void makeAPICall() {

    try {
      var request = HttpRequest.newBuilder()
          .uri(new URI(URL))
          .GET()
          .build();

      var response = HttpClient.newBuilder()
          .build()
          .send(request, BodyHandlers.ofString());

      msgQueue.add(response.body());
    } catch (IOException | URISyntaxException | InterruptedException e) {
      throw new APIException(e);
    }

  }

  protected void sendToProducer() {
    String msg = null;
    while (true) {
      try {
        msg = msgQueue.poll(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        logger.warn("Error : ", e);
      }
      if (msg != null) {
        logger.info(msg);
        producer.send(
            new ProducerRecord<>(this.getTopic(), msg), (recordMetadata, e) -> {
              if (e != null) {
                logger.error("Some error OR something bad happened", e);
              }
            }
        );
      }
    }
  }


  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public String getURL() {
    return URL;
  }

  public void setURL(String URL) {
    this.URL = URL;
  }

  public BlockingQueue<String> getMsgQueue() {
    return msgQueue;
  }
}
