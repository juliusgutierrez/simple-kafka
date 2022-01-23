import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
