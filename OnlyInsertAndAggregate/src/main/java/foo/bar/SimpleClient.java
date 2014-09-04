package foo.bar;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Самый простой вариант подключеня к кластеру.
 *
 * @author Sergey.Titkov
 * @version 001.00
 * @since 001.00
 */
public class SimpleClient {
  private Logger logger = LoggerFactory.getLogger(getClass());

  private Cluster cluster;
  private Session session;

  /**
   * Подключение к кластеру кассандры.
   * @param node адресс ноды, без адреса!
   */
  public void connect(String node) {
    cluster = Cluster.builder()
      .addContactPoint(node)
      .build();
    session = cluster.connect();
    Metadata metadata = cluster.getMetadata();
    logger.debug("Подключились к кластеру: {}", metadata.getClusterName());

    for (Host host : metadata.getAllHosts()) {
      logger.debug(
        "Datatacenter: {}; Host: {}; Rack: {}", new Object[]{
        host.getDatacenter(), host.getDatacenter(), host.getDatacenter()
      }
      );
    }
  }

  public Session getSession() {
    return this.session;
  }
  public void close() {
    cluster.close();
  }

}
