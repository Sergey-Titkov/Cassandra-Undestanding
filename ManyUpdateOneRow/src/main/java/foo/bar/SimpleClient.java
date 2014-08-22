package foo.bar;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

/**
 * Описание
 *
 * @author Sergey.Titkov
 * @version 001.00
 * @since 001.00
 */
public class SimpleClient {
  private Cluster cluster;
  private Session session;

  public void connect(String node) {
    cluster = Cluster.builder()
      .addContactPoint(node)
      .build();
    session = cluster.connect();

  }
  public Session getSession(){
    return this.session;
  }

  public void printMetadata(){
    Metadata metadata = cluster.getMetadata();
    System.out.printf(
      "Connected to cluster: %s\n",
      metadata.getClusterName()
    );
    for (Host host : metadata.getAllHosts()) {
      System.out.printf(
        "Datatacenter: %s; Host: %s; Rack: %s\n",
        host.getDatacenter(), host.getAddress(), host.getRack()
      );
    }

  }

  public void close() {
    cluster.close();
  }

}
