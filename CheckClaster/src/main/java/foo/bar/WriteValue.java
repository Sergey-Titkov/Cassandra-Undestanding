package foo.bar;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Описание
 *
 * @author Sergey.Titkov
 * @version 001.00
 * @since 001.00
 */
public class WriteValue {
  private Logger logger = LoggerFactory.getLogger(getClass());

  private Session session;

  // Один раз для сесси, рекомендацци DataStax
  private PreparedStatement insertPreparedStatement;

  // CSQL запросы абсолютно одинаковы для всех объектов.
  private static String insertCQL =
    "insert into %s (main_id, insert_time, vol_01) \n" +
      "values(?, now(), ?);";

  /**
   * Сколько ошибок WriteTimeoutException может возникнуть при попытке изменить запись.
   */
  private int maxErrorOccur = 5;

  /**
   * Единственный возможный конструктор.
   *
   * @param session       Сессия для подключения к кластеру Кассандры.
   * @param maxErrorOccur Сколько ошибок WriteTimeoutException может возникнуть при попытке изменить запись.
   */
  public WriteValue(String fullTableName, Session session, int maxErrorOccur) {
    this.session = session;
    insertPreparedStatement = this.session.prepare(String.format(insertCQL, fullTableName));
    this.maxErrorOccur = maxErrorOccur > 0 ? maxErrorOccur : this.maxErrorOccur;
  }

  public int write(
    Long mainID, long vol_01
  ) {
    int errorOccur = 0;
    while (errorOccur <= maxErrorOccur) {
      try {
        BoundStatement boundStatement;
        boundStatement = new BoundStatement(insertPreparedStatement);
        boundStatement.setConsistencyLevel(ConsistencyLevel.ONE);
        session.execute(
          boundStatement.bind(mainID, vol_01)
        );
        break;
      } catch (WriteTimeoutException e) {
        logger.debug("Ошибка при обновлении: {}", e);
        errorOccur++;
      }
    }
    return errorOccur;
  }

  /**
   * @return Сколько ошибок WriteTimeoutException может возникнуть при попытке изменить запись.
   */
  public int getMaxErrorOccur() {
    return maxErrorOccur;
  }
}
