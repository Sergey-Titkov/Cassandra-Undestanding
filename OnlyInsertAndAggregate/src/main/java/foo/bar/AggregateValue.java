package foo.bar;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import com.datastax.driver.core.utils.UUIDs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * Описание
 *
 * @author Sergey.Titkov
 * @version 001.00
 * @since 001.00
 */
public class AggregateValue {
  private Logger logger = LoggerFactory.getLogger(getClass());

  private Session session;

  // Один раз для сесси, рекомендацци DataStax
  private PreparedStatement selectCounters;
  private PreparedStatement insertAggValue;

  // CQL запросы абсолютно одинаковы для всех объектов.
  private static String selectCountersCQL =
    "select * \n" +
      "from test_data_mart.counters_values \n" +
      "where main_id = ? \n" +
      "  and insert_time < ?;";


  private static String insertAggValueCQL =
    "insert into test_data_mart.counters_aggregates(main_id, insert_time, vol_01, vol_02, vol_03) \n" +
      "values(?, ?, ?, ?, ?);";


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
  public AggregateValue(Session session, int maxErrorOccur) {
    this.session = session;
    selectCounters = this.session.prepare(selectCountersCQL);
    insertAggValue = this.session.prepare(insertAggValueCQL);
    this.maxErrorOccur = maxErrorOccur > 0 ? maxErrorOccur : this.maxErrorOccur;
  }


  public int aggregateValue(
    Long client
  ) {
    int errorOccur = maxErrorOccur;
    while (errorOccur > 0) {
      try {
        BoundStatement boundStatement;
        boundStatement = new BoundStatement(selectCounters);
        boundStatement.setFetchSize(1000);
        UUID fromTime = UUIDs.timeBased();
        ResultSet results;
        results = session.execute(boundStatement.bind(client, fromTime));
        break;
      } catch (WriteTimeoutException e) {
        logger.debug("Ошибка при обновлении: {}", e);
        errorOccur--;
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
