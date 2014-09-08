package foo.bar;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import com.datastax.driver.core.utils.UUIDs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Описание
 *
 * @author Sergey.Titkov
 * @version 001.00
 * @since 001.00
 */
public class AggregateValue {
  public long numberRow = 0;
  public long duration = 0;
  public static final int FETCH_SIZE = 10000;
  private Logger logger = LoggerFactory.getLogger(getClass());

  private Session session;

  // Один раз для сесси, рекомендацци DataStax
  private PreparedStatement selectPartCounters;
  private PreparedStatement selectAllCounters;
  private PreparedStatement insertAggValue;
  private PreparedStatement selectLastCounterslevel01;

  private static final String selectCountersBaseCQL =
    "select insert_time, vol_01, vol_02, vol_03 \n" +
      "from test_data_mart_.counters_values \n" +
      "where main_id = ? \n" +
      "and insert_time < ?";

  // CQL запросы абсолютно одинаковы для всех объектов.
  private static final String selectPartCountersCQL =
    selectCountersBaseCQL +
      "and insert_time > ? \n";

  private static final String selectAllCountersCQL =
    selectCountersBaseCQL +
      ";\n";

  private static final String insertAggValueCQL =
    "insert into test_data_mart_.counters_values_level_01(main_id, counters_values_insert_time, vol_01, vol_02, vol_03) \n" +
      "values(?, ?, ?, ?, ?);";

  private static final String selectLastCounterslevel01CQL =
    "select counters_values_insert_time, vol_01, vol_02, vol_03 \n" +
      "from test_data_mart_.counters_values_level_01 \n" +
      "where main_id = ? \n" +
      "LIMIT 1;";

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
    selectPartCounters = this.session.prepare(selectPartCountersCQL);
    selectAllCounters = this.session.prepare(selectAllCountersCQL);
    insertAggValue = this.session.prepare(insertAggValueCQL);
    selectLastCounterslevel01 = this.session.prepare(selectLastCounterslevel01CQL);

    this.maxErrorOccur = maxErrorOccur > 0 ? maxErrorOccur : this.maxErrorOccur;
  }


  public int aggregateValue(
    Long client
  ) {
    duration = -System.currentTimeMillis();

    int errorOccur = maxErrorOccur;
    while (errorOccur > 0) {
      try {
        numberRow = 0;
        long Vol01 = 0;
        long Vol02 = 0;
        long Vol03 = 0;

        long prevVol01 = 0;
        long prevVol02 = 0;
        long prevVol03 = 0;

        BoundStatement boundStatement;
        ResultSet results;
        Row row;
        results = session.execute(new BoundStatement(selectLastCounterslevel01).bind(client));

        // Достали нижнею границу.
        UUID maxLastCounterslevel01 = null;
        if (results != null && (row = results.one()) != null) {
          maxLastCounterslevel01 = row.getUUID("counters_values_insert_time");
          prevVol01 = row.getLong("vol_01");
          prevVol02 = row.getLong("vol_02");
          prevVol03 = row.getLong("vol_03");
        }
        UUID currentTime = UUIDs.startOf(System.currentTimeMillis()- TimeUnit.SECONDS.toMillis(5));
        if (maxLastCounterslevel01 != null) {
          results = session.execute(
            new BoundStatement(selectPartCounters).bind(client, currentTime, maxLastCounterslevel01).setFetchSize(
              FETCH_SIZE
            )
          );
        } else {
          results = session.execute(
            new BoundStatement(selectAllCounters).bind(client, currentTime).setFetchSize(
              FETCH_SIZE
            )
          );
        }
        UUID maxLastCounters = null;
        long count = 0;
        // Пробегаемся и суммируем.
        for (Row item : results) {
          if (maxLastCounters == null) {
            maxLastCounters = item.getUUID("insert_time");
          }
          Vol01 += item.getLong("vol_01");
          Vol02 += item.getLong("vol_02");
          Vol03 += item.getLong("vol_03");
          numberRow++;
        }
        // Вставляем новое агрегированное значение.
        if (maxLastCounters!=null){
        results = session
          .execute(
            new BoundStatement(insertAggValue)
              .bind(client, maxLastCounters, prevVol01 + Vol01, prevVol02 + Vol02, prevVol03 + Vol03)
          );
        }
        break;
      } catch (WriteTimeoutException e) {
        logger.debug("Ошибка при обновлении: {}", e);
        errorOccur--;
      }
    }
    duration += System.currentTimeMillis();
    return errorOccur;
  }

  /**
   * @return Сколько ошибок WriteTimeoutException может возникнуть при попытке изменить запись.
   */
  public int getMaxErrorOccur() {
    return maxErrorOccur;
  }
}
