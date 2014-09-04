package foo.bar;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * Класс предназначен для обновления баланса клиента.
 * Предполагается, что обновление происходит из множества потоков, задача класса не допустить затирание баланса
 * не правильными данными.
 *
 * @author Sergey.Titkov
 * @version 001.00
 * @since 001.00
 */
public class UpdateCounterWithLWTransaction {
  private Logger logger = LoggerFactory.getLogger(getClass());

  private Session session;

  // Один раз для сесси, рекомендацци DataStax
  private PreparedStatement insertPreparedStatement;
  private PreparedStatement updatePreparedStatement;

  // CSQL запросы абсолютно одинаковы для всех объектов.
  private static String insertCQL =
    "insert into test_data_mart.counters(vol_01, vol_02, vol_03, main_id) \n" +
      "values(?, ?, ?, ?, ?)\n" +
      "IF NOT EXISTS;\n";

  private static String updateCQL =
    "update test_data_mart.counters \n" +
      "set " +
      "  vol_01 = vol_01 + ?, \n" +
      "  vol_02 = vol_02 + ?, \n" +
      "  vol_03 = vol_03 + ? \n" +
      "where main_id = ?";


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
  public UpdateCounterWithLWTransaction(Session session, int maxErrorOccur) {
    this.session = session;
    updatePreparedStatement = this.session.prepare(updateCQL);
    this.maxErrorOccur = maxErrorOccur > 0 ? maxErrorOccur : this.maxErrorOccur;
  }

  /**
   * Обновление баланса указанного клиента.
   * Для обновления баланса используется механизм легковесных транзакций.
   * Алгоритм работы следующий.
   * Если eventDate меньше чем дата в таблице кассандры, то в этом случае баланс не обновляется.
   * Если же eventDate больше чем дата в таблице кассандры, то будет предпринята попытка обновления баланса.
   * Если попытка будет не успешная, то она будет повторена. Попытки будут повторяться до тех пор пока значение в базе не
   * станет больше чем  eventDate.
   *
   * @param client Клиент
   * @param vol_01 Баланс
   * @param vol_02 Дата события.
   * @return Количество оставшихся попыток обновления. Если значение ==0, это означает, что во время выполнения
   *         обновления баланса возникло  getMaxErrorOccur ошибок WriteTimeoutException.
   */
  public int updateCounters(
    Long client, java.math.BigDecimal vol_01, java.math.BigDecimal vol_02, java.math.BigDecimal vol_03
  ) {
    UUID uuid = UUID.randomUUID();

    int errorOccur = maxErrorOccur;
    while (errorOccur > 0) {
      try {
        BoundStatement boundStatement;
        ResultSet results;
        Row row;
        boundStatement = new BoundStatement(updatePreparedStatement);
        //boundStatement.setConsistencyLevel(ConsistencyLevel.ALL);
        results = session.execute(
          boundStatement.bind(vol_01.longValue(), vol_02.longValue(), vol_03.longValue(), client)
//          boundStatement.bind(Long.valueOf(-1), Long.valueOf(-1), Long.valueOf(-1), client)
        );
        row = results.one();
        logger.debug("Вставка {}", row);
        // Как то так!
        while (row != null && !row.getBool("[applied]")) {
          boundStatement = new BoundStatement(updatePreparedStatement);
          results = session.execute(
            boundStatement.bind(
              vol_01,
              vol_02,
              vol_03,
              client
            )
          );
          row = results.one();
          logger.debug("Обновление {}", row);
        }
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
