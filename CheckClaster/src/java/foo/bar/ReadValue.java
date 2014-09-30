package foo.bar;

import com.datastax.driver.core.BoundStatement;
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
public class ReadValue {
  private Logger logger = LoggerFactory.getLogger(getClass());

  private Session session;

  // Один раз для сесси, рекомендацци DataStax
  private PreparedStatement insertPreparedStatement;

  // CSQL запросы абсолютно одинаковы для всех объектов.
  private static String insertCQL =
    "insert into test_data_mart_.counters_values(main_id, insert_time, vol_01, vol_02, vol_03) \n" +
      "values(?, now(), ?, ?, ?);";


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
  public ReadValue(Session session, int maxErrorOccur) {
    this.session = session;
    insertPreparedStatement = this.session.prepare(insertCQL);
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
   * @param client    Клиент
   * @return Количество оставшихся попыток обновления. Если значение ==0, это означает, что во время выполнения
   *         обновления баланса возникло  getMaxErrorOccur ошибок WriteTimeoutException.
   */
  public int updateBalance(
    Long client, long vol_01, long vol_02, long vol_03
  ) {
    int errorOccur = maxErrorOccur;
    while (errorOccur > 0) {
      try {
        BoundStatement boundStatement;
        boundStatement = new BoundStatement(insertPreparedStatement);
        session.execute(
          boundStatement.bind(client, vol_01, vol_02, vol_03)
        );
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
