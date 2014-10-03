package foo.bar;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Класс читающий данные из таблицы кассандры.
 *
 * @author Sergey.Titkov
 * @version 001.00
 * @since 001.00
 */
public class ReadValue {
  private Logger logger = LoggerFactory.getLogger(getClass());

  private Session session;

  // Один раз для сессии, рекомендацци DataStax
  private PreparedStatement selectPreparedStatement;

  // CSQL запрос для получения списка значений.
  private static String selectCQL =
    "select vol_01\n" +
      "from %s\n" +
      "where main_id = ?;";


  /**
   * Сколько ошибок WriteTimeoutException может возникнуть при попытке прочитать данные.
   */
  private int maxErrorOccur = 5;

  /**
   * Единственный возможный конструктор.
   *
   * @param fullTableName Полное имя таблицы откуда читаем данные.
   * @param session       Сессия для подключения к кластеру Кассандры.
   * @param maxErrorOccur Сколько ошибок WriteTimeoutException может возникнуть при попытке изменить запись.
   */
  public ReadValue(String fullTableName, Session session, int maxErrorOccur) {
    this.session = session;
    selectPreparedStatement = this.session.prepare(String.format(selectCQL, fullTableName));
    this.maxErrorOccur = maxErrorOccur > 0 ? maxErrorOccur : this.maxErrorOccur;
  }

  /**
   * Читаем данные. Метод потокобезопасный.
   *
   * @param mainID Строковый ключ.
   * @return Объект ReadValueResult, который содержит следующие информацию: сумма записанных значений, количество считанных строк, сколько ошибок возникло при чтении.
   */
  public ReadValueResult read(
    Long mainID
  ) {
    long result = 0;
    long count = 0;
    int errorOccur = 0;
    while (errorOccur <= maxErrorOccur) {
      try {
        result = 0;
        count = 0;
        BoundStatement boundStatement;
        boundStatement = new BoundStatement(selectPreparedStatement);
        // За раз читаем по 1000
        boundStatement.setFetchSize(1000);
        // Информация должна быть считана со всех нод кассандры.
        boundStatement.setConsistencyLevel(ConsistencyLevel.ALL);
        ResultSet resultSet = session.execute(
          boundStatement.bind(mainID)
        );
        for (Row row : resultSet) {
          result += row.getLong("vol_01");
          count++;
        }
        break;
      } catch (WriteTimeoutException e) {
        logger.debug("Ошибка при обновлении: {}", e);
        errorOccur++;
      }
    }
    return new ReadValueResult(result, errorOccur, count);
  }

  /**
   * @return Сколько ошибок WriteTimeoutException может возникнуть при попытке изменить запись.
   */
  public int getMaxErrorOccur() {
    return maxErrorOccur;
  }
}
