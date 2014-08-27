package foo.bar;

import com.datastax.driver.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * Описание
 *
 * @author Sergey.Titkov
 * @version 001.00
 * @since 001.00
 */
public class UpdateBalance {
  private Logger logger = LoggerFactory.getLogger(getClass());

  private Session session;

  private PreparedStatement insertPreparedStatement;

  private PreparedStatement updatePreparedStatement;

  private static String insertCQL =
    "insert into test_data_mart.balances(bal, eventTime, clnt_id) \n" +
      "values(?,?, ?)\n" +
      "IF NOT EXISTS;\n";

  private static String updateCQL =
    "update test_data_mart.balances \n" +
      "set bal = ?, \n" +
      "eventTime = ? \n" +
      "where clnt_id = ?" +
      "if eventTime = ?;";


  public UpdateBalance(Session session) {
    this.session = session;
    insertPreparedStatement = this.session.prepare(insertCQL);
    updatePreparedStatement = this.session.prepare(updateCQL);
  }

  public void updateBalance(
    Long client, java.math.BigDecimal balance, Date eventDate
  ) {
    BoundStatement boundStatement;
    ResultSet results;
    Row row;
    boundStatement = new BoundStatement(insertPreparedStatement);
    results = session.execute(
      boundStatement.bind(balance, eventDate, client)
    );
    row = results.one();
    logger.debug("Вставка {}",row);
    // Как то так!
    while(!row.getBool("[applied]") && eventDate.compareTo(row.getDate("eventtime")) > 0){
      boundStatement = new BoundStatement(updatePreparedStatement);
      results = session.execute(
        boundStatement.bind(
          balance, eventDate, client, row.getDate("eventtime")
        )
      );
      row = results.one();
      logger.debug("Обновление {}",row);
    }

  }
}
