package foo.bar;

import com.datastax.driver.core.*;

import java.util.Date;
import java.util.List;

/**
 * Описание
 *
 * @author Sergey.Titkov
 * @version 001.00
 * @since 001.00
 */
public class ManyUpdateOneRow {

  public static void main(String[] args) {
    SimpleClient client = new SimpleClient();
    client.connect(args[0]);
    client.printMetadata();
    Session session = client.getSession();

    ResultSet results;

    results = session.execute(
      "select clnt_id, bal, eventTime \n" +
        "from test_data_mart.balances\n" +
        "where clnt_id = 8;"
    );

    System.out.println(
      String.format(
        "%-30s\t%-20s\t%-20s\n%s", "clnt_id", "bal", "eventTime",
        "-------------------------------+-----------------------+--------------------"
      )
    );
    for (Row row : results) {
      System.out.println(
        String.format(
          "%-30s\t%-20s\t%-20s", row.getLong("clnt_id"), row.getDecimal("bal"), row.getDate("eventTime")
          //row.getString("clnt_id"),
          //row.getString("bal"),
          // row.getString("eventTime")
        )
      );
    }


    System.out.println("!");

    results = session.execute(
      "update test_data_mart.balances \n" +
        "set bal = 12, \n" +
        "eventTime = '2014-08-08 12:12:12' \n" +
        "where clnt_id = 14;"
    );
    for (Row row : results) {
      System.out.println(row );
    }
    System.out.println("!!");

    results = session.execute(
      "update test_data_mart.balances \n" +
        "set bal = 12, \n" +
        "eventTime = '2014-08-08 12:12:12' \n" +
        "where clnt_id = 14" +
        "if eventTime = '2014-08-08 12:12:14';"
    );
    for (Row row : results) {
      System.out.println(row.getColumnDefinitions() );
      System.out.println(row);
    }
    System.out.println("!!!");

    //Отрабатываем бинды
    PreparedStatement statement = session.prepare(
      "update test_data_mart.balances \n" +
        "set bal = ?, \n" +
        "eventTime = ? \n" +
        "where clnt_id = ?" +
        "if eventTime = ?;"
    );
    BoundStatement boundStatement;

    boundStatement = new BoundStatement(statement);
    results = session.execute(boundStatement.bind(java.math.BigDecimal.valueOf(12.9),new Date(),Long.valueOf(14L),new Date())
    );
    for (Row row : results) {
      System.out.println(row.getColumnDefinitions() );
      System.out.println(row);
    }
    System.out.println("!!!!");

    boundStatement = new BoundStatement(statement);
    results = session.execute(boundStatement.bind(java.math.BigDecimal.valueOf(12.9),new Date(),Long.valueOf(17L),null)
    );
    for (Row row : results) {
      System.out.println(row.getColumnDefinitions() );
      System.out.println(row);
    }

    statement = session.prepare(
      "insert into test_data_mart.balances(bal, eventTime, clnt_id) \n" +
        "values(?,?, ?)\n" +
        "IF NOT EXISTS;\n"
    );
    boundStatement = new BoundStatement(statement);
    results = session.execute(boundStatement.bind(java.math.BigDecimal.valueOf(1111.78),new Date(),Long.valueOf(-999L))
    );
    List<Row> tt = results.all();
    System.out.println(">>" + tt.size());
    for (Row row : tt) {
      System.out.println(row.getColumnDefinitions() );
      System.out.println(row);
    }
    /*
    insert into test_data_mart.balances(bal, eventTime, clnt_id)
values(12.88,'2012-08-25 12:10', 22)
IF NOT EXISTS
     */
    System.out.println("!!!!!");


    UpdateBalance updateBalance = new UpdateBalance(session);
    updateBalance.updateBalance(Long.valueOf(-999L), java.math.BigDecimal.valueOf(1111.78), new Date());

    client.close();

  }
}
