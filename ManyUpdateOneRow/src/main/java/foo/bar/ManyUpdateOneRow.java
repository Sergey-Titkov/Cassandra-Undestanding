package foo.bar;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

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
    client.connect("");
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

    client.close();
  }

}
