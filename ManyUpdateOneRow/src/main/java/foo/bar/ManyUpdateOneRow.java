package foo.bar;

import com.datastax.driver.core.*;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;

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
    Long clnt = Long.valueOf(1000000L);


    UpdateBalance updateBalance = new UpdateBalance(session, 10);


    int numberThread = 10;
    CountDownLatch countDownLatch = new CountDownLatch(numberThread);

    List<ProcessUpdateBalance> listProcessUpdateBalance = new ArrayList<>();
    for (int i = 0; i < numberThread; i++) {
      listProcessUpdateBalance.add(new ProcessUpdateBalance(3, countDownLatch, updateBalance, clnt));
    }


    System.out.println("Обновляем баланс");

    for (int i = 0; i < numberThread; i++) {
      System.out.println("Запускаем нить:" + i);
      listProcessUpdateBalance.get(i).start();
    }
    try {
      countDownLatch.await();
    } catch (InterruptedException e) {
      System.err.println("Ошибка при ожидании завершения нитей: " + e.getMessage());
    }

    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");

    System.out.println("Закончили");
    System.out.println("Последние значения балансов для нити:");
    System.out.println(
      String.format(
        "%-36s\t%-26s\t%-20s\t%-21s\t%-26s\n%s",
        "UUID",
        "Дата",
        "Баланс",
        "WriteTimeoutException",
        "Не удалось обновить баланс",
        "------------------------------------\t--------------------------\t--------------------\t---------------------\t--------------------------"
      )
    );

    Collections.sort(listProcessUpdateBalance);
    for (ProcessUpdateBalance item : listProcessUpdateBalance) {
      System.out.println(
        String.format(
          "%-36s\t%-26s\t%-20s\t%-21s\t%-26s",
          item.getThreadUUID(),
          simpleDateFormat.format(item.getLastDate().getTime()),
          item.getLastBal(),
          item.getNumberOfWriteTimeoutException(),
          item.getNumberOfErrorUpdateBalance()
        )
      );
    }

    System.out.println(String.format("\nТекущий баланс в базе."));

    ResultSet results;
    results = session.execute(
      String.format(
        "select clnt_id, bal, eventTime \n" +
          "from test_data_mart.balances\n" +
          "where clnt_id = %s;", clnt
      )
    );
    System.out.println(
      String.format(
        "%-26s\t%-20s\t%-20s\n%s", "eventTime", "bal", "clnt",
        "--------------------------\t--------------------\t--------------------"
      )
    );
    for (Row row : results) {
      System.out.println(
        String.format(
          "%-26s\t%-20s\t%-20s",simpleDateFormat.format(row.getDate("eventTime")), row.getDecimal("bal"), row.getLong("clnt_id")
        )
      );
    }
    client.close();

  }


}
