package bar;

import com.beust.jcommander.JCommander;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Исполняемый класс
 *
 * @author Sergey.Titkov
 * @version 001.00
 * @since 001.00
 */
public class ManyUpdateOneRow {

  public static void main(String[] args) {
    try {
      new ConsoleCodingSettings();
    } catch (UnsupportedEncodingException e) {
      return;
    }

    // Разбираем командною строку.
    CommandLineParameters commandLineParameters = new CommandLineParameters();
    JCommander jCommander;
    try {
      jCommander = new JCommander(commandLineParameters, args);
    } catch (Exception e) {
      System.err.println("Ошибка в аргументах командоной строки: " + Arrays.toString(args));
      jCommander = new JCommander(commandLineParameters);
      jCommander.setProgramName("ManyUpdateOneRow", "Многопоточное обновление значения баланса в кассандре.");
      jCommander.usage();
      return;

    }

    if (commandLineParameters.help) {
      jCommander.setProgramName("ManyUpdateOneRow", "Многопоточное обновление значения баланса в кассандре.");
      jCommander.usage();
      return;
    }

    SimpleClient client = new SimpleClient();
    client.connect(commandLineParameters.host);
    Session session = client.getSession();

    Long clnt = Long.valueOf(commandLineParameters.client);

    // Прницип работы UpdateBalance это LTS, создали и передели в нитки
    UpdateBalance updateBalance = new UpdateBalance(session, 10);

    int numberThread = commandLineParameters.numberOfThread < 0 || commandLineParameters.numberOfThread  > 128 ? 10 : commandLineParameters.numberOfThread  ;
    int timeToWork = commandLineParameters.time < 0 || commandLineParameters.time  > 86400 ? 5 : commandLineParameters.time  ;

    CountDownLatch countDownLatch = new CountDownLatch(numberThread);

    List<ProcessUpdateBalance> listProcessUpdateBalance = new ArrayList<>();
    for (int i = 0; i < numberThread; i++) {
      listProcessUpdateBalance.add(new ProcessUpdateBalance(timeToWork, countDownLatch, updateBalance, clnt));
    }

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
        "%-36s\t%-26s\t%-20s\t%-21s\t%-26s\t%-20s\n%s",
        "UUID",
        "Дата",
        "Баланс",
        "WriteTimeoutException",
        "Не удалось обновить баланс",
        "Обновлений в секунду",
        "------------------------------------\t--------------------------\t--------------------\t---------------------\t--------------------------\t--------------------"
      )
    );

    Collections.sort(listProcessUpdateBalance);
    for (ProcessUpdateBalance item : listProcessUpdateBalance) {
      System.out.println(
        String.format(
          "%-36s\t%-26s\t%-20s\t%-21s\t%-26s\t%-20s",
          item.getThreadUUID(),
          simpleDateFormat.format(item.getLastDate().getTime()),
          item.getLastBal(),
          item.getNumberOfWriteTimeoutException(),
          item.getNumberOfErrorUpdateBalance(),
          Math.round(item.getNumberUpdates()/timeToWork)
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
          "%-26s\t%-20s\t%-20s",
          simpleDateFormat.format(row.getDate("eventTime")),
          row.getDecimal("bal"),
          row.getLong("clnt_id")
        )
      );
    }
    client.close();

  }


}
