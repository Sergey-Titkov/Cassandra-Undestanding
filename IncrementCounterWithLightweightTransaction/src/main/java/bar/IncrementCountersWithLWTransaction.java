package bar;

import com.beust.jcommander.JCommander;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Исполняемый класс
 *
 * @author Sergey.Titkov
 * @version 001.00
 * @since 001.00
 */
public class IncrementCountersWithLWTransaction {

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
      jCommander.setProgramName("IncrementCountersWithLWTransaction", "Многопоточное обновление значения баланса в кассандре.");
      jCommander.usage();
      return;

    }

    if (commandLineParameters.help) {
      jCommander.setProgramName("IncrementCountersWithLWTransaction", "Многопоточное обновление значения баланса в кассандре.");
      jCommander.usage();
      return;
    }

    SimpleClient client = new SimpleClient();
    client.connect(commandLineParameters.host);
    Session session = client.getSession();

    Long clnt = Long.valueOf(commandLineParameters.client);

    // Прницип работы UpdateCounterWithLWTransaction это LTS, создали и передели в нитки
    UpdateCounterWithLWTransaction updateCounterWithLWTransaction = new UpdateCounterWithLWTransaction(session, 10);

    int numberThread = commandLineParameters.numberOfThread < 0 || commandLineParameters.numberOfThread > 128 ? 10 : commandLineParameters.numberOfThread;
    int timeToWork = commandLineParameters.time < 0 || commandLineParameters.time > 86400 ? 5 : commandLineParameters.time;

    CountDownLatch countDownLatch = new CountDownLatch(numberThread);

    List<ProcessUpdateCounters> listProcessUpdateCounters = new ArrayList<>();
    for (int i = 0; i < numberThread; i++) {
      listProcessUpdateCounters.add(
        new ProcessUpdateCounters(
          timeToWork, countDownLatch,
          updateCounterWithLWTransaction, clnt
        )
      );
    }
    // Удалям данные, пока обтработка в рамках одного приложения.
    ResultSet results;
    results = session.execute(
      String.format(
        "delete from test_data_mart.counters where main_id = %s;", clnt
      )
    );

    for (int i = 0; i < numberThread; i++) {
      System.out.println("Запускаем нить:" + i);
      listProcessUpdateCounters.get(i).start();
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
        "%-36s\t%-20s\t%-20s\t%-20s\t%-20s\t%-20s\t%-20s\n%s",
        "UUID",
        "VOL 01",
        "VOL 02",
        "VOL 03",
        "WriteTimeoutException",
        "Не удалось обновить счетчики",
        "Обновлений в секунду",
        "------------------------------------\t" +
          "--------------------\t" +
          "--------------------\t" +
          "--------------------\t" +
          "--------------------\t" +
          "--------------------\t" +
          "--------------------\t"
      )
    );

    long incrementVol01 = 0;
    long incrementVol02 = 0;
    long incrementVol03 = 0;
    long numberUpdates = 0;

    for (ProcessUpdateCounters item : listProcessUpdateCounters) {
      System.out.println(
        String.format(
          "%-36s\t%-20s\t%-20s\t%-20s\t%-20s\t%-20s\t%-20s",
          item.getThreadUUID(),
          item.getIncrementVol01(),
          item.getIncrementVol02(),
          item.getIncrementVol03(),
          item.getNumberOfWriteTimeoutException(),
          item.getNumberOfErrorUpdateBalance(),
          Math.round(item.getNumberUpdates() / timeToWork)
        )
      );
      incrementVol01 += item.getIncrementVol01();
      incrementVol02 += item.getIncrementVol02();
      incrementVol03 += item.getIncrementVol03();
      numberUpdates += Math.round(item.getNumberUpdates() / timeToWork);
    }
    System.out.println(
      String.format(
        "%s",
        "------------------------------------\t" +
          "--------------------\t" +
          "--------------------\t" +
          "--------------------\t" +
          "--------------------\t" +
          "--------------------\t" +
          "--------------------\t"
      )
    );
    System.out.println(
      String.format(
      "%-36s\t%-20s\t%-20s\t%-20s\t%-20s\t%-20s\t%-20s",
      "",
      incrementVol01,
      incrementVol02,
      incrementVol03,
      "",
      "",
      numberUpdates
    )
    );

    System.out.println(String.format("\nТекущие значения счетчиков в базе."));

    results = session.execute(
      String.format(
        "select * \n" +
          "from test_data_mart.counters\n" +
          "where main_id = %s;", clnt
      )
    );

    System.out.println(
      String.format(
        "%-36s\t%-20s\t%-20s\t%-20s\t%-20s\n%s",
        "UUID",
        "VOL 01",
        "VOL 02",
        "VOL 03",
        "CLNT ID",
        "--------------------------\t" +
          "--------------------\t" +
          "--------------------\t" +
          "--------------------\t" +
          "--------------------\t"
      )
    );
    for (Row row : results) {
      System.out.println(
        String.format(
          "%-36s\t%-20s\t%-20s\t%-20s\t%-20s",
          row.getUUID("uuid"),
          row.getDecimal("vol_01"),
          row.getDecimal("vol_02"),
          row.getDecimal("vol_03"),
          row.getLong("main_id")
        )
      );
    }
    client.close();

  }


}
