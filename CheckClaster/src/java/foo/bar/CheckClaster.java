package foo.bar;

import com.beust.jcommander.JCommander;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Исполняемый класс
 *
 * @author Sergey.Titkov
 * @version 001.00
 * @since 001.00
 */
public class CheckClaster {
  public static final int MAX_ERROR_OCCUR = 10;
  public static final String CLASTER_NAME = "check_cluster";
  public static final String TABLE_NAME = "test_table";

  private static final String create_keyspace =
    "CREATE KEYSPACE if not exists " + CLASTER_NAME + "\n" +
      "  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };\n";

  // Всегда изменям пространство ключей таким образом, что бы оно реплицировалось на все ноды кластера.
  private static final String alter_keyspace =
    "alter KEYSPACE " + CLASTER_NAME + " " +
      "WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : %d };";

  // Всегда создаем новую таблицу.
  private static final String create_table =
    "create table if not exists " + CLASTER_NAME + "." + TABLE_NAME + "(\n" +
      "  main_id bigint,\n" +
      "insert_time timeuuid,\n" +
      "vol_01 bigint,\n" +
      "PRIMARY KEY (main_id,insert_time)\n" +
      ") WITH CLUSTERING ORDER BY (insert_time DESC);";

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
      showUsega(jCommander);
      return;
    }

    if (commandLineParameters.help) {
      showUsega(jCommander);
      return;
    }
    SimpleClient client = new SimpleClient();
    client.connect(commandLineParameters.host);
    Session session = client.getSession();

    // Настраиваем пространсво ключей, таким образом что бы оно реплицировалось на все ноды кластера.
    session.execute(create_keyspace);
    session.execute(String.format(alter_keyspace, client.getNumberOfHosts()));

    // Создаем по необходимости таблицу
    session.execute(create_table);

    // Формируем случайный ключ строки.
    Long mainID = Long.valueOf(new Random().nextInt(1000000));

    int numberThread = commandLineParameters.numberOfThread < 0 || commandLineParameters.numberOfThread > 128 ? 10 : commandLineParameters.numberOfThread;
    int timeToWork = commandLineParameters.time < 0 || commandLineParameters.time > 86400 ? 5 : commandLineParameters.time;
    CountDownLatch countDownLatchWriter = new CountDownLatch(numberThread + 1);
    CountDownLatch countDownLatchReader = new CountDownLatch(numberThread + 1);

    // Объекты для работы с данными
    String fullTableName = CLASTER_NAME + "." + TABLE_NAME;
    WriteValue writeValue = new WriteValue(fullTableName, session, MAX_ERROR_OCCUR);
    ReadValue readValue = new ReadValue(fullTableName, session, MAX_ERROR_OCCUR);

    // Запускаем нити пищущие данные в таблицу.
    List<ProcessWriteValue> listProcessWriteValue = new ArrayList<>(numberThread);
    for (int i = 0; i < numberThread; i++) {
      listProcessWriteValue.add(new ProcessWriteValue(timeToWork, countDownLatchWriter, mainID));
      listProcessWriteValue.get(i).start();
    }

    // Запускаем нити читающие данные из таблицы.
    List<ProcessReadValue> listProcessReadValue = new ArrayList<>(numberThread);
    for (int i = 0; i < numberThread; i++) {
      listProcessReadValue.add(new ProcessReadValue(timeToWork, countDownLatchReader, mainID));
      listProcessReadValue.get(i).start();
    }

    // Ждем когда все писатели отработают.
    try {
      countDownLatchWriter.await();
      countDownLatchReader.await();
    } catch (InterruptedException e) {
      System.err.println("Ошибка при ожидании завершения нитей: " + e.getMessage());
    }
    // И еще раз запускаем читателей, для проверки корретности чтения/записи.
    List<ProcessReadValue> listProcessReadValue = new ArrayList<>(numberThread);
    for (int i = 0; i < numberThread; i++) {
      listProcessReadValue.add(new ProcessReadValue(0, countDownLatchReader, mainID));
      listProcessReadValue.get(i).start();
    }

    // Сверяем данные.

    /*

     */
//    Long clnt = Long.valueOf(commandLineParameters.mainID);
//
//
//
//
//
//    List<ProcessWriteValue> listProcessUpdateBalance = new ArrayList<>();
//    for (int i = 0; i < numberThread; i++) {
//      listProcessUpdateBalance.add(new ProcessWriteValue(timeToWork, countDownLatch, insertValue, clnt));
//    }
//
//    ProcessAggregateValue processAggregateValue = new ProcessAggregateValue(
//      timeToWork,
//      10,
//      countDownLatch,
//      aggregateValue,
//      clnt
//    );
//
//
//    ResultSet results = null;
//    UUID maxUUID = null;
//    results = session.execute(
//      new BoundStatement(
//        session.prepare(
//          "select insert_time " +
//            "from test_data_mart_.counters_values " +
//            "where main_id = ? " +
//            "LIMIT 1;"
//        )
//      ).bind(clnt)
//    );
//    Row row;
//    if (results != null && (row = results.one()) != null) {
//      maxUUID = row.getUUID("insert_time");
//    }
//
//
//    for (int i = 0; i < numberThread; i++) {
//      System.out.println("Запускаем нить:" + i);
//      listProcessUpdateBalance.get(i).start();
//    }
//    processAggregateValue.start();
//    try {
//      countDownLatch.await();
//    } catch (InterruptedException e) {
//      System.err.println("Ошибка при ожидании завершения нитей: " + e.getMessage());
//    }
//
//    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
//
//    System.out.println("Закончили");
//    System.out.println("Последние значения балансов для нити:");
//    String header = "%-36s\t%-20s\t%-20s\t%-20s\t%-20s\t%-28s\t%-20s\t%-20s";
//    String line = "------------------------------------\t" +
//      "--------------------\t" +
//      "--------------------\t" +
//      "--------------------\t" +
//      "--------------------\t" +
//      "----------------------------\t" +
//      "--------------------\t";
//    System.out.println(
//      String.format(
//        header + "\n%s",
//        "UUID",
//        "VOL 01",
//        "VOL 02",
//        "VOL 03",
//        "WriteTimeoutException",
//        "Не удалось обновить счетчики",
//        "Вставлено записей",
//        "Обновлений в секунду",
//        line
//      )
//    );
//
//    long incrementVol01 = 0;
//    long incrementVol02 = 0;
//    long incrementVol03 = 0;
//    long totalUpdates = 0;
//    long numberUpdates = 0;
//
//    for (ProcessWriteValue item : listProcessUpdateBalance) {
//      System.out.println(
//        String.format(
//          header,
//          item.getThreadUUID(),
//          item.getIncrementVol01(),
//          item.getIncrementVol02(),
//          item.getIncrementVol03(),
//          item.getNumberOfWriteTimeoutException(),
//          item.getNumberOfErrorUpdateBalance(),
//          item.getNumberUpdates(),
//          Math.round(item.getNumberUpdates() / timeToWork)
//        )
//      );
//      incrementVol01 += item.getIncrementVol01();
//      incrementVol02 += item.getIncrementVol02();
//      incrementVol03 += item.getIncrementVol03();
//      totalUpdates += item.getNumberUpdates();
//      numberUpdates += Math.round(item.getNumberUpdates() / timeToWork);
//    }
//    System.out.println(
//      String.format(
//        "%s",
//        line
//      )
//    );
//    System.out.println(
//      String.format(
//        header,
//        "",
//        incrementVol01,
//        incrementVol02,
//        incrementVol03,
//        "",
//        "",
//        totalUpdates,
//        numberUpdates
//      )
//    );
//
//    long testVol01 = 0;
//    long testVol02 = 0;
//    long testVol03 = 0;
//    long count = 0;
//
//    BoundStatement boundStatement;
//    Calendar firstDate = Calendar.getInstance();
//
//    if (maxUUID != null) {
//      // А вот теперь читаем с начала до отсечки :)
//      results = session.execute(
//        new BoundStatement(
//          session.prepare(
//            "select  vol_01, vol_02, vol_03 \n" +
//              "from test_data_mart_.counters_values \n" +
//              "where main_id = ? \n" +
//              "and insert_time > ? \n" +
//              ";\n"
//          )
//        ).bind(clnt, maxUUID)
//      );
//    } else {
//      // А вот теперь читаем с начала до отсечки :)
//      results = session.execute(
//        new BoundStatement(
//          session.prepare(
//            "select  vol_01, vol_02, vol_03 \n" +
//              "from test_data_mart_.counters_values \n" +
//              "where main_id = ? \n" +
//              ";\n"
//          )
//        ).bind(clnt)
//      );
//
//    }
//
//    testVol01 = 0;
//    testVol02 = 0;
//    testVol03 = 0;
//    count = 0;
//    for (Row item : results) {
//      testVol01 += item.getLong("vol_01");
//      testVol02 += item.getLong("vol_02");
//      testVol03 += item.getLong("vol_03");
//      count++;
//    }
//    Calendar secondDate = Calendar.getInstance();
//    long totalRow = count;
//    long millesecDifference = (count / (secondDate.getTimeInMillis() - firstDate.getTimeInMillis())) * 1000;
//
//    System.out.println(String.format("%s", line));
//
//    System.out.println(
//      String.format(
//        header,
//        "Кассандра",
//        testVol01,
//        testVol02,
//        testVol03,
//        "",
//        "",
//        totalRow,
//        millesecDifference
//      )
//    );
//    System.out.println(String.format("%s", line));
//
//    System.out.println("Проверям работу агрегатора.");
//
//    results = session.execute(
//      new BoundStatement(
//        session.prepare(
//          "select counters_values_insert_time, vol_01, vol_02, vol_03 \n" +
//            "from test_data_mart_.counters_values_level_01 \n" +
//            "where main_id = ? \n" +
//            "LIMIT 1;\n"
//        )
//      ).bind(clnt)
//    );
//    row = results.one();
//    long aggvol01 = row.getLong("vol_01");
//    long aggvol02 = row.getLong("vol_02");
//    long aggvol03 = row.getLong("vol_03");
//    System.out.println(
//      String.format(
//        header,
//        "Накопители",
//        aggvol01,
//        aggvol02,
//        aggvol03,
//        "",
//        "",
//        processAggregateValue.totalProcessRow,
//        TimeUnit.MILLISECONDS.toSeconds(processAggregateValue.totalDuration)
//      )
//    );
//    maxUUID = row.getUUID("counters_values_insert_time");
//
//    // А вот теперь читаем с момента последней агрегации
//    results = session.execute(
//      new BoundStatement(
//        session.prepare(
//          "select  vol_01, vol_02, vol_03 \n" +
//            "from test_data_mart_.counters_values \n" +
//            "where main_id = ? \n" +
//            "and insert_time > ? \n" +
//            ";\n"
//        )
//      ).bind(clnt, maxUUID)
//    );
//    testVol01 = 0;
//    testVol02 = 0;
//    testVol03 = 0;
//    count = 0;
//    for (Row item : results) {
//      testVol01 += item.getLong("vol_01");
//      testVol02 += item.getLong("vol_02");
//      testVol03 += item.getLong("vol_03");
//      count++;
//    }
//    secondDate = Calendar.getInstance();
//    totalRow = count;
//    millesecDifference = (count / (secondDate.getTimeInMillis() - firstDate.getTimeInMillis())) * 1000;
//
//    System.out.println(String.format("%s", line));
//
//    System.out.println(
//      String.format(
//        header,
//        "С момента последней агрегации",
//        testVol01,
//        testVol02,
//        testVol03,
//        "",
//        "",
//        totalRow,
//        millesecDifference
//      )
//    );
//    System.out.println(String.format("%s", line));
//    aggvol01 += testVol01;
//    aggvol02 += testVol02;
//    aggvol03 += testVol03;
//
//    System.out.println(
//      String.format(
//        header,
//        "Сумарно",
//        aggvol01,
//        aggvol02,
//        aggvol03,
//        "",
//        "",
//        totalRow,
//        millesecDifference
//      )
//    );
//    System.out.println(String.format("%s", line));
//    firstDate = Calendar.getInstance();
//    // А вот теперь читаем с начала до отсечки :)
//    results = session.execute(
//      new BoundStatement(
//        session.prepare(
//          "select  vol_01, vol_02, vol_03 \n" +
//            "from test_data_mart_.counters_values \n" +
//            "where main_id = ? \n" +
//            ";\n"
//        )
//      ).bind(clnt)
//    );
//    testVol01 = 0;
//    testVol02 = 0;
//    testVol03 = 0;
//    count = 0;
//    for (Row item : results) {
//      testVol01 += item.getLong("vol_01");
//      testVol02 += item.getLong("vol_02");
//      testVol03 += item.getLong("vol_03");
//      count++;
//    }
//    secondDate = Calendar.getInstance();
//    totalRow = count;
//    millesecDifference = (count / (secondDate.getTimeInMillis() - firstDate.getTimeInMillis())) * 1000;
//
//    System.out.println(String.format("%s", line));
//
//    System.out.println(
//      String.format(
//        header,
//        "Всего",
//        testVol01,
//        testVol02,
//        testVol03,
//        "",
//        "",
//        totalRow,
//        millesecDifference
//      )
//    );
//    System.out.println(String.format("%s", line));
//    System.out.println(String.format("%s", line));
//
//    System.out.println(
//      String.format(
//        header,
//        "Расхождение",
//        testVol01 - aggvol01,
//        testVol02 - aggvol02,
//        testVol03 - aggvol03,
//        "",
//        "",
//        "",
//        ""
//      )
//    );
//    System.out.println(String.format("%s", line));


    client.close();

  }

  private static void showUsega(JCommander jCommander) {
    jCommander.setProgramName("CheckClaster", "Проверка кластера кассандры на работоспособность.");
    jCommander.usage();
  }


}
