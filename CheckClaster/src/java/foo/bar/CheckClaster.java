package foo.bar;

import com.beust.jcommander.JCommander;
import com.datastax.driver.core.Session;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

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
    System.out.println("Запускаем писателей.");
    List<ProcessWriteValue> listProcessWriteValue = new ArrayList<>(numberThread);
    for (int i = 0; i < numberThread; i++) {
      listProcessWriteValue.add(new ProcessWriteValue(timeToWork, countDownLatchWriter, writeValue, mainID));
      listProcessWriteValue.get(i).start();
    }

    // Запускаем нити читающие данные из таблицы.
    System.out.println("Запускаем читателей.");
    List<ProcessReadValue> listProcessReadValue = new ArrayList<>(numberThread);
    for (int i = 0; i < numberThread; i++) {
      listProcessReadValue.add(new ProcessReadValue(timeToWork, countDownLatchReader, readValue, mainID));
      listProcessReadValue.get(i).start();
    }

    // Ждем когда все писатели отработают.
    try {
      countDownLatchWriter.await();
      countDownLatchReader.await();
    } catch (InterruptedException e) {
      System.err.println("Ошибка при ожидании завершения нитей: " + e.getMessage());
    }

    System.out.println("Контрольное чтение.");
    countDownLatchReader = new CountDownLatch(numberThread + 1);
    // И еще раз запускаем читателей, для проверки корретности чтения/записи.
    listProcessReadValue = new ArrayList<>(numberThread);
    for (int i = 0; i < numberThread; i++) {
      listProcessReadValue.add(new ProcessReadValue(0, countDownLatchReader, readValue, mainID));
      listProcessReadValue.get(i).start();
    }

    System.out.println("Строковый ключ: " + mainID);
    System.out.println("Писатели:");
    String header = "%-36s\t%-28s\t%-28s\t%-28s\t%-28s\t%-28s";
    String line =
      "------------------------------------\t" +
      "----------------------------\t" +
      "----------------------------\t" +
      "----------------------------\t" +
      "----------------------------\t" +
      "----------------------------\t";
    System.out.println(
      String.format(
        header + "\n%s",
        "UUID",
        "Сумма Vol",
        "Всего таймаутов",
        "Не удалось обновить счетчики",
        "Вставлено записей",
        "Обновлений в секунду",
        line
      )
    );
    long sumVol = 0;
    long totalRowInsert = 0;
    long totalInsertPerSecond = 0;

    for (ProcessWriteValue item : listProcessWriteValue) {
      System.out.println(
        String.format(
          header,
          item.getThreadUUID(),
          item.getSumVol(),
          item.getNumberOfWriteTimeoutException(),
          item.getNumberOfErrorWriteValue(),
          item.getNumberOfInsert(),
          Math.round((item.getNumberOfInsert() / item.getDuration())*1000)
        )
      );
      sumVol += item.getSumVol();
      totalInsertPerSecond += Math.round((item.getNumberOfInsert() / item.getDuration())*1000);
    }
    System.out.println(line);
    System.out.println(
      String.format(
        header,
        "",
        sumVol,
        "",
        "",
        "",
        totalInsertPerSecond
      )
    );
    System.out.println(line);

    System.out.println("Читатели:");
    header = "%-36s\t%-28s\t%-28s\t%-28s\t%-28s\t%-28s";
    line =
      "------------------------------------\t" +
        "----------------------------\t" +
        "----------------------------\t" +
        "----------------------------\t" +
        "----------------------------\t" +
        "----------------------------\t";
    System.out.println(
      String.format(
        header + "\n%s",
        "UUID",
        "Сумма Vol",
        "Всего таймаутов",
        "Не удалось обновить счетчики",
        "Считано записей",
        "Чтений в секунду",
        line
      )
    );
    sumVol = 0;
    totalRowInsert = 0;
    totalInsertPerSecond = 0;

    for (ProcessReadValue item : listProcessReadValue) {
      System.out.println(
        String.format(
          header,
          item.getThreadUUID(),
          item.getSumVol(),
          item.getNumberOfReadTimeoutException(),
          item.getNumberOfErrorReadValue(),
          item.getNumberOfProcessCol(),
          Math.round((item.getNumberOfProcessCol() / item.getDuration())*1000)
        )
      );
    }

    client.close();

  }

  private static void showUsega(JCommander jCommander) {
    jCommander.setProgramName("CheckClaster", "Проверка кластера кассандры на работоспособность.");
    jCommander.usage();
  }


}
