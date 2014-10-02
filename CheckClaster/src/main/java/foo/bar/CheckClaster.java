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
    CountDownLatch countDownLatchWriter = new CountDownLatch(numberThread);
    CountDownLatch countDownLatchReader = new CountDownLatch(numberThread);

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

    // Ждем когда закончится контрольное чтение.
    try {
      countDownLatchReader.await();
    } catch (InterruptedException e) {
      System.err.println("Ошибка при ожидании завершения нитей: " + e.getMessage());
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
        "Вставок в секунду",
        line
      )
    );
    long totalWriteVol = 0;
    long totalWriteCol = 0;
    long totalWriteRowPerSecond = 0;
    long totalWriteNumberOfTimeoutException = 0;
    long totalWriteNumberOfTimeoutPerSecond = 0;
    long totalWriteNumberOfErrorValue = 0;

    for (ProcessWriteValue item : listProcessWriteValue) {
      System.out.println(
        String.format(
          header,
          item.getThreadUUID(),
          item.getSumVol(),
          item.getNumberOfWriteTimeoutException(),
          item.getNumberOfErrorWriteValue(),
          item.getNumberOfInsert(),
          Math.round(((float) item.getNumberOfInsert() / item.getDuration()) * 1000)
        )
      );
      totalWriteVol += item.getSumVol();
      totalWriteCol += item.getNumberOfInsert();
      totalWriteRowPerSecond += Math.round(((float) item.getNumberOfInsert() / item.getDuration()) * 1000);
      totalWriteNumberOfTimeoutException += item.getNumberOfWriteTimeoutException();
      totalWriteNumberOfErrorValue += item.getNumberOfErrorWriteValue();
      totalWriteNumberOfTimeoutPerSecond += Math.round(((float) item.getNumberOfWriteTimeoutException() / item.getDuration()) * 1000);
    }
    System.out.println(line);
    System.out.println(
      String.format(
        header,
        "",
        totalWriteVol,
        totalWriteNumberOfTimeoutException,
        totalWriteNumberOfErrorValue,
        totalWriteCol,
        totalWriteRowPerSecond
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
        "-//-",
        "Всего таймаутов",
        "Не удалось прочитать счетчики",
        "Считано записей",
        "Чтений в секунду",
        line
      )
    );
    long totalReadCol = 0;
    long totalReadRowPerSecond = 0;
    long totalReadNumberOfTimeoutException = 0;
    long totalReadNumberOfErrorValue = 0;
    long totalReadNumberOfTimeoutPerSecond = 0;

    for (ProcessReadValue item : listProcessReadValue) {
      System.out.println(
        String.format(
          header,
          item.getThreadUUID(),
          item.getSumVol(),
          item.getNumberOfReadTimeoutException(),
          item.getNumberOfErrorReadValue(),
          item.getNumberOfProcessCol(),
          Math.round(((float) item.getNumberOfProcessCol() / item.getDuration()) * 1000)
        )
      );
      totalReadCol += item.getNumberOfProcessCol();
      totalReadRowPerSecond += Math.round(((float) item.getNumberOfProcessCol() / item.getDuration()) * 1000);
      totalReadNumberOfTimeoutException += item.getNumberOfReadTimeoutException();
      totalReadNumberOfErrorValue += item.getNumberOfErrorReadValue();
      totalReadNumberOfTimeoutPerSecond += Math.round(
        ((float) item.getNumberOfReadTimeoutException() / item.getDuration()) * 1000
      );

    }
    System.out.println(line);
    System.out.println(
      String.format(
        header,
        "",
        "",
        totalReadNumberOfTimeoutException,
        totalReadNumberOfErrorValue,
        totalReadCol,
        totalReadRowPerSecond
      )
    );
    System.out.println(line);

    System.out.println("Контрольное чтение.");
    countDownLatchReader = new CountDownLatch(numberThread);
    // И еще раз запускаем читателей, для проверки корретности чтения/записи.
    listProcessReadValue = new ArrayList<>(numberThread);
    for (int i = 0; i < numberThread; i++) {
      listProcessReadValue.add(new ProcessReadValue(5, countDownLatchReader, readValue, mainID));
      listProcessReadValue.get(i).start();
    }

    // Ждем когда все писатели отработают.
    try {
      countDownLatchReader.await();
    } catch (InterruptedException e) {
      System.err.println("Ошибка при ожидании завершения нитей: " + e.getMessage());
    }

    header = "%-36s\t%-28s\t%-28s\t%-28s\t%-28s";
    line =
      "------------------------------------\t" +
        "----------------------------\t" +
        "----------------------------\t" +
        "----------------------------\t" +
        "----------------------------\t";
    System.out.println(
      String.format(
        header + "\n%s",
        "UUID",
        "Сумма Vol",
        "Разница с эталон",
        "Считано записей",
        "Разница с эталон",
        line
      )
    );
    for (ProcessReadValue item : listProcessReadValue) {
      System.out.println(
        String.format(
          header,
          item.getThreadUUID(),
          item.getSumVol(),
          item.getSumVol() - totalWriteVol,
          item.getNumberOfLastProcessCol(),
          item.getNumberOfLastProcessCol() - totalWriteCol
        )
      );
    }
    System.out.println(line);
    System.out.println();
    System.out.println( String.format("Скорость записи: %s значений в сек.", totalWriteRowPerSecond));
    System.out.println( String.format("Таймаутов при записи в секунду: %s", totalWriteNumberOfTimeoutPerSecond));
    System.out.println();
    System.out.println( String.format("Скорость чтения: %s значений в сек", totalReadRowPerSecond));
    System.out.println( String.format("Таймаутов при чтении в секунду: %s", totalReadNumberOfTimeoutPerSecond));

    client.close();

  }

  private static void showUsega(JCommander jCommander) {
    jCommander.setProgramName("CheckClaster", "Проверка кластера кассандры на работоспособность.");
    jCommander.usage();
  }


}
