package foo.bar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

/**
 * Нить для записи данных в таблицу.
 *
 * @author Sergey.Titkov
 * @version 001.00
 * @since 001.00
 */
public class ProcessWriteValue extends Thread {
  // Наш любимый логер.
  private Logger logger = LoggerFactory.getLogger(getClass());

  // С помощью чего обновляем баланс.
  private WriteValue writeValue;

  private Long rowKey;

  // Время работы нити в секундах
  private int countDownTime;

  // На этом барьере ожидает завершения работы нити породивший класс
  private CountDownLatch endWorkCDL;

  // Идентификатро нити.
  private UUID threadUUID = UUID.randomUUID();

  // Метрики работы.
  private int numberOfWriteTimeoutException = 0;
  private int numberOfErrorWriteValue = 0;
  private long numberOfInsert = 0;
  // Время работы в микросикундах.
  private long duration = 0;
  private long sumVol = 0;

  /**
   * Основной конструктор
   */
  public ProcessWriteValue(int workTime, CountDownLatch endWorkCDL, WriteValue writeValue, Long rowKey) {
    this.countDownTime = workTime;
    this.endWorkCDL = endWorkCDL;
    this.writeValue = writeValue;
    this.rowKey = rowKey;
  }

  public void run() {
    try {
      // Объект блокировки должен быть освобожден в луюбом случае!
      logger.debug("Начало работы нити: {} ", threadUUID);
      countDownTime = countDownTime < 1 ? 1 : countDownTime;

      Calendar dateBegin = Calendar.getInstance();
      Calendar dateEnd = Calendar.getInstance();
      dateEnd.add(Calendar.SECOND, countDownTime);

      Random rand = new Random();

      do {
        long vol = rand.nextInt(100);
        // Обновляем
        int numberOfChance = writeValue.write(rowKey, vol);
        numberOfWriteTimeoutException = numberOfWriteTimeoutException + numberOfChance;
        // Увеличиваем счетчик в том случае если совсем не удалось обновить баланс.
        if (numberOfChance == writeValue.getMaxErrorOccur()) {
          numberOfErrorWriteValue++;
        }
        sumVol += vol;
        numberOfInsert++;

      } while (dateEnd.compareTo(Calendar.getInstance()) > 0);

      duration = Calendar.getInstance().getTimeInMillis() - dateBegin.getTimeInMillis();
    } finally {
      this.endWorkCDL.countDown();
      logger.debug("Конец работы нити: {}.", threadUUID);
    }
  }

  public long getSumVol() {
    return sumVol;
  }

  public long getDuration() {
    return duration;
  }

  public long getNumberOfInsert() {
    return numberOfInsert;
  }

  public int getNumberOfErrorWriteValue() {
    return numberOfErrorWriteValue;
  }

  public int getNumberOfWriteTimeoutException() {
    return numberOfWriteTimeoutException;
  }

  public UUID getThreadUUID() {
    return threadUUID;
  }
}
