package foo.bar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

/**
 * Нить для чтения данных из таблицы.
 *
 * @author Sergey.Titkov
 * @version 001.00
 * @since 001.00
 */
public class ProcessReadValue extends Thread {
  // Наш любимый логер.
  private Logger logger = LoggerFactory.getLogger(getClass());

  private ReadValue readValue;

  private Long rowKey;

  // Время работы нити в секундах
  private int countDownTime;

  // На этом барьере ожидает завершения работы нити породивший класс
  private CountDownLatch endWorkCDL;

  // Идентификатро нити.
  private UUID threadUUID = UUID.randomUUID();

  // Метрики работы.
  private int numberOfReadTimeoutException = 0;
  private int numberOfErrorReadValue = 0;
  private long numberOfProcessCol = 0;
  // Сколько прочитали строк за последенее чтение.
  private long numberOfLastProcessCol = 0;
  // Время работы в микросикундах.
  private long duration = 0;
  private long sumVol = 0;

  /**
   * Основной конструктор
   *
   * @param workTime   сколько времени работать в секундах.
   * @param endWorkCDL На чем ждать завершения работы.
   * @param readValue  С помощюь чего читать данные.
   * @param rowKey     Строковый ключ.
   */
  public ProcessReadValue(int workTime, CountDownLatch endWorkCDL, ReadValue readValue, Long rowKey) {
    this.countDownTime = workTime;
    this.endWorkCDL = endWorkCDL;
    this.readValue = readValue;
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

      do {
        ReadValueResult readValueResult = readValue.read(rowKey);
        // Сколько было тайм утов при встаки
        numberOfReadTimeoutException = numberOfReadTimeoutException + readValue.getMaxErrorOccur();

        // Увеличиваем счетчик в том случае если совсем не удалось обновить баланс.
        if (readValueResult.getErrorOccur() == readValue.getMaxErrorOccur()) {
          numberOfErrorReadValue++;
        }

        sumVol = readValueResult.getSum();

        // Сколько столбцов обработали.
        numberOfLastProcessCol = readValueResult.getCount();
        numberOfProcessCol += readValueResult.getCount();

      } while (dateEnd.compareTo(Calendar.getInstance()) > 0);
      duration = Calendar.getInstance().getTimeInMillis() - dateBegin.getTimeInMillis();
    } finally {
      this.endWorkCDL.countDown();
      logger.debug("Конец работы нити: {}.", threadUUID);
    }
  }

  public UUID getThreadUUID() {
    return threadUUID;
  }

  public int getNumberOfReadTimeoutException() {
    return numberOfReadTimeoutException;
  }

  public int getNumberOfErrorReadValue() {
    return numberOfErrorReadValue;
  }

  public long getNumberOfProcessCol() {
    return numberOfProcessCol;
  }

  public long getDuration() {
    return duration;
  }

  public long getSumVol() {
    return sumVol;
  }

  public long getNumberOfLastProcessCol() {
    return numberOfLastProcessCol;
  }
}
