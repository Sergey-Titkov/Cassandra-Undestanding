package foo.bar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Описание
 *
 * @author Sergey.Titkov
 * @version 001.00
 * @since 001.00
 */
public class ProcessAggregateValue extends Thread {
  public long totalProcessRow = 0;
  public long totalDuration = 0;

  // Наш любимый логер.
  private Logger logger = LoggerFactory.getLogger(getClass());

  // Что обновляет баланс
  private AggregateValue aggregateValue;

  // Время работы нити в секундах
  private int countDownTime;

  // На этом барьере ожидает завершения работы нити породивший класс
  private CountDownLatch endWorkCDL;

  // Идентификатро нити.
  private UUID threadUUID = UUID.randomUUID();

  private Long client;

  // С календарем удобнее работать, чем с Date
  // Полследние значения даты обновления баланса и значения баланса, нужно для контроля.
  private Calendar lastDate = Calendar.getInstance();
  private BigDecimal lastBal;

  // Метрики работы.
  private int numberOfWriteTimeoutException = 0;
  private int numberOfErrorUpdateBalance = 0;
  private long numberUpdates = 0;

  private long incrementVol01 = 0;
  private long incrementVol02 = 0;
  private long incrementVol03 = 0;

  private int period;

  /**
   * Основной конструктор
   */
  public ProcessAggregateValue(int workTime, int period, CountDownLatch endWorkCDL, AggregateValue aggregateValue, Long client) {
    this.countDownTime = workTime;
    this.endWorkCDL = endWorkCDL;
    this.aggregateValue = aggregateValue;
    this.client = client;
    this.period = period > 0 ? period : 10;

  }

  public void run() {
    try {
      // Объект блокировки должен быть освобожден в луюбом случае!
      logger.debug("Начало работы нити: {} ", threadUUID);
      countDownTime = countDownTime < 1 ? 1 : countDownTime;

      Calendar dateEnd = Calendar.getInstance();
      dateEnd.add(Calendar.SECOND, countDownTime);

      BigDecimal bal;

      Random rand = new Random();

      // Обновляем баланс.
      while (dateEnd.compareTo(Calendar.getInstance()) > 0) {

        // Обновляем
        int numberOfChance = aggregateValue.aggregateValue(
          client
        );
        totalProcessRow += aggregateValue.numberRow;
        totalDuration += aggregateValue.duration;

        numberOfWriteTimeoutException = numberOfWriteTimeoutException + (aggregateValue
          .getMaxErrorOccur() - numberOfChance);

        // Увеличиваем счетчик в том случае если совсем не удалось обновить баланс.
        if (numberOfChance == 0) {
          numberOfErrorUpdateBalance++;
        }

        // Сколько успели сделать.
        numberUpdates++;
        TimeUnit.SECONDS.sleep(period);
      }

    } catch (InterruptedException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    } finally {
      this.endWorkCDL.countDown();
      logger.debug("Конец работы нити: {}.", threadUUID);
    }
  }

  public Calendar getLastDate() {
    return lastDate;
  }

  public BigDecimal getLastBal() {
    return lastBal;
  }

  public int getNumberOfWriteTimeoutException() {
    return numberOfWriteTimeoutException;
  }

  public UUID getThreadUUID() {
    return threadUUID;
  }

  public int getNumberOfErrorUpdateBalance() {
    return numberOfErrorUpdateBalance;
  }

  public long getNumberUpdates() {
    return numberUpdates;
  }

  public long getIncrementVol01() {
    return incrementVol01;
  }

  public long getIncrementVol02() {
    return incrementVol02;
  }

  public long getIncrementVol03() {
    return incrementVol03;
  }
}
