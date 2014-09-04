package foo.bar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

/**
 * Нить для рандомного обновления баланса.
 *
 * @author Sergey.Titkov
 * @version 001.00
 * @since 001.00
 */
public class ProcessUpdateCounters extends Thread {
  // Наш любимый логер.
  private Logger logger = LoggerFactory.getLogger(getClass());

  // Что обновляет баланс
  private UpdateCounterWithLWTransaction updateCounterWithLWTransaction;

  // Время работы нити в секундах
  private int countDownTime;

  // На этом барьере ожидает завершения работы нити породивший класс
  private CountDownLatch endWorkCDL;

  // Идентификатро нити.
  private UUID threadUUID = UUID.randomUUID();

  private Long client;

  // Метрики работы.
  private int numberOfWriteTimeoutException = 0;
  private int numberOfErrorUpdateBalance = 0;
  private long numberUpdates = 0;

  private long incrementVol01 = 0;
  private long incrementVol02 = 0;
  private long incrementVol03 = 0;


  /**
   * Основной конструктор
   */
  public ProcessUpdateCounters(int workTime, CountDownLatch endWorkCDL, UpdateCounterWithLWTransaction updateCounterWithLWTransaction, Long client) {
    this.countDownTime = workTime;
    this.endWorkCDL = endWorkCDL;
    this.updateCounterWithLWTransaction = updateCounterWithLWTransaction;
    this.client = client;
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

        // Рандомные значения счетчиков.
        boolean isPositive = rand.nextBoolean();
        long vol01 = isPositive ? 1 : -1 * rand.nextInt(5);
        isPositive = rand.nextBoolean();
        long vol02 = isPositive ? 1 : -1 * rand.nextInt(5);
        isPositive = rand.nextBoolean();
        long vol03 = isPositive ? 1 : -1 * rand.nextInt(5);

        // Рандомный баланс
        bal = BigDecimal.valueOf(rand.nextDouble());

        // Обновляем
        int numberOfChance = updateCounterWithLWTransaction.updateCounters(
          this.client, BigDecimal.valueOf(vol01), BigDecimal.valueOf(
          vol02
        ), BigDecimal.valueOf(vol03)
        );

        numberOfWriteTimeoutException = numberOfWriteTimeoutException + (updateCounterWithLWTransaction
          .getMaxErrorOccur() - numberOfChance);

        // Увеличиваем счетчик в том случае если совсем не удалось обновить баланс.
        if (numberOfChance == 0) {
          numberOfErrorUpdateBalance++;
        }

        incrementVol01 += vol01;
        incrementVol02 += vol02;
        incrementVol03 += vol03;

        // Сколько успели сделать.
        numberUpdates++;
      }

    } finally {
      this.endWorkCDL.countDown();
      logger.debug("Конец работы нити: {}.", threadUUID);
    }
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
