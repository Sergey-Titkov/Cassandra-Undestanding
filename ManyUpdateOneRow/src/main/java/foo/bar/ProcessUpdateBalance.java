package foo.bar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

/**
 * Описание
 *
 * @author Sergey.Titkov
 * @version 001.00
 * @since 001.00
 */
public class ProcessUpdateBalance extends Thread {
  // Наш любимый логер.
  private Logger logger = LoggerFactory.getLogger(getClass());

  private UpdateBalance updateBalance;

  // Время работы нити в секундах
  private int countDownTime;

  // На этом барьере ожидает завершения работы нити породивший класс
  private CountDownLatch endWorkCDL;

  // Идентификатро нити.
  private UUID threadUUID = UUID.randomUUID();

  private Calendar lastDate = Calendar.getInstance();
  private java.math.BigDecimal lastBal;

  /**
   * Основной конструктор
   */
  public ProcessUpdateBalance(int workTime, CountDownLatch endWorkCDL, UpdateBalance updateBalance) {
    this.countDownTime = workTime;
    this.endWorkCDL = endWorkCDL;
    this.updateBalance = updateBalance;
  }

  public void run() {
    try {
      // Объект блокировки должен быть освобожден в луюбом случае!
      logger.debug("Начало работы нити: {} ", threadUUID);
      countDownTime = countDownTime < 1 ? 1 : countDownTime;

      Calendar dateEnd = Calendar.getInstance();
      dateEnd.add(Calendar.SECOND, countDownTime);

      Long clnt = Long.valueOf(1000000L);
      java.math.BigDecimal bal;

      Random rand = new Random();

      // Обновляем баланс.
      while (dateEnd.compareTo(Calendar.getInstance()) > 0) {

        // Рандомное значение наскольку надо сдвинуть дату от текущей.
        int randomMillisecond = rand.nextInt(1000000);
        // Будем прибавлять или удалять
        boolean isPositive = rand.nextBoolean();
        Calendar eventTime = Calendar.getInstance();
        eventTime.add(Calendar.MILLISECOND, isPositive ? randomMillisecond:-1*randomMillisecond);

        // Рандомный баланс
        bal = java.math.BigDecimal.valueOf(rand.nextDouble());

        // Обновляем
        updateBalance.updateBalance(clnt, bal, eventTime.getTime());

        // Протоколируем факт обновления.
        if (eventTime.compareTo(lastDate)>0){
          lastDate = eventTime;
          lastBal =  bal;
        }
      }

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

  public UUID getThreadUUID() {
    return threadUUID;
  }
}
