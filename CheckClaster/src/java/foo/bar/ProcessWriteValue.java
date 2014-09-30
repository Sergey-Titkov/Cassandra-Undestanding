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
public class ProcessWriteValue extends Thread implements Comparable<ProcessWriteValue> {
  // Наш любимый логер.
  private Logger logger = LoggerFactory.getLogger(getClass());

  // Что обновляет баланс
  private WriteValue insertValue;

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

  /**
   * Основной конструктор
   */
  public ProcessWriteValue(int workTime, CountDownLatch endWorkCDL, WriteValue insertValue, Long client) {
    this.countDownTime = workTime;
    this.endWorkCDL = endWorkCDL;
    this.insertValue = insertValue;
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
        long vol01 = rand.nextInt(10);
        long vol02 = rand.nextInt(100);
        long vol03 = rand.nextInt(50);

        vol01 = 1;
        vol02 = 10;
        vol03 = 100;
        // Обновляем
        int numberOfChance = insertValue.updateBalance(
          client,
          vol01,
          vol02,
          vol03
        );

        numberOfWriteTimeoutException = numberOfWriteTimeoutException + (insertValue
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

  /**
   * Compares this object with the specified object for order.  Returns a
   * negative integer, zero, or a positive integer as this object is less
   * than, equal to, or greater than the specified object.
   * <p/>
   * <p>The implementor must ensure <tt>sgn(x.compareTo(y)) ==
   * -sgn(y.compareTo(x))</tt> for all <tt>x</tt> and <tt>y</tt>.  (This
   * implies that <tt>x.compareTo(y)</tt> must throw an exception iff
   * <tt>y.compareTo(x)</tt> throws an exception.)
   * <p/>
   * <p>The implementor must also ensure that the relation is transitive:
   * <tt>(x.compareTo(y)&gt;0 &amp;&amp; y.compareTo(z)&gt;0)</tt> implies
   * <tt>x.compareTo(z)&gt;0</tt>.
   * <p/>
   * <p>Finally, the implementor must ensure that <tt>x.compareTo(y)==0</tt>
   * implies that <tt>sgn(x.compareTo(z)) == sgn(y.compareTo(z))</tt>, for
   * all <tt>z</tt>.
   * <p/>
   * <p>It is strongly recommended, but <i>not</i> strictly required that
   * <tt>(x.compareTo(y)==0) == (x.equals(y))</tt>.  Generally speaking, any
   * class that implements the <tt>Comparable</tt> interface and violates
   * this condition should clearly indicate this fact.  The recommended
   * language is "Note: this class has a natural ordering that is
   * inconsistent with equals."
   * <p/>
   * <p>In the foregoing description, the notation
   * <tt>sgn(</tt><i>expression</i><tt>)</tt> designates the mathematical
   * <i>signum</i> function, which is defined to return one of <tt>-1</tt>,
   * <tt>0</tt>, or <tt>1</tt> according to whether the value of
   * <i>expression</i> is negative, zero or positive.
   *
   * @param o the object to be compared.
   * @return a negative integer, zero, or a positive integer as this object
   *         is less than, equal to, or greater than the specified object.
   * @throws NullPointerException if the specified object is null
   * @throws ClassCastException   if the specified object's type prevents it
   *                              from being compared to this object.
   */
  @Override
  public int compareTo(ProcessWriteValue o) {
    return this.lastDate.compareTo(o.lastDate);
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
