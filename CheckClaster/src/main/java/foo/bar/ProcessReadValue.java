package foo.bar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

/**
 * Нить для рандомного обновления баланса.
 *
 * @author Sergey.Titkov
 * @version 001.00
 * @since 001.00
 */
public class ProcessReadValue extends Thread implements Comparable<ProcessReadValue> {
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
  private long numberOfLastProcessCol = 0;

  private long duration = 0;

  private long sumVol = 0;

  /**
   * Основной конструктор
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
  @Override public int compareTo(ProcessReadValue o) {
    return Long.compare(duration, o.duration);
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
