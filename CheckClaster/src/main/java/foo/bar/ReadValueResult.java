package foo.bar;

/**
 * Контейнер для результатов чтения данных из таблицы кассандры.
 *
 * @author Sergey.Titkov
 * @version 001.00
 * @since 001.00
 */
public class ReadValueResult {
  private Long sum = null;
  private int errorOccur = 0;
  private long count = 0;

  /**
   * @param sum        Суммарное считаных значений.
   * @param errorOccur Сколько ошибок возникло при чтении.
   * @param count      Сколько строк считано.
   */
  public ReadValueResult(Long sum, int errorOccur, long count) {
    this.sum = sum;
    this.errorOccur = errorOccur;
    this.count = count;
  }

  public Long getSum() {
    return sum;
  }

  public int getErrorOccur() {
    return errorOccur;
  }

  public long getCount() {
    return count;
  }
}
