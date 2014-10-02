package foo.bar;

/**
 * Описание
 *
 * @author Sergey.Titkov
 * @version 001.00
 * @since 001.00
 */
public class ReadValueResult {
  private Long sum = null;
  private int errorOccur = 0;
  private long count = 0;

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
