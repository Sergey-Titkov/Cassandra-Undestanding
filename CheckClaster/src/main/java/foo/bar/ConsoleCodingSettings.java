package foo.bar;

import java.io.PrintStream;
import java.io.UnsupportedEncodingException;

/**
 * Устанавливаем кодировку консоли.
 * Значение читается из системной переменной java, ее можно установить например добавив ключ -DconsoleEncoding=<кодировка>
 *
 * @author Sergey.Titkov
 * @version 001.00
 * @since 001.00
 */
public class ConsoleCodingSettings {
  /**
   * Конструктор устанавливает кодировку консоли, других дополнительных действий не нужно.
   * Значение читается из системной переменной java, ее можно установить например добави ключ -DconsoleEncoding=<кодировка>
   * в параметры запуска java.
   * По умолчанию кодировка консоли у нас 866
   * @throws java.io.UnsupportedEncodingException Если не угадали с кодировкой.
   */
  public ConsoleCodingSettings() throws UnsupportedEncodingException {
    // Значение читается из системной переменной java, ее можно установить например добави ключ -DconsoleEncoding=<кодировка>
    // в параметры запуска java.
    // По умолчанию кодировка консоли у нас 866
    String consoleEncoding = System.getProperty("consoleEncoding");
    consoleEncoding = (consoleEncoding == null || consoleEncoding.equals("")) ? "CP866" : consoleEncoding;
    try {
      System.setOut(new PrintStream(System.out, true, consoleEncoding));
    } catch (UnsupportedEncodingException ex) {
      System.err.println("Unsupported encoding set for stdout: " + consoleEncoding);
      throw ex;
    }
    try {
      System.setErr((new PrintStream(System.err, true, consoleEncoding)));
    } catch (UnsupportedEncodingException ex) {
      System.err.println("Unsupported encoding set for stderr: " + consoleEncoding);
      throw ex;
    }
  }
}
