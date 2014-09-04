package foo.bar;

import com.datastax.driver.core.utils.UUIDs;

import java.io.UnsupportedEncodingException;
import java.util.Random;
import java.util.UUID;

/**
 * Описание
 *
 * @author Sergey.Titkov
 * @version 001.00
 * @since 001.00
 */
public class GenerateTimeUUD {

  private static long makeClockSeqAndNode() {
    long clock = new Random(System.currentTimeMillis()).nextLong();
    long node = makeNode();

    long lsb = 0;
    lsb |= (clock & 0x0000000000003FFFL) << 48;
    lsb |= 0x8000000000000000L;
    lsb |= node;
    return lsb;
  }

  // Package visible for testing
  static long makeMSB(long timestamp) {
    long msb = 0L;
    msb |= (0x00000000ffffffffL & timestamp) << 32;
    msb |= (0x0000ffff00000000L & timestamp) >>> 16;
    msb |= (0x0fff000000000000L & timestamp) >>> 48;
    msb |= 0x0000000000001000L; // sets the version to 1.
    return msb;
  }

  public static void main(String[] args) {
    try {
      new ConsoleCodingSettings();
    } catch (UnsupportedEncodingException e) {
      return;
    }
    UUID uuid = UUIDs.timeBased();
  }
}
