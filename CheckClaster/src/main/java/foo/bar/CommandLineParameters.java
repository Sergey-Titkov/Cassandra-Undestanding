package foo.bar;

import com.beust.jcommander.Parameter;

/**
 * Параметры командной строки.
 *
 * @author Sergey.Titkov
 * @version 001.00
 * @since 001.00
 */
public class CommandLineParameters {

  @Parameter(names = {"--host"},
             description = "Адресс кластера кассандры",
             required = true)
  public String host = "127.0.0.1";

  @Parameter(names = {"--numberOfThread"},
             description = "Колличетсво нитей обновления. MAX 128")
  public Integer numberOfThread = 10;

  @Parameter(names = {"--time"},
             description = "Сколько времени обновлять клиента, в секундах. MAX 86400")
  public Integer time = 5;


  @Parameter(names = {"-?", "-h", "--help"}, description = "Справка о программе"
  ,help=true)
  public boolean help = false;


}
