Изучение NoSQL базы Кассандра.
=========

В этом репозитарии находятся примеры изучения как следует использовать Кассандру .

Для удобства, проект разделен на модули. Каждый модуль демонстрирует метод решения конкретной задачи.

CheckClaster
----
#####Базовая проверка кластера кассандры на работоспособность. 
После развертывания кластера необходимо убедитсья, что он работоспособен и все ноды кластера работают корректно.
На кластер можно подать нагрузку с помощью штатной утилиты от DataStax и произвести мониторинг. А можно подстраховаться и воспользоваться этой утилитой :)
Идея утилиты следующая, производиться запись информации, после чего производится чтение информации и ее сверка. Для этого делается следующее:
- Создаем пространство ключей с репликацией на все ноды кластера.
- Создаем таблицу c строковым ключом main_id(рандомное значение), временем вставки и неким значением.
- Многопоточно пишем данные в эту таблицу. Консистенотность записи ONE. Паралельно многопоточно читаем данные. Иметируем нагрузгу. После чего производим чтение данных и проверям скольок строк было записано и на какая сумма значения была записана. Полученные значения сверям с эталонными. Консистентность чтения ALL. Если при работе модуля возникло больше количество ошибок или же он вообще не смог отработать, то следует заняться настройкой кластера.

#####Параметры командной строки
```
--host Адресс кластера кассандры, без порта.
--numberOfThread Колличетсво нитей чтения/записи. MAX 128 
             description = "Колличетсво нитей обновления. Максимум 128, по умолчанию 10.
--time Сколько времени записывать/читать данные, в секундах. Максимум 86400.
-?, -h, Справка о программе
```
#####Параметры запуска VM
```
-DconsoleEncoding=Кодировака, по умолчанию используется CP866
```

```
mvn clean:package
```


```
java.exe -DconsoleEncoding=CP866 -jar CheckClaster.one-jar.jar --host 127.0.0.1 --time 5 --numberOfThread 2
Запускаем писателей.
Запускаем читателей.
Строковый ключ: 643419
Писатели:
UUID                                    Сумма Vol                       Всего таймаутов                 Не удалось обновить счетчики    Вставлено записей               Вставок в секунду
------------------------------------    ----------------------------    ----------------------------    ----------------------------    ----------------------------    ----------------------------
32e7d39f-e425-4f23-8a92-da33db76bcbe    57611                           0                               0                               1156                            230
66c27f00-f0e2-446e-a54e-8cfb2daa3541    58745                           0                               0                               1177                            234
------------------------------------    ----------------------------    ----------------------------    ----------------------------    ----------------------------    ----------------------------
                                        116356                          0                               0                               2333                            464
------------------------------------    ----------------------------    ----------------------------    ----------------------------    ----------------------------    ----------------------------
Читатели:
UUID                                    -//-                            Всего таймаутов                 Не удалось прочитать счетчики   Считано записей                 Чтений в секунду
------------------------------------    ----------------------------    ----------------------------    ----------------------------    ----------------------------    ----------------------------
ef22d56a-fe01-411e-87de-29232c9fd52d    115433                          3280                            0                               277923                          55253
439df722-acf3-49cf-b485-a0807bf7f1f7    115983                          3380                            0                               288711                          57329
------------------------------------    ----------------------------    ----------------------------    ----------------------------    ----------------------------    ----------------------------
                                                                        6660                            0                               566634                          112582
------------------------------------    ----------------------------    ----------------------------    ----------------------------    ----------------------------    ----------------------------
Контрольное чтение.
UUID                                    Сумма Vol                       Разница с эталон                Считано записей                 Разница с эталон
------------------------------------    ----------------------------    ----------------------------    ----------------------------    ----------------------------
d12d6d0d-3c8e-4824-95fe-34614ac607d5    116356                          0                               2333                            0
f995d929-b17d-4cb6-b47a-bb7a12c91089    116356                          0                               2333                            0
------------------------------------    ----------------------------    ----------------------------    ----------------------------    ----------------------------

Скорость записи: 464 значений в сек.
Таймаутов при записи в секунду: 0

Скорость чтения: 112582 значений в сек
Таймаутов при чтении в секунду: 1323
```
И проверяем данные, если идут постоянно ошибки time, настраиваем тайм ауты или приводим в чувство кластер.

```java
"C:\Program Files\Java\jdk1.7.0_25\bin\java" -DconsoleEncoding=UTF-8 -Didea.launcher.port=7535 "-Didea.launcher.bin.path=C:\Program Files (x86)\JetBrains\IntelliJ IDEA 12.1.6\bin" -Dfile.encoding=UTF-8 -classpath "C:\Program Files\Java\jdk1.7.0_25\jre\lib\charsets.jar;C:\Program Files\Java\jdk1.7.0_25\jre\lib\deploy.jar;C:\Program Files\Java\jdk1.7.0_25\jre\lib\javaws.jar;C:\Program Files\Java\jdk1.7.0_25\jre\lib\jce.jar;C:\Program Files\Java\jdk1.7.0_25\jre\lib\jfr.jar;C:\Program Files\Java\jdk1.7.0_25\jre\lib\jfxrt.jar;C:\Program Files\Java\jdk1.7.0_25\jre\lib\jsse.jar;C:\Program Files\Java\jdk1.7.0_25\jre\lib\management-agent.jar;C:\Program Files\Java\jdk1.7.0_25\jre\lib\plugin.jar;C:\Program Files\Java\jdk1.7.0_25\jre\lib\resources.jar;C:\Program Files\Java\jdk1.7.0_25\jre\lib\rt.jar;C:\Program Files\Java\jdk1.7.0_25\jre\lib\ext\access-bridge-64.jar;C:\Program Files\Java\jdk1.7.0_25\jre\lib\ext\dnsns.jar;C:\Program Files\Java\jdk1.7.0_25\jre\lib\ext\jaccess.jar;C:\Program Files\Java\jdk1.7.0_25\jre\lib\ext\localedata.jar;C:\Program Files\Java\jdk1.7.0_25\jre\lib\ext\sunec.jar;C:\Program Files\Java\jdk1.7.0_25\jre\lib\ext\sunjce_provider.jar;C:\Program Files\Java\jdk1.7.0_25\jre\lib\ext\sunmscapi.jar;C:\Program Files\Java\jdk1.7.0_25\jre\lib\ext\zipfs.jar;C:\GIT_REPO\Cassandra-Undestanding\CheckClaster\target\classes;C:\Users\Sergey.Titkov\.m2\repository\com\beust\jcommander\1.30\jcommander-1.30.jar;C:\Users\Sergey.Titkov\.m2\repository\com\datastax\cassandra\cassandra-driver-core\2.0.4\cassandra-driver-core-2.0.4.jar;C:\Users\Sergey.Titkov\.m2\repository\io\netty\netty\3.9.0.Final\netty-3.9.0.Final.jar;C:\Users\Sergey.Titkov\.m2\repository\com\google\guava\guava\16.0.1\guava-16.0.1.jar;C:\Users\Sergey.Titkov\.m2\repository\com\codahale\metrics\metrics-core\3.0.2\metrics-core-3.0.2.jar;C:\Users\Sergey.Titkov\.m2\repository\org\slf4j\slf4j-api\1.6.6\slf4j-api-1.6.6.jar;C:\Users\Sergey.Titkov\.m2\repository\org\slf4j\jcl-over-slf4j\1.6.6\jcl-over-slf4j-1.6.6.jar;C:\Users\Sergey.Titkov\.m2\repository\ch\qos\logback\logback-classic\1.0.9\logback-classic-1.0.9.jar;C:\Users\Sergey.Titkov\.m2\repository\ch\qos\logback\logback-core\1.0.9\logback-core-1.0.9.jar;C:\Program Files (x86)\JetBrains\IntelliJ IDEA 12.1.6\lib\idea_rt.jar" com.intellij.rt.execution.application.AppMain foo.bar.CheckClaster --host 172.30.16.239 --time 5 --numberOfThread 2
Запускаем писателей.
Запускаем читателей.
Строковый ключ: 367636
Писатели:
UUID                                	Сумма Vol                   	Всего таймаутов             	Не удалось обновить счетчики	Вставлено записей           	Вставок в секунду           
------------------------------------	----------------------------	----------------------------	----------------------------	----------------------------	----------------------------	
1da3a548-64e6-4370-9bfa-b4fa04933b57	61234                       	0                           	0                           	1243                        	248                         
0c60ab2f-1c8f-4ac7-961b-4769f6805f29	61147                       	0                           	0                           	1238                        	247                         
------------------------------------	----------------------------	----------------------------	----------------------------	----------------------------	----------------------------	
                                    	122381                      	0                           	0                           	2481                        	495                         
------------------------------------	----------------------------	----------------------------	----------------------------	----------------------------	----------------------------	
Читатели:
UUID                                	-//-                        	Всего таймаутов             	Не удалось прочитать счетчики	Считано записей             	Чтений в секунду            
------------------------------------	----------------------------	----------------------------	----------------------------	----------------------------	----------------------------	
fcc8853d-1cac-4760-8747-460bf600fe02	122139                      	3000                        	0                           	285647                      	56981                       
e1e3452c-e676-4383-b91e-df7d89c8e74f	122291                      	2990                        	0                           	294690                      	58528                       
------------------------------------	----------------------------	----------------------------	----------------------------	----------------------------	----------------------------	
                                    	                            	5990                        	0                           	580337                      	115509                      
------------------------------------	----------------------------	----------------------------	----------------------------	----------------------------	----------------------------	
Контрольное чтение.
UUID                                	Сумма Vol                   	Разница с эталон            	Считано записей             	Разница с эталон            
------------------------------------	----------------------------	----------------------------	----------------------------	----------------------------	
d0668469-88d2-48ef-8b2c-7a7b47494111	122381                      	0                           	2481                        	0                           
56db6458-8b22-40e7-8662-e185172d9470	122381                      	0                           	2481                        	0                           
------------------------------------	----------------------------	----------------------------	----------------------------	----------------------------	

Скорость записи: 495 значений в сек.
Таймаутов при записи в секунду: 0

Скорость чтения: 115509 значений в сек
Таймаутов при чтении в секунду: 1192
CREATE TABLE check_cluster.test_table (
  main_id bigint,
  insert_time timeuuid,
  vol_01 bigint,
  PRIMARY KEY (main_id, insert_time)
) WITH CLUSTERING ORDER BY (insert_time DESC) AND
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.100000 AND
  gc_grace_seconds=864000 AND
  read_repair_chance=0.000000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  compaction={'class': 'SizeTieredCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};
```
  
Dillinger is a cloud-enabled HTML5 Markdown editor.

  - Type some Markdown text in the left window
  - See the HTML in the right
  - Magic

Markdown is a lightweight markup language based on the formatting conventions that people naturally use in email.  As [John Gruber] writes on the [Markdown site] [1]:

> The overriding design goal for Markdown's
> formatting syntax is to make it as readable 
> as possible. The idea is that a
> Markdown-formatted document should be
> publishable as-is, as plain text, without
> looking like it's been marked up with tags
> or formatting instructions.

This text you see here is *actually* written in Markdown! To get a feel for Markdown's syntax, type some text into the left window and watch the results in the right.  

Version
----

2.0

Tech
-----------

Dillinger uses a number of open source projects to work properly:

* [Ace Editor] - awesome web-based text editor
* [Marked] - a super fast port of Markdown to JavaScript
* [Twitter Bootstrap] - great UI boilerplate for modern web apps
* [node.js] - evented I/O for the backend
* [Express] - fast node.js network app framework [@tjholowaychuk]
* [keymaster.js] - awesome keyboard handler lib by [@thomasfuchs]
* [jQuery] - duh 

Installation
--------------

```sh
git clone [git-repo-url] dillinger
cd dillinger
npm i -d
mkdir -p public/files/{md,html,pdf}
```

##### Configure Plugins. Instructions in following README.md files

* plugins/dropbox/README.md
* plugins/github/README.md
* plugins/googledrive/README.md

```sh
node app
```


License
----

MIT


**Free Software, Hell Yeah!**

[john gruber]:http://daringfireball.net/
[@thomasfuchs]:http://twitter.com/thomasfuchs
[1]:http://daringfireball.net/projects/markdown/
[marked]:https://github.com/chjj/marked
[Ace Editor]:http://ace.ajax.org
[node.js]:http://nodejs.org
[Twitter Bootstrap]:http://twitter.github.com/bootstrap/
[keymaster.js]:https://github.com/madrobby/keymaster
[jQuery]:http://jquery.com
[@tjholowaychuk]:http://twitter.com/tjholowaychuk
[express]:http://expressjs.com
