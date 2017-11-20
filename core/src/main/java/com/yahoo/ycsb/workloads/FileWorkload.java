/**
 * Copyright (c) 2010 Yahoo! Inc., Copyright (c) 2016 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.DiscreteGenerator;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * For mongo read by key from file .
 * <p>
 * Properties to control the client:
 * <UL>
 * </ul>
 */
public class FileWorkload extends CoreWorkload {

  /**
   * The name of the database table to run queries against.
   */
  public static final String TABLENAME_PROPERTY = "table";

  /**
   * The default name of the database table to run queries against.
   */
  public static final String TABLENAME_PROPERTY_DEFAULT = "usertable";

  public static final String WRITE_TABLENAME_PROPERTY = "writetable";
  public static final String WRITE_TABLENAME_PROPERTY_DEFAULT = "usertable_for_write";

  public static final String WRITE_RATE = "writerate";
  public static final String WRITE_RATE_DEFAULT = "0";

  protected String table;
  protected static String writetable;


  /**
   *  The path of data file to load.
   */
  private static final String DATA_FILE = "datafile";
  /**
   *  The default path of data file to load.
   */
  private static final String DATA_FILE_DEFAULT = "/data/publicdata/wikipedia/wikipedia.txt";

  private static final String KEY_FILE = "keyfile";
  private static final String KEY_FILE_DEFAULT = "/data/publicdata/wikipedia/wikipedia_key.txt";

  public static final String DATA_FILE_DELIMITER = "delimiter";
  public static final String DATA_FILE_DELIMIITER_DEFAULT = "\t";

  /**
   *  The size of data queue.
   */
  public static final String QUEUE_SIZE = "queuesize";
  /**
   *  The default size of data queue.
   */
  public static final String QUEUE_SIZE_DEFAULT = "20000";

  public static final String BATCH_READ = "batchread";
  public static final String BATCH_READ_DEFAULT = "1";

  public static final String FIELD_NUM = "fieldnum";
  public static final String FIELD_NUM_DEFAULT = "15";

  /**
   *  A String of the fieldnames split by.
   */
  private static final String FIELD_NAMES = "fieldnames";
  /**
   *  Default value of fieldnames.
   */
  private static final String FIELD_NAMES_DEFAULTS = null;
  private static final String KEY_FIELD = "keyfield";
  private static final String KEY_FIELD_DEFAULTS = "0";


  public static final String USE_CUSTOM_KEY = "usecustomkey";
  public static final String USE_CUSTOM_KEY_DEFAULT = "false";

  public static final String THREAD_PLAN = "threadplan";
  public static final String THREAD_PLAN_DEFAULT = "0-0:1,0";

  public static final String WRITE_IN_READ = "writeinread";
  public static final String WRITE_IN_READ_DEFAULT = "true";

  /**
   * The name of the property for the proportion of transactions that are reads.
   */
  public static final String READ_PROPORTION_PROPERTY = "readproportion";

  /**
   * The default proportion of transactions that are reads.
   */
  public static final String READ_PROPORTION_PROPERTY_DEFAULT = "0.95";

  /**
   * The name of the property for the proportion of transactions that are inserts.
   */
  public static final String INSERT_PROPORTION_PROPERTY = "insertproportion";

  /**
   * The default proportion of transactions that are inserts.
   */
  public static final String INSERT_PROPORTION_PROPERTY_DEFAULT = "0.05";

  protected DiscreteGenerator operationchooser;

  /**
   *  Data queue store lines to insert.
   */
  private static LinkedBlockingQueue<String> dataQueue = null;
  private static LinkedBlockingQueue<String> keyQueue = null;
  private static String datafile;
  public static String getdatafile() {
    return datafile;
  }

  private static String keyfile;
  public static String getkeyfile() {
    return keyfile;
  }

  private static int batchread;
  public static int getBatchread() {
    return batchread;
  }

  private static double writerate;

  private static int fieldnum;
  public static int getFieldnum() {
    return fieldnum;
  }
  protected List<String> fieldnames;
  protected HashSet<String> fieldnamesset;
  private ArrayList<Boolean> keyfieldsbits;

  private static String delimiter;
  public static String getDelimiter() {
    return delimiter;
  }
  private static volatile boolean dataFileEof = false;
  private static volatile boolean keyFileEof = false;
  private static volatile boolean isStop = false;
  public static boolean getIsStop() {
    return isStop;
  }

  protected boolean usecustomkey;
  private static boolean dotransactions = true;
  public static boolean getDotransactions() {
    return dotransactions;
  }

  private boolean writeinread;
  public boolean getWriteinread() {
    return writeinread;
  }

  private static ExecutorService producer = Executors.newFixedThreadPool(1);


  /**
   * Initialize the scenario.
   * Called once, in the main client thread, before any operations are started.
   */
  @Override
  public void init(Properties p) throws WorkloadException {
    super.init(p);
    table = p.getProperty(TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);
    writetable = p.getProperty(WRITE_TABLENAME_PROPERTY, WRITE_TABLENAME_PROPERTY_DEFAULT);

    final int queuesize = Integer.parseInt(p.getProperty(QUEUE_SIZE, QUEUE_SIZE_DEFAULT));

    usecustomkey = Boolean.valueOf(p.getProperty(USE_CUSTOM_KEY, USE_CUSTOM_KEY_DEFAULT));
    System.err.println("usecustomkey: " + usecustomkey);
    String fieldNamesStr = p.getProperty(FIELD_NAMES, FIELD_NAMES_DEFAULTS);
    if (fieldNamesStr != null) {
      fieldnames = Arrays.asList(fieldNamesStr.split(","));
    }

    delimiter = p.getProperty(DATA_FILE_DELIMITER, DATA_FILE_DELIMIITER_DEFAULT);
    fieldnum = Integer.valueOf(p.getProperty(FIELD_NUM, FIELD_NUM_DEFAULT));
    keyfieldsbits = new ArrayList<>(Collections.nCopies(fieldnum, false));
    List<String> keyfieldsStr = Arrays.asList(p.getProperty(KEY_FIELD, KEY_FIELD_DEFAULTS).split(","));
    for (String fieldStr : keyfieldsStr) {
      Integer keyindex = Integer.valueOf(fieldStr);
      keyfieldsbits.set(keyindex, true);
    }
    fieldnamesset = new HashSet<>();
    for (int i = 0; i < fieldnames.size(); ++i) {
      if (!keyfieldsbits.get(i)) {
        fieldnamesset.add(fieldnames.get(i));
      }
    }

    writeinread = Boolean.valueOf(p.getProperty(WRITE_IN_READ, WRITE_IN_READ_DEFAULT));

    dotransactions = Boolean.valueOf(p.getProperty(Client.DO_TRANSACTIONS_PROPERTY, String.valueOf(true)));
    keyQueue = new LinkedBlockingQueue<>();

    if((!getDotransactions()) || (!getWriteinread())) {   // insert
      datafile = p.getProperty(DATA_FILE, DATA_FILE_DEFAULT);
      dataQueue = new LinkedBlockingQueue<>();
      operationchooser = createOperationGenerator(p);

      // 单线程不断的从文件收集key
      producer.execute(new Runnable() {
        @Override
        public void run() {
          try {
            BufferedReader reader = new BufferedReader(new FileReader(getdatafile()));
            while (!getIsStop()) {
              if (dataQueue.size() < queuesize) {
                String line = reader.readLine();
                if (line == null) {
                  dataFileEof = true;
                  break;
                }
                dataQueue.add(line);
              }
            }
            reader.close();
          } catch (Exception e) {
            throw new RuntimeException("DataFile read error : " + e.getCause().getMessage());
          } finally {
            dataFileEof = true;
          }
        }
      });
    } else {
      batchread = Integer.parseInt(p.getProperty(BATCH_READ, BATCH_READ_DEFAULT));
      writerate = Double.parseDouble(p.getProperty(WRITE_RATE, WRITE_RATE_DEFAULT));

      if (!getWriteinread()) {
        keyfile = p.getProperty(KEY_FILE, KEY_FILE_DEFAULT);
        producer.execute(new Runnable() {
          @Override
          public void run() {
            try {
              BufferedReader reader = new BufferedReader(new FileReader(getkeyfile()));
              while (!getIsStop()) {
                if (keyQueue.size() < queuesize) {
                  String line = reader.readLine();
                  if (line == null) {
                    keyFileEof = true;
                    break;
                  }
                  keyQueue.add(line);
                }
              }
              reader.close();
            } catch (Exception e) {
              throw new RuntimeException(("KeyFile read error : " + e.getCause().getMessage()));
            } finally {
              keyFileEof = true;
            }
          }
        });
      }
    }
  }

  private HashMap<String, ByteIterator> buildValues(String[] fields) {
    if (fields.length < fieldnum) {
      throw new RuntimeException("Data fields length error");
    }
    HashMap<String, ByteIterator> values = new HashMap<>();
    if (fieldnames == null || fieldnames.isEmpty()) {
      int valueCnt = 0;
      for (int i = 0; i < fieldnum; ++i) {
        if (!keyfieldsbits.get(i)) {
          values.put("field" + valueCnt++, new StringByteIterator(fields[i]));
        }
      }
    } else {
      for (int i = 0; i < fieldnum; ++i) {
        if (!keyfieldsbits.get(i)) {
          values.put(fieldnames.get(i), new StringByteIterator(fields[i]));
        }
      }
    }
    return values;
  }

  private String buildKeys(String[] fields) {
    if (fields.length < fieldnum) {
      throw new RuntimeException("Data fields length error");
    }
    String key;
    if (usecustomkey) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < fieldnum; ++i) {
        if (keyfieldsbits.get(i)) {
          sb.append(fields[i]);
          sb.append(getDelimiter());
        }
      }
      int delimiterLength = getDelimiter().length();
      for (int len = 1; sb.length() > 0 && len <= delimiterLength; ++len) {
        sb.deleteCharAt(sb.length() - len);
      }
      key = sb.toString();
    } else {
      int keynum = keysequence.nextValue().intValue();
      key = buildKeyName(keynum);
    }
    return key;
  }

  @Override
  public boolean doInsert(DB db, Object threadstate) {
    if (dataFileEof && dataQueue.isEmpty()) {
      return false;
    }
    HashMap<String, ByteIterator> values;
    String line, dbkey;
    try {
      line = dataQueue.take();
    } catch (Exception e) {
      throw new RuntimeException("Data convert error: " + e.getCause().getMessage());
    }
    String[] strings = line.split(getDelimiter());
    if (strings.length < fieldnum) {
      System.err.println("There too little fields in the data: " + line);
      return true;
    }
    values = buildValues(strings);
    dbkey = buildKeys(strings);
    Status status;
    int numOfRetries = 0;
    do {
      status = db.insert(table, dbkey, values);
      if (null != status && status.isOk()) {
        break;
      }
      // Retry if configured. Without retrying, the load process will fail
      // even if one single insertion fails. User can optionally configure
      // an insertion retry limit (default is 0) to enable retry.
      if (++numOfRetries <= insertionRetryLimit) {
        System.err.println("Retrying insertion, retry count: " + numOfRetries);
        try {
          // Sleep for a random number between [0.8, 1.2)*insertionRetryInterval.
          int sleepTime = (int) (1000 * insertionRetryInterval * (0.8 + 0.4 * Math.random()));
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          break;
        }
      } else {
        System.err.println("Error inserting, not retrying any more. number of attempts: " + numOfRetries +
                "Insertion Retry Limit: " + insertionRetryLimit);
        break;
      }
    } while (true);

    return null != status && status.isOk();
  }

  /**
   * Builds keys from String or keysequence.
   */
  private String buildKeysInRead() {
    String key;
    if (usecustomkey) {
      try {
        key = keyQueue.take();
      } catch (Exception e) {
        throw new RuntimeException("Key convert error: " + e.getCause().getMessage());
      }
    } else {
      int keynum = nextKeynum();
      key = buildKeyName(keynum);
    }
    return key;
  }


  /**
   * Do one transaction operation. Because it will be called concurrently from multiple client
   * threads, this function must be thread safe. However, avoid synchronized, or the threads will block waiting
   * for each other, and it will be difficult to reach the target throughput. Ideally, this function would
   * have no side effects other than DB operations.
   */
  @Override
  public boolean doTransaction(DB db, Object threadstate) {
    if (getWriteinread()) {
      return doRead(db);
    } else {
      String operation = operationchooser.nextString();
      if (operation == null) {
        return false;
      }
      switch (operation) {
      case "READ":
        return doRead(db);
      case "INSERT":
        return doWrite(db);
      default:
        return doRead(db);
      }
    }
  }


  public boolean doRead(DB db) {
    if (keyFileEof && keyQueue.isEmpty()) {
      return false;
    }
    //HashSet<String> fields = null;
    if (getBatchread() == 1) {
      String keyname = buildKeysInRead();
      HashMap<String, ByteIterator> cells = new HashMap<>();
      Status status = db.read(table, keyname, fieldnamesset, cells);

      double rand = Math.random();
      if(rand < writerate && status.isOk() && getWriteinread()) {
        db.insert(writetable, keyname, cells);
      }
    } else {
      ArrayList<String> keynames = new ArrayList<>();
      for (int i = 0; i < getBatchread(); ++i) {
        String keyname = buildKeysInRead();
        keynames.add(keyname);
      }
      Vector<HashMap<String, ByteIterator>> cells = new Vector<>(keynames.size());
      for (int i = 0; i < keynames.size(); ++i) {
        cells.add(new HashMap<String, ByteIterator>());
      }
      ArrayList<Status> results = db.batchRead(table, keynames, fieldnamesset, cells);
      if (writerate > 0 && getWriteinread()) {
        for (int i = 0; i < results.size(); ++i) {
          if (results.get(i).isOk()) {
            double rand = Math.random();
            if (rand < writerate) {
              db.insert(writetable, keynames.get(i), cells.get(i));
            }
          }
        }
      }
    }
    return true;
  }

  public boolean doWrite(DB db) {
    if (dataFileEof && dataQueue.isEmpty()) {
      return false;
    }

    if (getWriteinread()) {
      throw new RuntimeException("Write setting error");
    }

    HashMap<String, ByteIterator> values;
    String line, dbkey;
    try {
      line = dataQueue.take();

    } catch (Exception e) {
      throw new RuntimeException("Data convert error: " + e.getCause().getMessage());
    }
    String[] strings = line.split(getDelimiter());
    values = buildValues(strings);
    dbkey = buildKeys(strings);
    Status status;
    int numOfRetries = 0;
    do {
      if (dbkey == null) {
        throw new RuntimeException("dbkey is empty");
      }
      status = db.insert(writetable, dbkey, values);
      if (null != status && status.isOk()) {
        break;
      }
      // Retry if configured. Without retrying, the load process will fail
      // even if one single insertion fails. User can optionally configure
      // an insertion retry limit (default is 0) to enable retry.
      if (++numOfRetries <= insertionRetryLimit) {
        System.err.println("Retrying write, retry count: " + numOfRetries);
        try {
          // Sleep for a random number between [0.8, 1.2)*insertionRetryInterval.
          int sleepTime = (int) (1000 * insertionRetryInterval * (0.8 + 0.4 * Math.random()));
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          break;
        }
      } else {
        System.err.println("Error writing, not retrying any more. number of attempts: " + numOfRetries +
                "Write Retry Limit: " + insertionRetryLimit);
        break;
      }
    } while (true);

    return null != status && status.isOk();
  }

  @Override
  public void cleanup() throws WorkloadException {
    isStop = true;
    if (getDotransactions()) {
      keyQueue.clear();
    } else {
      dataQueue.clear();
    }
  }


  /**
   * Creates a weighted discrete values with database operations for a workload to perform.
   * Weights/proportions are read from the properties list and defaults are used
   * when values are not configured.
   * Current operations are "READ", "UPDATE", "INSERT", "SCAN" and "READMODIFYWRITE".
   *
   * @param p The properties list to pull weights from.
   * @return A generator that can be used to determine the next operation to perform.
   * @throws IllegalArgumentException if the properties object was null.
   */
  protected static DiscreteGenerator createOperationGenerator(final Properties p) {
    if (p == null) {
      throw new IllegalArgumentException("Properties object cannot be null");
    }
    final double readproportion = Double.parseDouble(
            p.getProperty(READ_PROPORTION_PROPERTY, READ_PROPORTION_PROPERTY_DEFAULT));
    final double insertproportion = Double.parseDouble(
            p.getProperty(INSERT_PROPORTION_PROPERTY, INSERT_PROPORTION_PROPERTY_DEFAULT));

    final DiscreteGenerator operationchooser = new DiscreteGenerator();
    if (readproportion > 0) {
      operationchooser.addValue(readproportion, "READ");
    }

    if (insertproportion > 0) {
      operationchooser.addValue(insertproportion, "INSERT");
    }
    return operationchooser;
  }
}
