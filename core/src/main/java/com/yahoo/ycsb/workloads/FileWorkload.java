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

  private static double writerate;

  /**
   *  The path of data file to load.
   */
  private static final String DATA_FILE = "datafile";

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

  /**
   *  The default path of data file to load.
   */
  private static final String DATA_FILE_DEFAULT = "/data/publicdata/wikipedia/wikipedia.txt";

  /**
   *  A String of the fieldnames split by.
   */
  private static final String FIELD_NAMES = "fieldnames";
  /**
   *  Default value of fieldnames.
   */
  private static final String FIELD_NAMES_DEFAULTS = "field0,field1,field2,field3,field4";
  protected int recordcount;

  /**
   *  Data queue store lines to insert.
   */
  private static LinkedBlockingQueue<String> dataQueue = null;
  private static String dataFile;
  private static int batchread;
  public static int getBatchread() {
    return batchread;
  }
  public static String getDataFile() {
    return dataFile;
  }
  private static String delimiter;
  public static String getDelimiter() {
    return delimiter;
  }
  private static String fieldNamesStr;
  public String getFieldNamesStr() {
    return fieldNamesStr;
  }
  private static ExecutorService producer = Executors.newFixedThreadPool(1);
  private static volatile boolean dataFileEof = false;
  private static volatile boolean isStop = false;
  public static boolean getIsStop() {
    return isStop;
  }

  protected List<String> fieldnames;

  /**
   * Initialize the scenario.
   * Called once, in the main client thread, before any operations are started.
   */
  @Override
  public void init(Properties p) throws WorkloadException {
    super.init(p);
    table = p.getProperty(TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);
    writetable = p.getProperty(WRITE_TABLENAME_PROPERTY, WRITE_TABLENAME_PROPERTY_DEFAULT);
    writerate = Double.parseDouble(p.getProperty(WRITE_RATE, WRITE_RATE_DEFAULT));
    dataFile = p.getProperty(DATA_FILE, DATA_FILE_DEFAULT);
    delimiter = p.getProperty(DATA_FILE_DELIMITER, DATA_FILE_DELIMIITER_DEFAULT);
    fieldNamesStr = p.getProperty(FIELD_NAMES, FIELD_NAMES_DEFAULTS);

    final int queuesize = Integer.parseInt(p.getProperty(QUEUE_SIZE, QUEUE_SIZE_DEFAULT));
    dataQueue = new LinkedBlockingQueue<>();
    batchread = Integer.parseInt(p.getProperty(BATCH_READ, BATCH_READ_DEFAULT));

    String filedNamesStr = getFieldNamesStr();
    fieldnames = Arrays.asList(filedNamesStr.split(","));

    boolean dotransactions = Boolean.valueOf(p.getProperty(Client.DO_TRANSACTIONS_PROPERTY, String.valueOf(true)));

    if(!dotransactions) {
      // 单线程不断的从文件收集key
      producer.execute(new Runnable() {
        @Override
        public void run() {
          try {
            BufferedReader reader = new BufferedReader(new FileReader(getDataFile()));
            HashMap<String, ByteIterator> data;
            while(!getIsStop()) {
              if (dataQueue.size() < queuesize) {
                String line = reader.readLine();
                if(line == null) {
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
    }
  }

  @Override
  public boolean doInsert(DB db, Object threadstate) {
    if (dataFileEof && dataQueue.isEmpty()) {
      return false;
    }
    HashMap<String, ByteIterator> values = new HashMap<>();
    String line;
    try {
      line = dataQueue.take();
      String[] strings = line.split(getDelimiter());
      for(int i = 0; i < fieldnames.size(); ++i) {
        values.put(fieldnames.get(i), new StringByteIterator(strings[i]));
      }
    } catch (Exception e) {
      throw new RuntimeException("Data convert error : " + e.getCause().getMessage());
    }
    int keynum = keysequence.nextValue().intValue();
    String dbkey = buildKeyName(keynum);

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
   * Do one transaction operation. Because it will be called concurrently from multiple client
   * threads, this function must be thread safe. However, avoid synchronized, or the threads will block waiting
   * for each other, and it will be difficult to reach the target throughput. Ideally, this function would
   * have no side effects other than DB operations.
   */
  @Override
  public boolean doTransaction(DB db, Object threadstate) {
    doTransactionRead(db);
    return true;
  }

  public void doTransactionRead(DB db) {
    HashSet<String> fields = null;
    if (getBatchread() == 1) {
      int keynum = nextKeynum();
      String keyname = buildKeyName(keynum);
      HashMap<String, ByteIterator> cells = new HashMap<>();
      Status status = db.read(table, keyname, fields, cells);

      double rand = Math.random();
      if(rand < writerate && status.isOk()) {
        db.insert(writetable, keyname, cells);
      }
    } else {
      ArrayList<String> keynames = new ArrayList<>();
      for (int i = 0; i < getBatchread(); ++i) {
        int keynum = nextKeynum();
        String keyname = buildKeyName(keynum);
        keynames.add(keyname);
      }
      Vector<HashMap<String, ByteIterator>> cells = new Vector<>(keynames.size());
      for (int i = 0; i < keynames.size(); ++i) {
        cells.add(new HashMap<String, ByteIterator>());
      }
      ArrayList<Status> results = db.batchRead(table, keynames, fields, cells);
      if (writerate > 0) {
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
    return;
  }

  @Override
  public void cleanup() throws WorkloadException {
    isStop = true;
    dataQueue.clear();
  }
}
