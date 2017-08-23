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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Vector;
import java.util.ArrayList;
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
public class MongoReadByKeyFromFileWorkload extends Workload {

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

  protected static String table;
  protected static String writetable;

  public static final String KEY_FILE = "keyfile";

  public static final String QUEUE_SIZE = "queuesize";
  public static final String QUEUE_SIZE_DEFAULT = "20000";

  public static final String BATCH_READ = "batchread";
  public static final String BATCH_READ_DEFAULT = "1";

  public static final String WRITE_RATE = "writerate";
  public static final String WRITE_RATE_DEFAULT = "0";


  private static LinkedBlockingQueue<ArrayList<String>> keyQueue = null;
  private static String keyfile;
  private static int batchread;
  private static int getBatchread() {
    return batchread;
  }
  private static double writerate;

  public static LinkedBlockingQueue<ArrayList<String>> getKeyQueue() {
    return keyQueue;
  }
  public static String getKeyfile() {
    return keyfile;
  }

  private static ExecutorService producer = Executors.newFixedThreadPool(1);
  private static volatile boolean keyFileEof = false;
  private static volatile boolean isStop = false;
  public static boolean getIsStop() {
    return isStop;
  }

  /**
   * Initialize the scenario.
   * Called once, in the main client thread, before any operations are started.
   */
  @Override
  public void init(Properties p) throws WorkloadException {
    table = p.getProperty(TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);
    writetable = p.getProperty(WRITE_TABLENAME_PROPERTY, WRITE_TABLENAME_PROPERTY_DEFAULT);

    keyfile = p.getProperty(KEY_FILE, KEY_FILE);

    final int queuesize = Integer.parseInt(p.getProperty(QUEUE_SIZE, QUEUE_SIZE_DEFAULT));
    keyQueue = new LinkedBlockingQueue<>();
    batchread = Integer.parseInt(p.getProperty(BATCH_READ, BATCH_READ_DEFAULT));
    writerate = Double.parseDouble(p.getProperty(WRITE_RATE, WRITE_RATE_DEFAULT));


    // 单线程不断的从文件收集key
    producer.execute(new Runnable() {
      @Override
      public void run() {
        try {
          BufferedReader reader = new BufferedReader(new FileReader(getKeyfile()));
          ArrayList<String> strArr;
          int batch = getBatchread();
          while (!getIsStop()) {
            if(keyQueue.size() < queuesize){
              strArr = new ArrayList<String>(batch);
              for (int i = 0; i < batch; ++i) {
                String line = reader.readLine();
                if (line == null) {
                  if (i > 0) {
                    keyQueue.add(strArr);
                  }
                  throw new Exception("eof");
                }
                strArr.add(line);
              }
              keyQueue.add(strArr);
            }
          }
          reader.close();
        } catch (Exception e) {
          throw new RuntimeException("Keyfile 读取出错 : " + e.getCause().getMessage());
        } finally {
          keyFileEof = true;
        }
      }
    });

  }

  @Override
  public boolean doInsert(DB db, Object threadstate) {
    return false;
  }

  /**
   * Do one transaction operation. Because it will be called concurrently from multiple client
   * threads, this function must be thread safe. However, avoid synchronized, or the threads will block waiting
   * for each other, and it will be difficult to reach the target throughput. Ideally, this function would
   * have no side effects other than DB operations.
   */
  @Override
  public boolean doTransaction(DB db, Object threadstate) {
    return doTransactionRead(db);
  }


  public boolean doTransactionRead(DB db) {
    ArrayList<String> keyVec;
    if (keyFileEof && keyQueue.isEmpty()) {
      return false;
    }
    try {
      keyVec = keyQueue.take();
    } catch (InterruptedException e) {
      return false;
    }
    HashSet<String> fields = null;
    if (keyVec.size() == 1) {
      HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();
      if (db.read(table, keyVec.get(0), fields, cells).isOk() && writerate > 0) {
        double rand = Math.random();
        if (rand < writerate) {
          db.insert(writetable, keyVec.get(0), cells);
        }
      }
    } else {
      Vector<HashMap<String, ByteIterator>> cells = new Vector<>(keyVec.size());
      for (int i = 0; i < keyVec.size(); ++i) {
        cells.add(new HashMap<String, ByteIterator>());
      }
      ArrayList<Status> results = db.batchRead(table, keyVec, fields, cells);
      if (writerate > 0) {
        for (int i = 0; i < results.size(); ++i) {
          if (results.get(i).isOk()) {
            double rand = Math.random();
            if (rand < writerate) {
              db.insert(writetable, keyVec.get(i), cells.get(i));
            }
          }
        }
      }
    }
    return true;
  }

  @Override
  public void cleanup() throws WorkloadException {
    isStop = true;
    keyQueue.clear();
  }
}
