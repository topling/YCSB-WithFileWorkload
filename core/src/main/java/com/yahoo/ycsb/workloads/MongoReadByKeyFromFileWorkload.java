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

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Workload;
import com.yahoo.ycsb.WorkloadException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Vector;
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

  protected static String table;

  public static final String KEY_FILE = "keyfile";

  public static final String QUEUE_SIZE = "queuesize";
  public static final String QUEUE_SIZE_DEFAULT = "20000";

  public static final String READ_COUNT = "readcount";
  public static final String READ_COUNT_DEFAULT = "1";


  private static LinkedBlockingQueue<String> keyQueue = null;
  private static String keyfile;
  private static int readcount;

  public static LinkedBlockingQueue<String> getKeyQueue() {
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

    keyfile = p.getProperty(KEY_FILE, KEY_FILE);

    final int queuesize = Integer.parseInt(p.getProperty(QUEUE_SIZE, QUEUE_SIZE_DEFAULT));
    keyQueue = new LinkedBlockingQueue<>();
    readcount = Integer.parseInt(p.getProperty(READ_COUNT, READ_COUNT_DEFAULT));


    // 单线程不断的从文件收集key
    producer.execute(new Runnable() {
      @Override
      public void run() {
        try {
          BufferedReader reader = new BufferedReader(new FileReader(getKeyfile()));
          String line = reader.readLine();
          while (line != null && !getIsStop()) {
            if(keyQueue.size() < queuesize){
              keyQueue.add(line);
              line = reader.readLine();
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
    Vector<String> keyVec = new Vector<>(readcount);
    if (keyFileEof && keyQueue.isEmpty()) {
      return false;
    } else {
      for (int i = 0; i < readcount; ++i) {
        try {
          keyVec.add(i, keyQueue.take());
        } catch (InterruptedException e) {
          if (keyVec.size() == 0) {
            return false;
          }
        }
      }
      HashSet<String> fields = null;
      if (keyVec.size() == 1) {
        HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();
        db.read(table, keyVec.firstElement(), fields, cells);
      } else {
        Vector<HashMap<String, ByteIterator>> cells = new Vector<>(keyVec.size());
        for (int i = 0; i < keyVec.size(); ++i) {
          cells.add(new HashMap<String, ByteIterator>());
        }
        db.read(table, keyVec, fields, cells);
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
