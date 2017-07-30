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
import com.yahoo.ycsb.generator.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.*;
import java.util.*;
import java.util.stream.Stream;

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

  protected String table;

  public static final String KEY_FILE = "keyfile";

  public static final String QUEUE_SIZE = "queuesize";
  public static final String QUEUE_SIZE_DEFAULT = "20000";


  private static LinkedBlockingQueue<String> keyQueue = null;
  public static LinkedBlockingQueue<String> getKeyQueue() {
    return keyQueue;
  }
  private static ExecutorService producer = Executors.newFixedThreadPool(1);
  private static volatile boolean keyFileEof = false;

  /**
   * Initialize the scenario.
   * Called once, in the main client thread, before any operations are started.
   */
  @Override
  public void init(Properties p) throws WorkloadException {
    table = p.getProperty(TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);
    
    int queuesize = Integer.parseInt(p.getProperty(QUEUE_SIZE, QUEUE_SIZE_DEFAULT));
    keyQueue = new LinkedBlockingQueue<>(queuesize);


    // 单线程不断的从文件收集key
    producer.execute(() -> {
        try {
          Stream<String> stream;
          switch (KEY_FILE) {
          case "/dev/stdin":
            stream = new BufferedReader(new InputStreamReader(System.in)).lines();
            break;
          default:
            stream = Files.lines(Paths.get(KEY_FILE));
          }

          stream.forEach(line -> {
              try {
                getKeyQueue().put(line);
              } catch (InterruptedException e) {
                throw new RuntimeException("KEY插入共享队列出错 : " + e.getMessage());
              }
            }
          );
        } catch (Exception e) {
          throw new RuntimeException("Keyfile 读取出错 : " + e.getCause().getMessage());
        } finally {
          keyFileEof = true;
        }
      }
    );

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
    String key = null;
    if (keyFileEof && keyQueue.isEmpty()) {
      return false;
    } else {
      try {
        key = keyQueue.take();
      } catch (InterruptedException e) {
        e.printStackTrace();
        return false;
      }
    }

    HashSet<String> fields = null;
    HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();
    db.read(table, key, fields, cells);
    return true;
  }

}
