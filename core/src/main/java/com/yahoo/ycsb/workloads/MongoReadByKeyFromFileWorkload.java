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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.*;
import java.util.*;
import java.util.stream.Stream;

public class MongoReadByKeyFromFileWorkload extends Workload {
  private class LoadThread extends Threads {

  }


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


  public static final LinkedBlockingQueue<String> keyQueue = new LinkedBlockingQueue<>();
  private static final ExecutorService producer = Executors.newFixedThreadPool(1);
  private static volatile boolean KEY_FILE_EOF = false;

  /**
   * Initialize the scenario.
   * Called once, in the main client thread, before any operations are started.
   */
  @Override
  public void init(Properties p) throws WorkloadException {
    table = p.getProperty(TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);

    recordcount =
        Integer.parseInt(p.getProperty(Client.RECORD_COUNT_PROPERTY, Client.DEFAULT_RECORD_COUNT));


    // 单线程不断的从文件收集key
    producer.execute(() -> {
      try (Stream<String> stream = Files.lines(Paths.get(KEY_FILE))) {
        stream.forEach(keyQueue::add);
      } catch (IOException e) {
        throw new RuntimeException("Keyfile 读取出错 : " + e.getCause().getMessage());
      } finally {
        KEY_FILE_EOF = true;
      }
    });

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
    //ByteIterator keydata =
    //TODO read from queue

    String key;
    if (KEY_FILE_EOF && keyQueue.isEmpty()) {
      return;
    } else {
      try {
        key = keyQueue.take();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }


    String keyname = "_id";

    HashSet<String> fields = null;

    if (!readallfields) {
      // read a random field
      String fieldname = fieldnames.get(fieldchooser.nextValue().intValue());

      fields = new HashSet<String>();
      fields.add(fieldname);
    } else if (dataintegrity) {
      // pass the full field list if dataintegrity is on for verification
      fields = new HashSet<String>(fieldnames);
    }

    HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();
    db.read(table, keyname, fields, cells);

    if (dataintegrity) {
      verifyRow(keyname, cells);
    }
  }

}
