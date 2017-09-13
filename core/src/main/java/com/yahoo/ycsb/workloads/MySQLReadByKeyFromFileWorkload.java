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
 * For mysql read by key from file .
 * <p>
 * Properties to control the client:
 * <UL>
 * </ul>
 */
public class MySQLReadByKeyFromFileWorkload extends Workload {
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

  private static final String KEY_FILE = "keyfile";
  private static final String KEY_FILE_DEFAULT = "/data/publicdata/wikipedia/wikipedia_text_old_ids.txt";

  /**
   *  A String of the fieldnames split by.
   */
  private static final String FIELD_NAMES = "fieldnames";
  /**
   *  Default value of fieldnames.
   */
  private static final String FIELD_NAMES_DEFAULTS = null;

  protected HashSet<String> fieldnames;

  /**
   *  The size of data queue.
   */
  public static final String QUEUE_SIZE = "queuesize";
  /**
   *  The default size of data queue.
   */
  public static final String QUEUE_SIZE_DEFAULT = "20000";

  private static LinkedBlockingQueue<String> keyQueue = null;

  /** The name of primary key. */
  public static final String PRIMARY_KEY_NAME = "primarykey";

  /** Default name of primary key. */
  public static final String PRIMARY_KEY_NAME_DEFAULT = "YCSB_KEY";

  /** The primary key in the user table. */
  private static String primarykey = "YCSB_KEY";
  public static String getPrimarykey() {
    return primarykey;
  }

  private static String keyfile;
  public static String getkeyfile() {
    return keyfile;
  }

  private static double writerate;
  public static double getWriterate() {
    return writerate;
  }

  private static volatile boolean keyFileEof = false;
  private static volatile boolean isStop = false;
  public static boolean getIsStop() {
    return isStop;
  }

  private static ExecutorService producer = Executors.newFixedThreadPool(1);

  /**
   * Initialize the scenario.
   * Called once, in the main client thread, before any operations are started.
   */
  @Override
  public void init(Properties p) throws WorkloadException {
    table = p.getProperty(TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);

    final int queuesize = Integer.parseInt(p.getProperty(QUEUE_SIZE, QUEUE_SIZE_DEFAULT));

    keyfile = p.getProperty(KEY_FILE, KEY_FILE_DEFAULT);

    primarykey = p.getProperty(PRIMARY_KEY_NAME, PRIMARY_KEY_NAME_DEFAULT);
    String fieldNamesStr = p.getProperty(FIELD_NAMES, FIELD_NAMES_DEFAULTS);
    fieldnames = new HashSet<>();
    if (fieldNamesStr != null) {
      List<String> fieldnamelist = Arrays.asList(fieldNamesStr.split(","));
      for (String field : fieldnamelist) {
        fieldnames.add(field);
      }
    }

    keyQueue = new LinkedBlockingQueue<>();

    writetable = p.getProperty(WRITE_TABLENAME_PROPERTY, WRITE_TABLENAME_PROPERTY_DEFAULT);
    writerate = Double.parseDouble(p.getProperty(WRITE_RATE, WRITE_RATE_DEFAULT));

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

  @Override
  public boolean doInsert(DB db, Object threadstate) {
    return false;
  }

  @Override
  public boolean doTransaction(DB db, Object threadstate) {
    return doTransactionRead(db);
  }

  public boolean doTransactionRead(DB db) {
    if (keyFileEof && keyQueue.isEmpty()) {
      return false;
    }
    String key;
    try {
      key = keyQueue.take();
    } catch (InterruptedException e) {
      System.err.println("Read key error: " + e.getCause().getMessage());
      return false;
    }

    HashMap<String, ByteIterator> cells = new HashMap<>();
    if (db.read(table, key, fieldnames, cells).isOk() && writerate > 0) {
      double rand = Math.random();
      if (rand < writerate) {
        if (cells.get(primarykey) != null) {
          cells.remove(primarykey);
        }
        db.insert(writetable, key, cells);
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
