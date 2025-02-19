/**
 * Copyright (c) 2016 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package com.yahoo.ycsb.db.flavors;

import com.yahoo.ycsb.db.JdbcDBClient;
import com.yahoo.ycsb.db.StatementType;

/**
 * A default flavor for relational databases.
 */
public class DefaultDBFlavor extends DBFlavor {
  public DefaultDBFlavor() {
    super(DBName.DEFAULT);
  }
  public DefaultDBFlavor(DBName dbName) {
    super(dbName);
  }

  @Override
  public String createInsertStatement(StatementType insertType, String key) {
    StringBuilder insert;
    if (insertType.getType() == StatementType.Type.INSERTREPLACE) {
      insert = new StringBuilder("REPLACE INTO ");
    } else {
      insert = new StringBuilder("INSERT INTO ");
    }
    insert.append(insertType.getTableName());
    insert.append(" (" + JdbcDBClient.getPrimarykey() + "," + insertType.getFieldString() + ")");
    insert.append(" VALUES(?");
    for (int i = 0; i < insertType.getNumFields(); i++) {
      insert.append(",?");
    }
    insert.append(")");
    return insert.toString();
  }

  @Override
  public String createReadStatement(StatementType readType, String key) {
    StringBuilder read = new StringBuilder("SELECT * FROM ");
    read.append(readType.getTableName());
    read.append(" WHERE ");
    read.append(JdbcDBClient.getPrimarykey());
    read.append(" = ");
    read.append("?");
    return read.toString();
  }

  @Override
  public String createDeleteStatement(StatementType deleteType, String key) {
    StringBuilder delete = new StringBuilder("DELETE FROM ");
    delete.append(deleteType.getTableName());
    delete.append(" WHERE ");
    delete.append(JdbcDBClient.getPrimarykey());
    delete.append(" = ?");
    return delete.toString();
  }

  @Override
  public String createUpdateStatement(StatementType updateType, String key) {
    String[] fieldKeys = updateType.getFieldString().split(",");
    StringBuilder update = new StringBuilder("UPDATE ");
    update.append(updateType.getTableName());
    update.append(" SET ");
    for (int i = 0; i < fieldKeys.length; i++) {
      update.append(fieldKeys[i]);
      update.append("=?");
      if (i < fieldKeys.length - 1) {
        update.append(", ");
      }
    }
    update.append(" WHERE ");
    update.append(JdbcDBClient.getPrimarykey());
    update.append(" = ?");
    return update.toString();
  }

  @Override
  public String createScanStatement(StatementType scanType, String key) {
    StringBuilder select = new StringBuilder("SELECT * FROM ");
    select.append(scanType.getTableName());
    select.append(" WHERE ");
    select.append(JdbcDBClient.getPrimarykey());
    select.append(" >= ?");
    select.append(" ORDER BY ");
    select.append(JdbcDBClient.getPrimarykey());
    select.append(" LIMIT ?");
    return select.toString();
  }
}
