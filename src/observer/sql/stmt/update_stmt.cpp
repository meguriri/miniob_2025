/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2022/5/22.
//

#include "sql/stmt/update_stmt.h"
#include "common/log/log.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include "sql/stmt/filter_stmt.h"

UpdateStmt::UpdateStmt(Table *table, const Value *value, string attr_name, FilterStmt *filter_stmt)
    : table_(table), value_(value), attr_name_(attr_name), filter_stmt_(filter_stmt)
{}

RC UpdateStmt::create(Db *db, const UpdateSqlNode &update, Stmt *&stmt)
{
  const char *table_name = update.relation_name.c_str();
  if (nullptr == db || nullptr == table_name ||  update.attribute_name.empty() || update.value.attr_type() == AttrType::UNDEFINED) {
    LOG_WARN("invalid argument. db=%p, table_name=%p, attribute_name=%s",db, table_name, update.attribute_name.c_str());
    return RC::INVALID_ARGUMENT;
  }

  // check whether the table exists
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  // check whether the column exists
  TableMeta tm = table->table_meta();
  const FieldMeta *fm = tm.field(update.attribute_name.c_str());
  if (fm == nullptr){
    LOG_WARN("no such column. db=%s, column_name=%s", db->name(), update.attribute_name.c_str());
    return RC::SCHEMA_FIELD_NOT_EXIST;
  }

  const Value     *value     = &update.value;

  FilterStmt *filter_stmt = nullptr;
  if (!update.conditions.empty()) {
    unordered_map<string, Table *> table_map;
    table_map.insert(pair<string, Table *>(string(table_name), table));
    RC          rc          = FilterStmt::create(
        db, table, &table_map, update.conditions.data(), static_cast<int>(update.conditions.size()), filter_stmt);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create filter statement. rc=%d:%s", rc, strrc(rc));
      return rc;
    }
  }

  stmt = new UpdateStmt(table, value, update.attribute_name, filter_stmt);
  return RC::SUCCESS;
}
