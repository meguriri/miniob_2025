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
// Created by meguriri on 2025/10/12.
//

#include "sql/operator/update_physical_operator.h"
#include "common/log/log.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"

RC UpdatePhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    return RC::SUCCESS;
  }

  unique_ptr<PhysicalOperator> &child = children_[0];

  RC rc = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  trx_ = trx;

  const TableMeta &table_meta = table_->table_meta();
  const FieldMeta *field = table_meta.field(attr_name_.c_str());

  while (OB_SUCC(rc = child->next())) {
    Tuple *tuple = child->current_tuple();
    if (nullptr == tuple) {
      LOG_WARN("failed to get current record: %s", strrc(rc));
      return rc;
    }
    // 获取需要修改的元组
    RowTuple *row_tuple = static_cast<RowTuple *>(tuple);
    Record   &record    = row_tuple->record();
    records_.emplace_back(record);
  }

  child->close();

  //先收集记录再修改
  //记录的有效性由事务来保证，如果事务不保证删除的有效性，那说明此事务类型不支持并发控制，比如VacuousTrx
  for (Record &old_record : records_) {
    // 不再使用拷贝构造函数，因为它会进行浅拷贝
    // Record new_record(old_record); 

    // 1. 创建一个空的 Record 对象用于存放新记录
    Record new_record;

    // 2. 使用 copy_data 进行深拷贝，为 new_record 分配独立的内存
    rc = new_record.copy_data(old_record.data(), old_record.len());
    if (rc != RC::SUCCESS) {
        LOG_WARN("failed to copy record data for update. rc=%s", strrc(rc));
        return rc;
    }
    // 3. 别忘了把 RID 也复制过来！
    new_record.set_rid(old_record.rid());

    // 4. 获取新记录的可写数据区指针
    char *new_data = new_record.data();
    
    // 如果字段类型和值的类型不匹配，需要进行类型转换
    if (field->type() != value_->attr_type()) {
        Value casted_value;
        // 正确的类型转换调用方式
        rc = DataType::type_instance(value_->attr_type())->cast_to(*value_, field->type(), casted_value);
        if (rc != RC::SUCCESS) {
            LOG_WARN("Failed to cast value during update. rc=%s", strrc(rc));
            // 类型转换失败，可以返回一个空记录或未修改的记录
            return rc; 
        }
        // 使用转换后的值来更新记录
        memcpy(new_data + field->offset(), casted_value.data(), field->len());
    } else {
        // 类型匹配，直接使用原始值更新
        memcpy(new_data + field->offset(), value_->data(), field->len());
    }
    rc = trx_->update_record(table_, old_record, new_record);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to update record: %s", strrc(rc));
      return rc;
    }
  }
  return RC::SUCCESS;
}

RC UpdatePhysicalOperator::next()
{
  return RC::RECORD_EOF;
}

RC UpdatePhysicalOperator::close()
{
  return RC::SUCCESS;
}

