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
    
     // 确定要复制的字节数
    size_t copy_len = 0;
    const Value *value_to_copy = value_; // 默认使用原始值
    // 如果需要类型转换
    Value casted_value; // 提前声明以扩大作用域
    if (field->type() != value_->attr_type()) {
        rc = DataType::type_instance(value_->attr_type())->cast_to(*value_, field->type(), casted_value);
        if (rc != RC::SUCCESS) {
            LOG_WARN("Failed to cast value during update. rc=%s", strrc(rc));
            return rc; 
        }
        value_to_copy = &casted_value; // 后续使用转换后的值
    }

    // 根据值的类型和字段定义，计算正确的复制长度
    if (field->type() == AttrType::CHARS) {
        // 对于 CHARS 类型，复制新值的实际长度（包括 '\0'），但不能超过字段定义的最大长度
        copy_len = value_to_copy->length() + 1; // +1 for '\0'
        if (copy_len > field->len()) {
            copy_len = field->len(); // 截断
        }
    } else {
        // 对于非字符串类型（如 INT, FLOAT），直接使用字段定义的长度
        copy_len = field->len();
    }

    // 使用计算出的正确长度执行 memcpy
    memcpy(new_data + field->offset(), value_to_copy->data(), copy_len);

    // 如果是定长字符串，且新值比字段短，需要用空格或'\0'填充剩余部分
    if (field->type() == AttrType::CHARS && copy_len < field->len()) {
        memset(new_data + field->offset() + copy_len, 0, field->len() - copy_len);
    }
    rc = trx_->update_record(table_, old_record, new_record);
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

