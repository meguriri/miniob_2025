/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "common/lang/comparator.h"
#include "common/log/log.h"
#include "common/type/date_type.h"
#include "common/value.h"
#include <iomanip>

// 0一样，-1小于，1大于
int DateType::compare(const Value &left, const Value &right) const
{
  ASSERT(left.attr_type() == AttrType::DATES, "left type is not date");
  ASSERT(right.attr_type() == AttrType::DATES || right.attr_type() == AttrType::CHARS, "right type is not date or string");
  int left_val  = left.get_date();
  int right_val = right.get_date();
  return common::compare_int((void *)&left_val, (void *)&right_val);
}

RC DateType::set_value_from_str(Value &val, const string &data) const
{
  RC rc = RC::SUCCESS;
  int y=0,m=0,d=0;
  int arg = sscanf(data.c_str(), "%d-%d-%d", &y, &m, &d);
  if (arg != 3 || !is_valid_date(y, m, d)) {
    rc = RC::INVALID_DATE_FORMAT;
  } else {
    val.set_date(y,m,d);
  }
  return rc;
}

RC DateType::cast_to(const Value &val, AttrType type, Value &result) const
{
  switch (type) {
    case AttrType::CHARS: {
      result.set_type(AttrType::CHARS);
      result.set_string(val.to_string().c_str());
    } break;
    default: return RC::UNIMPLEMENTED;
  }
  return RC::SUCCESS;
}

RC DateType::to_string(const Value &val, string &result) const
{
  stringstream ss;
  int date = val.value_.date_value_;
  int year = date / 10000;
  int month = (date % 10000) / 100;
  int day = date % 100;
  ss<< std::setw(4) << std::setfill('0') << year << "-" << std::setw(2) << std::setfill('0') << month << "-" << std::setw(2) << std::setfill('0') << day;
  result = ss.str();
  return RC::SUCCESS;
}

bool DateType::is_valid_date(int year, int month, int day) const
{
    if(month < 1 || month > 12) {
        return false;
    }
    if(day < 1 || day > 31) {
        return false;
    }

    // 判断月份和日期的关系
    if(month == 2) { // 二月
        bool is_leap_year = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
        if(is_leap_year) {
            if(day > 29) {
                return false;
            }
        } else {
            if(day > 28) {
                return false;
            }
        }
    } else if(month == 4 || month == 6 || month == 9 || month == 11) { // 小月
        if(day > 30) {
            return false;
        }
    }
    return true;
}