
#pragma once

#include "cpprest/json.h"
#include "cpprest/asyncrt_utils.h"
#include "MsgTypes.h"

TableMeta* TableMetaFromJSON ( web::json::value & object );
ColumnMeta* ColumnMetaFromJSON ( web::json::value & object );
SelectedColumnMeta* SelectedColumnMetaFromJSON ( web::json::value & object );
std::unique_ptr<MetadataResponse> MetadataResponseFromJSON ( web::json::value & object );
std::unique_ptr<SQLResponse> SQLResponseFromJSON ( web::json::value & object );
std::unique_ptr<ErrorMessage> ErrorMessageFromJSON ( web::json::value & object );
