#pragma once

#include "MsgTypes.h"

unique_ptr<SQLResponse> loadCache ( const wchar_t* query );