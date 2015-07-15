#pragma once

#include "vld.h"
#include "MsgTypes.h"

//REST
bool restAuthenticate ( char* serverAddr, long port, char* username, char* passwd );

void restListProjects ( char* serverAddr, long port, char* username, char* passwd, std::vector<string>& holder );

std::unique_ptr<SQLResponse> restQuery ( wchar_t* rawSql, char* serverAddr, long port, char* username,
                                         char* passwd,
                                         char* project );

std::unique_ptr<MetadataResponse> restGetMeta ( char* serverAddr, long port, char* username, char* passwd,
                                                char* project );
