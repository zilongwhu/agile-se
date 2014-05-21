// =====================================================================================
//
//       Filename:  json2pb.h
//
//    Description:  
//
//        Version:  1.0
//        Created:  05/21/2014 04:14:31 PM
//       Revision:  none
//       Compiler:  g++
//
//         Author:  Zhu Zilong (), zhuzilong@baidu.com
//        Company:  baidu.com.houyi
//
// =====================================================================================

#ifndef __AGILE_SE_JSON2PB_H__
#define __AGILE_SE_JSON2PB_H__

#include <google/protobuf/message.h>
#include "cJSON.h"
#include "log.h"

bool json2pb(google::protobuf::Message &msg, cJSON *json);

inline bool json2pb(google::protobuf::Message &msg, const char *json)
{
    if (NULL == json)
    {
        WARNING("json is NULL");
        return false;
    }
    cJSON *cjson = cJSON_Parse(json);
    if (NULL == cjson)
    {
        WARNING("failed to parse json[%s]", json);
        return false;
    }
    bool ret = json2pb(msg, cjson);
    cJSON_Delete(cjson);
    return ret;
}

#endif
