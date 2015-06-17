#ifndef __AGILE_SE_JSON2PB_H__
#define __AGILE_SE_JSON2PB_H__

#include "utils/log.h"
#include "utils/cJSON.h"
#include <google/protobuf/message.h>

bool json2pb(google::protobuf::Message &msg, cJSON *json);

inline bool json2pb(google::protobuf::Message &msg, const char *json)
{
    if (NULL == json)
    {
        P_WARNING("json is NULL");
        return false;
    }
    cJSON *cjson = cJSON_Parse(json);
    if (NULL == cjson)
    {
        P_WARNING("failed to parse json[%s]", json);
        return false;
    }
    bool ret = json2pb(msg, cjson);
    cJSON_Delete(cjson);
    return ret;
}

#endif
