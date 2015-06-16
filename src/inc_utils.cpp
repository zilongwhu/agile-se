// =====================================================================================
//
//       Filename:  inc_utils.cpp
//
//    Description:  
//
//        Version:  1.0
//        Created:  05/08/2014 12:24:30 PM
//       Revision:  none
//       Compiler:  g++
//
//         Author:  Zilong.Zhu (), zilong.whu@gmail.com
//        Company:  edu.whu
//
// =====================================================================================

#include "log.h"
#include "inc_utils.h"
#include "cJSON.h"

int parse_forward_json(const std::string &json, std::vector<std::pair<std::string, std::string> > &kvs)
{
    kvs.clear();

    cJSON *cjson = cJSON_Parse(json.c_str());
    cJSON *fields = NULL;
    if (NULL == cjson)
    {
        WARNING("failed to parse json[%s]", json.c_str());
        return -1;
    }
    if (cJSON_Object != cjson->type)
    {
        WARNING("invalid json, must be an object, [%s]", json.c_str());
        goto FAIL;
    }
    fields = cJSON_GetObjectItem(cjson, "fields");
    if (NULL == fields)
    {
        WARNING("invalid json, must have fields array, [%s]", json.c_str());
        goto FAIL;
    }
    if (cJSON_Array != fields->type)
    {
        WARNING("invalid json, fields must be an array, [%s]", json.c_str());
        goto FAIL;
    }
    for (int i = 0, n = cJSON_GetArraySize(fields); i < n; ++i)
    {
        cJSON *item = cJSON_GetArrayItem(fields, i);
        if (NULL == item)
        {
            WARNING("invalid json, must have fields[%d] object, [%s]", i, json.c_str());
            goto FAIL;
        }
        if (cJSON_Object != item->type)
        {
            WARNING("invalid json, fields[%d] must be an object, [%s]", i, json.c_str());
            goto FAIL;
        }
        cJSON *key = cJSON_GetObjectItem(item, "k");
        if (NULL == key)
        {
            WARNING("invalid json, must have fields[%d].k, [%s]", i, json.c_str());
            goto FAIL;
        }
        if (cJSON_String != key->type)
        {
            WARNING("invalid json, fields[%d].k must be an string, [%s]", i, json.c_str());
            goto FAIL;
        }
        cJSON *value = cJSON_GetObjectItem(item, "v");
        if (NULL == value)
        {
            WARNING("invalid json, must have fields[%d].v, [%s]", i, json.c_str());
            goto FAIL;
        }
        if (cJSON_String != value->type)
        {
            WARNING("invalid json, fields[%d].v must be an string, [%s]", i, json.c_str());
            goto FAIL;
        }
        kvs.push_back(std::make_pair(std::string(key->valuestring), std::string(value->valuestring)));
    }
    cJSON_Delete(cjson);
    return 0;
FAIL:
    cJSON_Delete(cjson);
    return -1;
}

int parse_invert_json(const std::string &json, std::vector<invert_data_t> &kvs)
{
    kvs.clear();

    cJSON *cjson = cJSON_Parse(json.c_str());
    cJSON *inverts = NULL;
    if (NULL == cjson)
    {
        WARNING("failed to parse json[%s]", json.c_str());
        return -1;
    }
    if (cJSON_Object != cjson->type)
    {
        WARNING("invalid json, must be an object, [%s]", json.c_str());
        goto FAIL;
    }
    inverts = cJSON_GetObjectItem(cjson, "inverts");
    if (NULL == inverts)
    {
        WARNING("invalid json, must have inverts array, [%s]", json.c_str());
        goto FAIL;
    }
    if (cJSON_Array != inverts->type)
    {
        WARNING("invalid json, inverts must be an array, [%s]", json.c_str());
        goto FAIL;
    }
    for (int i = 0, n = cJSON_GetArraySize(inverts); i < n; ++i)
    {
        cJSON *invert = cJSON_GetArrayItem(inverts, i);
        if (NULL == invert)
        {
            WARNING("invalid json, must have inverts[%d] object, [%s]", i, json.c_str());
            goto FAIL;
        }
        if (cJSON_Object != invert->type)
        {
            WARNING("invalid json, inverts[%d] must be an object, [%s]", i, json.c_str());
            goto FAIL;
        }
        cJSON *invert_type = cJSON_GetObjectItem(invert, "invert_type");
        if (NULL == invert_type)
        {
            WARNING("invalid json, must have inverts[%d].invert_type, [%s]", i, json.c_str());
            goto FAIL;
        }
        if (cJSON_Number != invert_type->type)
        {
            WARNING("invalid json, inverts[%d].invert_type must be a number, [%s]", i, json.c_str());
            goto FAIL;
        }
        cJSON *items = cJSON_GetObjectItem(invert, "items");
        if (NULL == items)
        {
            WARNING("invalid json, must have inverts[%d].items, [%s]", i, json.c_str());
            goto FAIL;
        }
        if (cJSON_Array != items->type)
        {
            WARNING("invalid json, inverts[%d].items must be an array, [%s]", i, json.c_str());
            goto FAIL;
        }
        for (int j = 0, m = cJSON_GetArraySize(items); j < m; ++j)
        {
            cJSON *item = cJSON_GetArrayItem(items, j);
            if (NULL == item)
            {
                WARNING("invalid json, must have inverts[%d].items[%d] object, [%s]", i, j, json.c_str());
                goto FAIL;
            }
            if (cJSON_Object != item->type)
            {
                WARNING("invalid json, inverts[%d].items[%d] must be an object, [%s]", i, j, json.c_str());
                goto FAIL;
            }
            cJSON *key = cJSON_GetObjectItem(item, "k");
            if (NULL == key)
            {
                WARNING("invalid json, must have inverts[%d].items[%j].k, [%s]", i, j, json.c_str());
                goto FAIL;
            }
            if (cJSON_String != key->type)
            {
                WARNING("invalid json, inverts[%d].items[%d].k must be an string, [%s]", i, j, json.c_str());
                goto FAIL;
            }
            invert_data_t data;
            cJSON *value = cJSON_GetObjectItem(item, "v");
            if (NULL != value)
            {
                if (cJSON_String != value->type)
                {
                    WARNING("invalid json, inverts[%d].items[%d].v must be an string, [%s]", i, j, json.c_str());
                    goto FAIL;
                }
                data.value = std::string(value->valuestring);
            }
            data.type = invert_type->valueint;
            data.key = std::string(key->valuestring);
            kvs.push_back(data);
        }
    }
    cJSON_Delete(cjson);
    return 0;
FAIL:
    cJSON_Delete(cjson);
    return -1;
}
