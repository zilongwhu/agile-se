// =====================================================================================
//
//       Filename:  builder.cpp
//
//    Description:  
//
//        Version:  1.0
//        Created:  05/04/2014 02:30:26 PM
//       Revision:  none
//       Compiler:  g++
//
//         Author:  Zilong.Zhu (), zilong.whu@gmail.com
//        Company:  edu.whu
//
// =====================================================================================

#include "reader.h"
#include "forward_index.h"
#include "invert_index.h"

class TermParser: public InvertParser
{
    public:
        TermParser() { memset(m_bytes, 0, sizeof m_bytes); }
        void *parse(cJSON *json)
        {
            return m_bytes;
        }
    private:
        char m_bytes[16];
};

class DummyParser: public InvertParser
{
    public:
        void *parse(cJSON *json)
        {
            return NULL;
        }
};

DEFINE_INVERT_PARSER(TermParser);
DEFINE_INVERT_PARSER(DummyParser);

int parse_forward_json(const std::string &json, std::vector<std::pair<std::string, std::string> > &kvs)
{
    kvs.clear();

    cJSON *cjson = cJSON_Parse(json.c_str());
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
    cJSON *fields = cJSON_GetObjectItem(cjson, "fields");
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

struct invert_data_t
{
    int8_t type;
    std::string key;
    std::string value;
};

int parse_invert_json(const std::string &json, std::vector<invert_data_t> &kvs)
{
    kvs.clear();

    cJSON *cjson = cJSON_Parse(json.c_str());
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
    cJSON *inverts = cJSON_GetObjectItem(cjson, "inverts");
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

int build_index()
{
    enum
    {
        FORWARD_LEVEL = 2,
        INVERT_LEVEL = 3,
    };
    enum { OP_INSERT = 0, OP_DELETE = 1, OP_UPDATE = 2 };

    init_time_updater();

    REGISTER_INVERT_PARSER(TermParser);
    REGISTER_INVERT_PARSER(DummyParser);

    ForwardIndex idx;
    idx.init("./conf", "fields.conf");

    InvertIndex invert;
    invert.init("./conf", "invert.conf");

    IncReader reader;
    reader.init("./conf", "inc.meta");

    const size_t log_buffer_length = 4096;
    char *log_buffer = new char[log_buffer_length];

    char *line = reader._line_buf;
    uint32_t now = g_now_time;
    uint32_t delete_count = 0;
    uint32_t update_invert_count = 0;
    uint32_t update_forward_count = 0;

    std::vector<uint64_t> signs;
    std::vector<invert_data_t> items;
    std::vector<std::pair<std::string, std::string> > kvs;
    while (1)
    {
        int ret = reader.next();
        if (0 == ret)
        {
            ::usleep(10);
            return -1;
        }
        else if (1 == ret)
        {
            std::vector<std::string> fields;
            split(line, "\t", fields);
            if (fields.size() < 4)
            {
                P_MYLOG("must has [eventid, optype, level, oid]");
                continue;
            }
            int64_t eventid = ::strtoul(fields[0].c_str(), NULL, 10);
            int32_t optype = ::strtol(fields[1].c_str(), NULL, 10);
            int32_t level = ::strtol(fields[2].c_str(), NULL, 10);
            int32_t oid = ::strtoul(fields[3].c_str(), NULL, 10);
            if (oid % reader.total_partition() != reader.current_partition())
            {
                continue;
            }
            TRACE("eventid=%ld, optype=%d, level=%d, oid=%d", eventid, optype, level, oid);
            switch (level)
            {
                case FORWARD_LEVEL:
                    if (OP_UPDATE == optype)
                    {
                        if (parse_forward_json(fields[4], kvs) < 0)
                        {
                            P_MYLOG("failed to parse forward json");
                        }
                        else if (!idx.update(oid, kvs))
                        {
                            P_MYLOG("failed to update forward index");
                        }
                        else
                        {
                            TRACE("update forward ok, oid=%d", oid);
                            ++update_forward_count;
                        }
                    }
                    break;
                case INVERT_LEVEL:
                    if (OP_DELETE == optype)
                    {
                        if (invert.get_signs_by_docid(oid, signs))
                        {
                            for (size_t i = 0; i < signs.size(); ++i)
                            {
                                invert.remove(signs[i], oid);
                            }
                        }
                        idx.remove(oid);
                        TRACE("delete ok, oid=%d", oid);
                        ++delete_count;
                    }
                    else if (OP_UPDATE == optype)
                    {
                        if (parse_forward_json(fields[4], kvs) < 0)
                        {
                            P_MYLOG("failed to parse forward json");
                        }
                        else if (parse_invert_json(fields[5], items) < 0)
                        {
                            P_MYLOG("failed to parse invert json");
                        }
                        else
                        {
                            if (invert.get_signs_by_docid(oid, signs))
                            {
                                for (size_t i = 0; i < signs.size(); ++i)
                                {
                                    invert.remove(signs[i], oid);
                                }
                            }
                            if (!idx.update(oid, kvs))
                            {
                                P_MYLOG("failed to update forward index");
                            }
                            else
                            {
                                for (size_t i = 0; i < items.size(); ++i)
                                {
                                    invert.insert(items[i].key.c_str(), items[i].type, oid, items[i].value);
                                }
                                TRACE("update ok, oid=%d", oid);
                                ++update_invert_count;
                            }
                        }
                    }
                    break;
            };
            if (now != g_now_time)
            {
                WARNING("processe stats: update[forward=%u, invert=%u], delete[%u], in[%u ~ %u]",
                        update_forward_count, update_invert_count, delete_count, now, g_now_time);

                idx.recycle();
                invert.recycle();

                delete_count = 0;
                update_invert_count = 0;
                update_forward_count = 0;
                now = g_now_time;
            }
        }
        else if (ret < 0)
        {
            FATAL("disk file error");
            return -1;
        }
    }
    return 0;
}
