// =====================================================================================
//
//       Filename:  forward_index.cpp
//
//    Description:  
//
//        Version:  1.0
//        Created:  03/05/2014 05:15:20 PM
//       Revision:  none
//       Compiler:  g++
//
//         Author:  Zilong.Zhu (), zilong.whu@gmail.com
//        Company:  edu.whu
//
// =====================================================================================

#include <iostream>
#include "log.h"
#include "configure.h"
#include "forward_index.h"

std::map<std::string, FieldParser_creater> g_field_parsers;

struct FieldConfig
{
    std::string name;
    std::string parser;
    int type;
    int offset;
    int size;
};

ForwardIndex::~ForwardIndex()
{
    if (m_dict)
    {
        delete m_dict;
        m_dict = NULL;
    }
    __gnu_cxx::hash_map<std::string, FieldDes>::iterator it = m_fields.begin();
    while (it != m_fields.end())
    {
        if (it->second.parser)
        {
            delete it->second.parser;
        }
        ++it;
    }
    m_fields.clear();
}

int ForwardIndex::init(const char *path, const char *file)
{
    Config config(path, file);
    if (config.parse() < 0)
    {
        WARNING("failed parse config[%s:%s]", path, file);
        return -1;
    }
    int field_num = 0;
    if (!config.get("field_num", field_num) || field_num <= 0)
    {
        WARNING("failed to get field_num");
        return -1;
    }
    int max_size = 0;
    std::vector<FieldConfig> fields;
    for (int i = 0; i < field_num; ++i)
    {
        FieldConfig field;
        char buffer[256];

        ::snprintf(buffer, sizeof(buffer), "field_%d_name", i);
        if (!config.get(buffer, field.name))
        {
            WARNING("failed to get %s", buffer);
            return -1;
        }
        ::snprintf(buffer, sizeof(buffer), "field_%d_type", i);
        std::string type;
        if (!config.get(buffer, type))
        {
            WARNING("failed to get %s", buffer);
            return -1;
        }
        if ("int" == type)
        {
            field.type = INT_TYPE;
            field.size = sizeof(int);
        }
        else if ("float" == type)
        {
            field.type = FLOAT_TYPE;
            field.size = sizeof(float);
        }
        else if ("self_define" == type)
        {
            field.type = SELF_DEFINE_TYPE;
            field.size = sizeof(void *);
        }
        else
        {
            WARNING("invalid field_type of [%s] = %s", buffer, type.c_str());
            return -1;
        }
        if (SELF_DEFINE_TYPE == field.type)
        {
            ::snprintf(buffer, sizeof(buffer), "field_%d_parser", i);
            if (!config.get(buffer, field.parser))
            {
                WARNING("failed to get %s", buffer);
                return -1;
            }
        }
        ::snprintf(buffer, sizeof(buffer), "field_%d_offset", i);
        if (!config.get(buffer, field.offset))
        {
            WARNING("failed to get %s", buffer);
            return -1;
        }
        if (field.offset < 0 || field.offset % field.size != 0)
        {
            WARNING("invalid %s[%d] of field[%s]", buffer, field.offset, field.name.c_str());
            return -1;
        }
        fields.push_back(field);
        if (max_size < field.offset + field.size)
        {
            max_size = field.offset + field.size;
        }
    }
    WARNING("info_size = %d", max_size);

    std::vector<int> placeholder(max_size, -1);
    for (size_t i = 0; i < fields.size(); ++i)
    {
        for (int j = 0; j < fields[i].size; ++j)
        {
            if (placeholder[fields[i].offset + j] != -1)
            {
                WARNING("conflict configs at place[%d]", fields[i].offset + j);
                return -1;
            }
            placeholder[fields[i].offset + j] = fields[i].offset;
        }
    }
    for (size_t i = 0; i < placeholder.size(); ++i)
    {
        if (placeholder[i] != -1)
        {
            for (size_t j = 0; j < fields.size(); ++j)
            {
                if (fields[j].offset == placeholder[i])
                {
                    WARNING("field[%04d], type[%d], name[%s] ", i, fields[j].type, fields[j].name.c_str());
                    break;
                }
            }
        }
    }
    for (size_t i = 0; i < fields.size(); ++i)
    {
        FieldDes fd;

        if (m_fields.find(fields[i].name) != m_fields.end())
        {
            WARNING("duplicate field name[%s]", fields[i].name.c_str());
            goto FAIL;
        }
        fd.offset = fields[i].offset;
        fd.type = fields[i].type;
        if (SELF_DEFINE_TYPE == fd.type)
        {
            std::map<std::string, FieldParser_creater>::iterator it = g_field_parsers.find(fields[i].parser);
            if (it == g_field_parsers.end())
            {
                WARNING("unsupported field parser[%s]", fields[i].parser.c_str());
                goto FAIL;
            }
            fd.parser = (*it->second)();
            if (NULL == fd.parser)
            {
                WARNING("failed to create field parser[%s]", fields[i].parser.c_str());
                goto FAIL;
            }
        }
        else
        {
            fd.parser = NULL;
        }

        m_fields.insert(std::make_pair(fields[i].name, fd));
    }
    m_info_size = max_size;
    int mem_page_size;
    if (!config.get("mem_page_size", mem_page_size) || mem_page_size <= 0)
    {
        WARNING("failed to get mem_page_size");
        goto FAIL;
    }
    int mem_pool_size;
    if (!config.get("mem_pool_size", mem_pool_size) || mem_pool_size <= 0)
    {
        WARNING("failed to get mem_pool_size");
        goto FAIL;
    }
    if (m_pool.init(m_info_size,
                mem_page_size*1024L*1024L,
                mem_pool_size*1024L*1024L) < 0)
    {
        WARNING("failed to init mempool");
        goto FAIL;
    }
    int node_page_size;
    if (!config.get("node_page_size", node_page_size) || node_page_size <= 0)
    {
        WARNING("failed to get node_page_size");
        goto FAIL;
    }
    int node_pool_size;
    if (!config.get("node_pool_size", node_pool_size) || node_pool_size <= 0)
    {
        WARNING("failed to get node_pool_size");
        goto FAIL;
    }
    if (m_node_pool.init(node_page_size*1024L*1024L, node_pool_size*1024L*1024L) < 0)
    {
        WARNING("failed to init nodepool");
        goto FAIL;
    }
    int bucket_size;
    if (!config.get("bucket_size", bucket_size) || bucket_size <= 0)
    {
        WARNING("failed to get bucket_size");
        goto FAIL;
    }
    m_dict = new HashTable<long, void *>(&m_node_pool, bucket_size);
    if (NULL == m_dict)
    {
        WARNING("failed to init hash dict");
        goto FAIL;
    }
    WARNING("mempool[%d M, %d M], nodepool[%d M, %d M], bucket size[%d]",
            mem_page_size, mem_pool_size, node_page_size, node_pool_size, bucket_size);
    return 0;
FAIL:
    __gnu_cxx::hash_map<std::string, FieldDes>::iterator it = m_fields.begin();
    while (it != m_fields.end())
    {
        if (it->second.parser)
        {
            delete it->second.parser;
        }
        ++it;
    }
    m_fields.clear();
    return -1;
}

int ForwardIndex::get_offset_by_name(const char *name) const
{
    __gnu_cxx::hash_map<std::string, FieldDes>::const_iterator it = m_fields.find(name);
    if (it == m_fields.end())
    {
        return -1;
    }
    return it->second.offset;
}

void *ForwardIndex::get_info_by_id(long id) const
{
    void ***value;
    if (m_dict->get(id, value))
    {
        return **value;
    }
    return NULL;
}

bool ForwardIndex::update(long id, const std::vector<std::pair<std::string, std::string> > &kvs)
{
    std::vector<std::pair<std::string, cJSON *> > tmp;
    for (size_t i = 0; i < kvs.size(); ++i)
    {
        cJSON *json = cJSON_Parse(kvs[i].second.c_str());
        if (NULL == json)
        {
            break;
        }
        tmp.push_back(std::make_pair(kvs[i].first, json));
    }
    bool ret = true;
    if (tmp.size() < kvs.size())
    {
        ret = false;
        goto CLEAN;
    }
    ret = this->update(id, tmp);
CLEAN:
    for (size_t i = 0; i < tmp.size(); ++i)
    {
        cJSON_Delete(tmp[i].second);
    }
    return ret;
}

bool ForwardIndex::update(long id, const std::vector<std::pair<std::string, cJSON *> > &kvs)
{
    for (size_t i = 0; i < kvs.size(); ++i)
    {
        const std::pair<std::string, cJSON *> &kv = kvs[i];
        __gnu_cxx::hash_map<std::string, FieldDes>::const_iterator it = m_fields.find(kv.first);
        if (it == m_fields.end())
        {
            continue;
        }
        int offset = it->second.offset;
        int type = it->second.type;
        if (INT_TYPE == type)
        {
            if (kv.second->type == cJSON_Number)
            {
                kv.second->valueint;
            }
        }
        else if (FLOAT_TYPE == type)
        {
            if (kv.second->type == cJSON_Number)
            {
                kv.second->valuedouble;
            }
        }
        else
        {
            FieldParser *parser = it->second.parser;
            void *ptr = NULL;
            if (parser->parse(kv.second, ptr))
            {

            }
        }
    }
    return true;
}

void ForwardIndex::remove(long id)
{

}
