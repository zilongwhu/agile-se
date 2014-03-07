// =====================================================================================
//
//       Filename:  invert_index.cpp
//
//    Description:  
//
//        Version:  1.0
//        Created:  03/07/2014 02:35:59 PM
//       Revision:  none
//       Compiler:  g++
//
//         Author:  Zilong.Zhu (), zilong.whu@gmail.com
//        Company:  edu.whu
//
// =====================================================================================

#include "log.h"
#include "configure.h"
#include "invert_index.h"

InvertIndex::~InvertIndex()
{
    if (m_dict)
    {
        delete m_dict;
        m_dict = NULL;
    }
    if (m_add_dict)
    {
        delete m_add_dict;
        m_add_dict = NULL;
    }
    if (m_del_dict)
    {
        delete m_del_dict;
        m_del_dict = NULL;
    }
    __gnu_cxx::hash_map<size_t, DelayPool *>::iterator it = m_list_pools.begin();
    while (it != m_list_pools.end())
    {
        if (it->second)
        {
            delete it->second;
        }
        ++it;
    }
}

int InvertIndex::init(const char *path, const char *file)
{
    if (m_types.init(path, file) < 0)
    {
        WARNING("failed to init invert types");
        return -1;
    }
    Config config(path, file);
    if (config.parse() < 0)
    {
        WARNING("failed parse config[%s:%s]", path, file);
        return -1;
    }
    int node_page_size;
    if (!config.get("node_page_size", node_page_size) || node_page_size <= 0)
    {
        WARNING("failed to get node_page_size");
        return -1;
    }
    int node_pool_size;
    if (!config.get("node_pool_size", node_pool_size) || node_pool_size <= 0)
    {
        WARNING("failed to get node_pool_size");
        return -1;
    }
    if (m_node_pool.init(node_page_size*1024L*1024L, node_pool_size*1024L*1024L) < 0)
    {
        WARNING("failed to init node_pool");
        return -1;
    }
    int diff_node_page_size;
    if (!config.get("diff_node_page_size", diff_node_page_size) || diff_node_page_size <= 0)
    {
        WARNING("failed to get diff_node_page_size");
        return -1;
    }
    int diff_node_pool_size;
    if (!config.get("diff_node_pool_size", diff_node_pool_size) || diff_node_pool_size <= 0)
    {
        WARNING("failed to get diff_node_pool_size");
        return -1;
    }
    if (m_diff_node_pool.init(diff_node_page_size*1024L*1024L, diff_node_pool_size*1024L*1024L) < 0)
    {
        WARNING("failed to init diff_node_pool");
        return -1;
    }
    int list_page_size;
    if (!config.get("list_page_size", list_page_size) || list_page_size <= 0)
    {
        WARNING("failed to get list_page_size");
        return -1;
    }
    int list_pool_size;
    if (!config.get("list_pool_size", list_pool_size) || list_pool_size <= 0)
    {
        WARNING("failed to get list_pool_size");
        return -1;
    }
    if (m_list_pool.init(list_page_size*1024L*1024L, list_pool_size*1024L*1024L) < 0)
    {
        WARNING("failed to init list_pool");
        return -1;
    }
    int list_node_page_size;
    if (!config.get("list_node_page_size", list_node_page_size) || list_node_page_size <= 0)
    {
        WARNING("failed to get list_node_page_size");
        return -1;
    }
    int list_node_pool_size;
    if (!config.get("list_node_pool_size", list_node_pool_size) || list_node_pool_size <= 0)
    {
        WARNING("failed to get list_node_pool_size");
        return -1;
    }
    for (size_t i = 0; i < sizeof(m_types.types)/sizeof(m_types.types[0]); ++i)
    {
        if (m_types.is_valid_type(i))
        {
            m_list_pools[m_types.types[i].payload_len] = NULL;
        }
    }
    __gnu_cxx::hash_map<size_t, DelayPool *>::iterator it = m_list_pools.begin();
    while (it != m_list_pools.end())
    {
        it->second = new DelayPool();
        if (NULL == it->second)
        {
            WARNING("failed to new list_pools");
            return -1;
        }
        if (it->second->init(IDList::element_size(it->first),
                    list_node_page_size*1024L*1024L, list_node_pool_size*1024L*1024L) < 0)
        {
            WARNING("failed to init list_pools");
            return -1;
        }
        ++it;
    }
    for (size_t i = 0; i < sizeof(m_types.types)/sizeof(m_types.types[0]); ++i)
    {
        if (m_types.is_valid_type(i))
        {
            m_types.types[i].pool = m_list_pools[m_types.types[i].payload_len];
        }
    }
    int dict_hash_size;
    if (!config.get("dict_hash_size", dict_hash_size) || dict_hash_size <= 0)
    {
        WARNING("failed to get dict_hash_size");
        return -1;
    }
    m_dict = new HashTable<uint64_t, void *>(&m_node_pool, dict_hash_size);
    if (NULL == m_dict)
    {
        WARNING("failed to new m_dict");
        return -1;
    }
    int add_dict_hash_size;
    if (!config.get("add_dict_hash_size", add_dict_hash_size) || add_dict_hash_size <= 0)
    {
        WARNING("failed to get add_dict_hash_size");
        return -1;
    }
    m_add_dict = new HashTable<uint64_t, IDList *>(&m_diff_node_pool, add_dict_hash_size);
    if (NULL == m_add_dict)
    {
        WARNING("failed to new m_add_dict");
        return -1;
    }
    int del_dict_hash_size;
    if (!config.get("del_dict_hash_size", del_dict_hash_size) || del_dict_hash_size <= 0)
    {
        WARNING("failed to get del_dict_hash_size");
        return -1;
    }
    m_del_dict = new HashTable<uint64_t, IDList *>(&m_diff_node_pool, del_dict_hash_size);
    if (NULL == m_del_dict)
    {
        WARNING("failed to new m_del_dict");
        return -1;
    }
    return 0;
}

bool InvertIndex::insert(const char *keystr, uint8_t type, int docid, const std::string &json)
{
    cJSON *cjson = cJSON_Parse(json.c_str());
    if (NULL == cjson)
    {
        WARNING("failed to parse json[%s]", json.c_str());
        return false;
    }
    bool ret = this->insert(keystr, type, docid, cjson);
    cJSON_Delete(cjson);
    return ret;
}

bool InvertIndex::insert(const char *keystr, uint8_t type, int docid, cJSON *json)
{
    if (NULL == keystr || !m_types.is_valid_type(type) || NULL == json)
    {
        WARNING("invalid parameter");
        return false;
    }
    void *payload = m_types.types[type].parser->parse(json);
    if (NULL == payload)
    {
        WARNING("failed to parse payload for invert type[%d]", int(type));
        return false;
    }
    return this->insert(keystr, type, docid, payload);
}

bool InvertIndex::insert(const char *keystr, uint8_t type, int docid, void *payload)
{
    uint64_t sign = m_types.get_sign(keystr, type);
    IDList **del_list = m_del_dict->find(sign);
    if (del_list)
    {
        (*del_list)->remove(docid);
        if ((*del_list)->size() == 0)
        {
            m_del_dict->remove(sign);
        }
    }
    size_t payload_len = m_types.types[type].payload_len;
    IDList **add_list = m_add_dict->find(sign);
    if (add_list)
    {
        if ((*add_list)->payload_len() != payload_len)
        {
            WARNING("conflicting hash value[%s:%d]", keystr, int(type));
            return false;
        }
        if (!(*add_list)->insert(docid, payload))
        {
            WARNING("failed to insert docid[%d] for hash value[%s:%d]", docid, keystr, int(type));
            return false;
        }
        if ((*add_list)->size() > 32)
        {
            /* merge this list */
        }
    }
    else
    {
        IDList *tmp = m_list_pool.alloc(m_types.types[type].pool, payload_len);
        if (NULL == tmp)
        {
            WARNING("failed to alloc IDList");
            return false;
        }
        if (!tmp->insert(docid, payload)
                || !m_add_dict->insert(sign, tmp))
        {
            m_list_pool.free(tmp);
            WARNING("failed to insert docid[%d] for hash value[%s:%d]", docid, keystr, int(type));
            return false;
        }
    }
    return true;
}

bool InvertIndex::reomve(const char *keystr, uint8_t type, int docid)
{
    return true;
}
