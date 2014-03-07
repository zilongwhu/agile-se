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
    if (!config.get("diff_diff_node_page_size", diff_node_page_size) || diff_node_page_size <= 0)
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
    if (!config.get("diff_list_page_size", list_page_size) || list_page_size <= 0)
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
        if (it->second->init(IDList::element_size(it->first), list_page_size*1024L*1024L, list_pool_size*1024L*1024L) < 0)
        {
            WARNING("failed to init list_pools");
            return -1;
        }
        ++it;
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
