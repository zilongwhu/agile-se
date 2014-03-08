// =====================================================================================
//
//       Filename:  invert_index.h
//
//    Description:  
//
//        Version:  1.0
//        Created:  03/06/2014 08:39:45 PM
//       Revision:  none
//       Compiler:  g++
//
//         Author:  Zilong.Zhu (), zilong.whu@gmail.com
//        Company:  edu.whu
//
// =====================================================================================

#ifndef __AGILE_SE_INVERT_INDEX_H__
#define __AGILE_SE_INVERT_INDEX_H__

#include <stdint.h>
#include <ext/hash_map>
#include <string>
#include <vector>
#include "mempool.h"
#include "idlist.h"
#include "hashtable.h"
#include "invert_type.h"
#include "doclist.h"

class InvertIndex
{
    public:
        struct term_t
        {
            uint8_t type;
            std::string word;
        };
    private:
        InvertIndex(const InvertIndex &);
        InvertIndex &operator =(const InvertIndex &);
    public:
        InvertIndex()
        {
            m_dict = NULL;
            m_add_dict = NULL;
            m_del_dict = NULL;
        }
        ~ InvertIndex();

        int init(const char *path, const char *file);

        DocList *parse(const std::string &query, const std::vector<term_t> terms) const;
        DocList *trigger(const char *keystr, uint8_t type) const;

        bool insert(const char *keystr, uint8_t type, int32_t docid, const std::string &json);
        bool insert(const char *keystr, uint8_t type, int32_t docid, cJSON *json);
        bool reomve(const char *keystr, uint8_t type, int32_t docid);

        void recycle()
        {
            m_node_pool.recycle(cleanup_node, this);
            m_diff_node_pool.recycle(cleanup_diff_node, this);

            __gnu_cxx::hash_map<size_t, DelayPool *>::iterator it = m_list_pools.begin();
            while (it != m_list_pools.end())
            {
                it->second->recycle();
                ++it;
            }
        }
    private:
        DocList *trigger(uint64_t sign, uint8_t type) const;
        bool insert(const char *keystr, uint8_t type, int32_t docid, void *payload);
        void merge(uint64_t sign, uint8_t type);
    private:
        static void cleanup_node(HashTable<uint64_t, void *>::node_t *node, void *arg);
        static void cleanup_diff_node(HashTable<uint64_t, IDList *>::node_t *node, void *arg);
    private:
        InvertTypes m_types;

        ObjectPool<HashTable<uint64_t, void *>::node_t> m_node_pool;
        ObjectPool<HashTable<uint64_t, IDList *>::node_t> m_diff_node_pool;

        ObjectPool<IDList> m_list_pool;
        __gnu_cxx::hash_map<size_t, DelayPool *> m_list_pools;

        HashTable<uint64_t, void *> *m_dict;
        HashTable<uint64_t, IDList *> *m_add_dict;
        HashTable<uint64_t, IDList *> *m_del_dict;
};

#endif
