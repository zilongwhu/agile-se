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
#include "mempool.h"
#include "idlist.h"
#include "hashtable.h"
#include "invert_type.h"
#include "doclist.h"

class InvertIndex
{
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

        DocList *trigger(const char *keystr, uint8_t type) const;

        bool insert(const char *keystr, uint8_t type, int32_t docid, const std::string &json);
        bool insert(const char *keystr, uint8_t type, int32_t docid, cJSON *json);
        bool reomve(const char *keystr, uint8_t type, int32_t docid);
    private:
        bool insert(const char *keystr, uint8_t type, int32_t docid, void *payload);
        void merge(uint64_t sign, uint8_t type);
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
