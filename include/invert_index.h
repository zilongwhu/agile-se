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
#include "mempool2.h"
#include "delaypool.h"
#include "objectpool.h"
#include "hashtable.h"
#include "idlist.h"
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
        typedef TDelayPool<VMemoryPool> Pool;
        typedef Pool::vaddr_t vaddr_t;

        typedef HashTable<uint64_t, void *> Hash;
        typedef Hash::ObjectPool NodePool;

        typedef HashTable<uint64_t, vaddr_t> VHash;
        typedef VHash::ObjectPool VNodePool;

        typedef TIDList<VMemoryPool> IDList;
        typedef TObjectPool<IDList, VMemoryPool> ListPool;
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
            m_pool.recycle();
        }
    private:
        DocList *trigger(uint64_t sign, uint8_t type) const;
        bool insert(const char *keystr, uint8_t type, int32_t docid, void *payload);
        void merge(uint64_t sign, uint8_t type);
    private:
        static void cleanup_node(Hash::node_t *node, intptr_t arg);
        static void cleanup_diff_node(VHash::node_t *node, intptr_t arg);
    private:
        InvertTypes m_types;

        Pool m_pool;
        NodePool m_node_pool;
        VNodePool m_vnode_pool;
        ListPool m_list_pool;

        Hash *m_dict;
        VHash *m_add_dict;
        VHash *m_del_dict;
};

#endif
