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
#include <string>
#include <vector>
#include "mempool2.h"
#include "delaypool.h"
#include "objectpool.h"
#include "hashtable.h"
#include "skiplist.h"
#include "sortlist.h"
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

        typedef TSkipList<VMemoryPool> SkipList;
        typedef TObjectPool<SkipList, VMemoryPool> ListPool;

        typedef SortList<uint64_t, VMemoryPool> SignList;
        typedef TObjectPool<SignList, VMemoryPool> SignPool;
        typedef SignList::ObjectPool SNodePool;
    private:
        InvertIndex(const InvertIndex &);
        InvertIndex &operator =(const InvertIndex &);
    public:
        InvertIndex()
        {
            m_merge_threshold = 32;
            m_dict = NULL;
            m_add_dict = NULL;
            m_del_dict = NULL;
            m_docid2signs = NULL;
        }
        ~ InvertIndex();

        int init(const char *path, const char *file);

        DocList *parse(const std::string &query, const std::vector<term_t> terms) const;
        DocList *trigger(const char *keystr, uint8_t type) const;
        bool get_signs_by_docid(int32_t docid, std::vector<uint64_t> &signs) const;

        bool insert(const char *keystr, uint8_t type, int32_t docid, const std::string &json);
        bool insert(const char *keystr, uint8_t type, int32_t docid, cJSON *json);
        bool remove(const char *keystr, uint8_t type, int32_t docid);
        bool remove(int32_t docid);

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
        static void cleanup_sign_node(VHash::node_t *node, intptr_t arg);
    private:
        InvertTypes m_types;
        int32_t m_merge_threshold;

        Pool m_pool;
        NodePool m_node_pool;
        VNodePool m_vnode_pool;
        SNodePool m_snode_pool;
        ListPool m_list_pool;
        SignPool m_sign_pool;

        Hash *m_dict;
        VHash *m_add_dict;
        VHash *m_del_dict;
        VHash *m_docid2signs;
};

#endif
