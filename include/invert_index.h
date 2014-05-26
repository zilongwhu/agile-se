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
#include "signdict.h"

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

        typedef HashTable<uint32_t, void *> Hash;
        typedef Hash::ObjectPool NodePool;

        typedef HashTable<uint32_t, vaddr_t> VHash;
        typedef VHash::ObjectPool VNodePool;

        typedef SignDict::ObjectPool SNodePool;

        typedef TSkipList<VMemoryPool> SkipList;
        typedef TObjectPool<SkipList, VMemoryPool> SkipListPool;

        typedef SortList<uint32_t, VMemoryPool> IDList;
        typedef IDList::ObjectPool INodePool;
        typedef TObjectPool<IDList, VMemoryPool> IDListPool;
    private:
        InvertIndex(const InvertIndex &);
        InvertIndex &operator =(const InvertIndex &);
    public:
        InvertIndex()
        {
            m_merge_threshold = 10000;
            m_merge_all_threshold = 1000;
            m_dict = NULL;
            m_add_dict = NULL;
            m_del_dict = NULL;
            m_docid2signs = NULL;
        }
        ~ InvertIndex();

        int init(const char *path, const char *file);

        DocList *parse(const std::string &query, const std::vector<term_t> terms) const;
        DocList *trigger(const char *keystr, uint8_t type) const;
        bool get_signs_by_docid(int32_t docid, std::vector<uint32_t> &signs) const;

        bool insert(const char *keystr, uint8_t type, int32_t docid, const std::string &json);
        bool insert(const char *keystr, uint8_t type, int32_t docid, cJSON *json);
        bool remove(const char *keystr, uint8_t type, int32_t docid);
        bool remove(int32_t docid);

        void recycle()
        {
            m_pool.recycle();
        }
        size_t docs_num() const
        {
            return this->m_docid2signs->size();
        }
        void print_meta() const;
        void print_list_length();

        bool load(const char *dir);
        bool dump(const char *dir);
    private:
        DocList *trigger(uint32_t sign) const;
        bool insert(const char *keystr, uint8_t type, int32_t docid, void *payload);
        uint32_t merge(uint32_t sign);
        void mergeAll(uint32_t length);
    private:
        static void cleanup_node(Hash::node_t *node, intptr_t arg);
        static void cleanup_diff_node(VHash::node_t *node, intptr_t arg);
        static void cleanup_id_node(VHash::node_t *node, intptr_t arg);
    private:
        InvertTypes m_types;
        int32_t m_merge_threshold;
        int32_t m_merge_all_threshold;

        Pool m_pool;
        NodePool m_node_pool;
        VNodePool m_vnode_pool;
        SNodePool m_snode_pool;
        INodePool m_inode_pool;
        SkipListPool m_skiplist_pool;
        IDListPool m_idlist_pool;

        SignDict m_sign2id;
        Hash *m_dict;
        VHash *m_add_dict;
        VHash *m_del_dict;
        VHash *m_docid2signs;
};

#endif
