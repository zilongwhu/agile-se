#ifndef __AGILE_SE_INVERT_INDEX_H__
#define __AGILE_SE_INVERT_INDEX_H__

/*
#ifndef __NOT_USE_COWBTREE__
#define __NOT_USE_COWBTREE__
#endif
*/

#ifndef __NOT_DEBUG_COW_BTREE__
#define __NOT_DEBUG_COW_BTREE__
#endif

#include <stdint.h>
#include <string>
#include <vector>
#include "pool/mempool.h"
#include "pool/delaypool.h"
#include "pool/objectpool.h"
#include "index/hashtable.h"
#include "index/skiplist.h"
#include "index/sortlist.h"
#include "index/invert_type.h"
#include "index/signdict.h"
#include "search/doclist.h"
#include "utils/file_watcher.h"
#include "utils/cJSON.h"
#include "utils/fsint.h"

#ifndef __NOT_USE_COWBTREE__
#include "index/cow_btree.h"
#include "pool/mempool2.h"
#endif

class InvertIndex
{
    public:
        struct term_t
        {
            uint8_t type;
            std::string word;
        };
        struct node_t
        {
            char op; /* '*', '&', '|', '-', 'T' */
            uint32_t pos; /* 当op=='T'时，pos指示偏移位置；其他时候没用 */
            std::vector<node_t> children;

            node_t()
            {
                op = 'T';
                pos = (uint32_t)-1;
            }
        };
    public:
        typedef TDelayPool<Mempool> Pool;
        typedef Pool::vaddr_t vaddr_t;

#ifdef __NOT_USE_COWBTREE__
        typedef HashTable<uint32_t, void *> Hash;
        typedef Hash::ObjectPool NodePool;
#else
#if (1)
        typedef MultiMemoryPool RP;
#else
        typedef Mempool RP;
#endif
        typedef TDelayPool<RP> RPool;
        typedef CowBtree<RP, 32> Btree; /* use cowbtree32 */
        typedef HashTable<uint32_t, vaddr_t> Hash;

        typedef TObjectPool<Btree, Mempool> BtreePool;
#endif

        typedef HashTable<uint32_t, vaddr_t> VHash;
        typedef VHash::ObjectPool VNodePool;

        typedef SignDict::ObjectPool SNodePool;

        typedef TSkipList<Mempool> SkipList;
        typedef TObjectPool<SkipList, Mempool> SkipListPool;

        typedef SortList<uint32_t, Mempool> IDList;
        typedef IDList::ObjectPool INodePool;
        typedef TObjectPool<IDList, Mempool> IDListPool;
    private:
        InvertIndex(const InvertIndex &);
        InvertIndex &operator =(const InvertIndex &);
    public:
        InvertIndex()
        {
            m_merge_threshold = 10000;
            m_merge_all_threshold = 1000;
            m_merge_speed = 1024*1024*1024;
            m_merge_sleep = 50;
            m_dict = NULL;
            m_add_dict = NULL;
            m_del_dict = NULL;
            m_words_bag = NULL;
        }
        ~ InvertIndex();

        int init(const char *path, const char *file);

        bool is_valid_type(uint8_t type) const
        {
            return m_types.is_valid_type(type);
        }
        uint64_t get_sign(const char *keystr, uint8_t type) const
        {
            return m_types.get_sign(keystr, type);
        }

        DocList *parse(const std::string &query, const std::vector<term_t> &terms) const;
        DocList *parse_hp(const std::string &query, const std::vector<term_t> &terms,
                std::string *new_query = NULL) const;
        DocList *trigger(const char *keystr, uint8_t type) const;
        bool get_signs_by_docid(int32_t docid, std::vector<uint32_t> &signs) const;

        bool insert(const char *keystr, uint8_t type, int32_t docid, const std::string &json);
        bool insert(const char *keystr, uint8_t type, int32_t docid, cJSON *json);
        bool remove(const char *keystr, uint8_t type, int32_t docid);
        bool remove(int32_t docid);
        bool update_docid(int32_t from, int32_t to);

        void recycle()
        {
            m_pool.recycle();
#ifndef __NOT_USE_COWBTREE__
            m_rpool.recycle();
#endif
        }
        size_t docs_num() const
        {
            return this->m_words_bag->size();
        }
        void print_meta() const;
        void print_list_length(const char *filename = NULL) const;

        void try_exc_cmd();
        void exc_cmd() const;

        bool load(const char *dir, FSInterface *fs = NULL);
        bool dump(const char *dir, FSInterface *fs = NULL);

        size_t doc_num() const { return m_words_bag->size(); }
        void mergeAll(uint32_t length);
    private:
        DocList *trigger(uint32_t sign) const;
        DocList *trigger(const node_t &node, const std::vector<term_t> &terms) const;
        bool insert(const char *keystr, uint8_t type, int32_t docid, void *payload)
        {
            return this->insert(m_types.record_sign(keystr, type), docid, payload, keystr, type);
        }
        bool insert(uint32_t sign, int32_t docid, void *payload, const char *keystr, uint8_t type);
        uint32_t merge(uint32_t sign);
    private:
        static void cleanup_node(Hash::node_t *node, intptr_t arg);
        static void cleanup_diff_node(VHash::node_t *node, intptr_t arg);
        static void cleanup_id_node(VHash::node_t *node, intptr_t arg);
        static void adjust_node(node_t &node);
        static std::string print_node(const node_t &node);
    private:
        InvertTypes m_types;
        uint32_t m_merge_threshold;
        uint32_t m_merge_all_threshold;
        uint32_t m_merge_speed;
        uint32_t m_merge_sleep;

        Pool m_pool;
#ifdef __NOT_USE_COWBTREE__
        NodePool m_node_pool;
#else
        RPool m_rpool;
        BtreePool m_btree_pool;
#endif
        VNodePool m_vnode_pool;
        SNodePool m_snode_pool;
        INodePool m_inode_pool;
        SkipListPool m_skiplist_pool;
        IDListPool m_idlist_pool;

        SignDict m_sign2id;
        Hash *m_dict;
        VHash *m_add_dict;
        VHash *m_del_dict;
        VHash *m_words_bag;

        FileWatcher m_exc_cmd_fw;
        std::string m_exc_cmd_file;
};

#endif
