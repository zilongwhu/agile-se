#ifndef __STDC_LIMIT_MACROS
#define __STDC_LIMIT_MACROS
#endif

#include <stdint.h>
#include <stack>
#include <fstream>
#include "configure.h"
#include "parse/parser.h"
#include "index/invert_index.h"
#include "search/biglist.h"
#ifndef __NOT_USE_COWBTREE__
#include "search/cow_btree_list.h"
#include "search/mergelist_cowbtree.h"
#endif
#include "search/addlist.h"
#include "search/deletelist.h"
#include "search/andlist.h"
#include "search/mergelist.h"
#include "search/conjunction.h"
#include "search/disjunction.h"
#include "search/reqoptlist.h"
#include "fast_timer.h"
#include "log_utils.h"
#include "str_utils.h"

typedef AddList<InvertIndex::SkipList> AddListImpl;
typedef DeleteList<InvertIndex::SkipList> DeleteListImpl;

InvertIndex::~InvertIndex()
{
    /* 关闭延迟回收功能 */
    m_pool.set_delayed_time(0);
#ifndef __NOT_USE_COWBTREE__
    m_rpool.set_delayed_time(0);
#endif
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
    if (m_words_bag)
    {
        delete m_words_bag;
        m_words_bag = NULL;
    }
    m_pool.recycle();
#ifndef __NOT_USE_COWBTREE__
    m_rpool.recycle();
#endif
}

int InvertIndex::init(const char *path, const char *file)
{
    if (m_types.init(path, file) < 0)
    {
        P_WARNING("failed to init invert types");
        return -1;
    }
    Config conf(path, file) ;
    if (conf.parse() < 0)
    {
        P_WARNING("failed parse config[%s:%s]", path, file);
        return -1;
    }
    {
        m_exc_cmd_file = conf["commands_file"];
        if (m_exc_cmd_fw.create(m_exc_cmd_file.c_str()) < 0)
        {
            P_WARNING("failed to create commands file watcher at[%s]", m_exc_cmd_file.c_str());
            return -1;
        }
    }
    /* register items to pool */
    if (SkipList::init_pool(&m_pool, 0) < 0)
    {
        P_WARNING("failed to register item for deleted list");
        return -1;
    }
#ifndef __NOT_USE_COWBTREE__
    if (m_rpool.register_item(Btree::node_size()) < 0)
    {
        P_WARNING("failed to register item for btree node");
        return -1;
    }
#endif
    for (int i = 0; i < 0xFF; ++i)
    {
        if (m_types.is_valid_type(i))
        {
            if (SkipList::init_pool(&m_pool, m_types.types[i].payload_len) < 0)
            {
                P_WARNING("failed to register element size for invert type: %d", i);
                return -1;
            }
#ifndef __NOT_USE_COWBTREE__
            if (m_rpool.register_item(Btree::leaf_size(m_types.types[i].payload_len)) < 0)
            {
                P_WARNING("failed to register element size to btree for invert type: %d", i);
                return -1;
            }
#endif
        }
    }
#ifdef __NOT_USE_COWBTREE__
    if (m_node_pool.init(&m_pool) < 0)
    {
        P_WARNING("failed to init m_node_pool");
        return -1;
    }
#else
    if (m_btree_pool.init(&m_pool) < 0)
    {
        P_WARNING("failed to init m_btree_pool");
        return -1;
    }
#endif
    if (m_vnode_pool.init(&m_pool) < 0)
    {
        P_WARNING("failed to init m_vnode_pool");
        return -1;
    }
    if (m_snode_pool.init(&m_pool) < 0)
    {
        P_WARNING("failed to init m_snode_pool");
        return -1;
    }
    if (m_inode_pool.init(&m_pool) < 0)
    {
        P_WARNING("failed to init m_inode_pool");
        return -1;
    }
    if (m_skiplist_pool.init(&m_pool) < 0)
    {
        P_WARNING("failed to init m_skiplist_pool");
        return -1;
    }
    if (m_idlist_pool.init(&m_pool) < 0)
    {
        P_WARNING("failed to init m_idlist_pool");
        return -1;
    }
    uint32_t max_items_num;
    if (!parseUInt32(conf["max_items_num"], max_items_num))
    {
        P_WARNING("failed to get max_items_num");
        return -1;
    }
    if (m_pool.init(max_items_num) < 0)
    {
        P_WARNING("failed to init mempool");
        return -1;
    }
#ifndef __NOT_USE_COWBTREE__
    if (m_rpool.init(max_items_num) < 0)
    {
        P_WARNING("failed to init mempool");
        return -1;
    }
#endif
    if (!parseUInt32(conf["merge_threshold"], m_merge_threshold)
            || !parseUInt32(conf["merge_all_threshold"], m_merge_all_threshold)
            || !parseUInt32(conf["merge_speed"], m_merge_speed)
            || !parseUInt32(conf["merge_sleep"], m_merge_sleep))
    {
        P_WARNING("failed to get uint32 value for merge_threshold,"
                " merge_all_threshold, merge_speed, merge_sleep");
        return -1;
    }
    uint32_t signdict_hash_size;
    if (!parseUInt32(conf["signdict_hash_size"], signdict_hash_size))
    {
        P_WARNING("failed to get signdict_hash_size");
        return -1;
    }
    uint32_t signdict_buffer_size;
    if (!parseUInt32(conf["signdict_buffer_size"], signdict_buffer_size))
    {
        P_WARNING("failed to get signdict_buffer_size");
        return -1;
    }
    if (m_sign2id.init(&m_snode_pool, signdict_hash_size, signdict_buffer_size) < 0)
    {
        P_WARNING("failed to init sign2id dict");
        return -1;
    }
    uint32_t dict_hash_size;
    if (!parseUInt32(conf["dict_hash_size"], dict_hash_size))
    {
        P_WARNING("failed to get dict_hash_size");
        return -1;
    }
    m_dict = new Hash(dict_hash_size);
    if (NULL == m_dict)
    {
        P_WARNING("failed to new m_dict");
        return -1;
    }
#ifdef __NOT_USE_COWBTREE__
    m_dict->set_pool(&m_node_pool);
#else
    m_dict->set_pool(&m_vnode_pool);
#endif
    m_dict->set_cleanup(cleanup_node, (intptr_t)this);
    uint32_t add_dict_hash_size;
    if (!parseUInt32(conf["add_dict_hash_size"], add_dict_hash_size))
    {
        P_WARNING("failed to get add_dict_hash_size");
        return -1;
    }
    m_add_dict = new VHash(add_dict_hash_size);
    if (NULL == m_add_dict)
    {
        P_WARNING("failed to new m_add_dict");
        return -1;
    }
    m_add_dict->set_pool(&m_vnode_pool);
    m_add_dict->set_cleanup(cleanup_diff_node, (intptr_t)this);
    uint32_t del_dict_hash_size;
    if (!parseUInt32(conf["del_dict_hash_size"], del_dict_hash_size))
    {
        P_WARNING("failed to get del_dict_hash_size");
        return -1;
    }
    m_del_dict = new VHash(del_dict_hash_size);
    if (NULL == m_del_dict)
    {
        P_WARNING("failed to new m_del_dict");
        return -1;
    }
    m_del_dict->set_pool(&m_vnode_pool);
    m_del_dict->set_cleanup(cleanup_diff_node, (intptr_t)this);
    uint32_t words_bag_hash_size;
    if (!parseUInt32(conf["words_bag_hash_size"], words_bag_hash_size))
    {
        P_WARNING("failed to get words_bag_hash_size");
        return -1;
    }
    m_words_bag = new VHash(words_bag_hash_size);
    if (NULL == m_words_bag)
    {
        P_WARNING("failed to new m_words_bag");
        return -1;
    }
    m_words_bag->set_pool(&m_vnode_pool);
    m_words_bag->set_cleanup(cleanup_id_node, (intptr_t)this);

    m_types.set_sign_dict(&m_sign2id);

    P_WARNING("merge_threshold=%u", m_merge_threshold);
    P_WARNING("merge_all_threshold=%u", m_merge_all_threshold);
    P_WARNING("merge_speed=%u", m_merge_speed);
    P_WARNING("merge_sleep=%u", m_merge_sleep);
    P_WARNING("signdict_hash_size=%u", signdict_hash_size);
    P_WARNING("signdict_buffer_size=%u", signdict_buffer_size);
    P_WARNING("dict_hash_size=%u", dict_hash_size);
    P_WARNING("add_dict_hash_size=%u", add_dict_hash_size);
    P_WARNING("del_dict_hash_size=%u", del_dict_hash_size);
    P_WARNING("words_bag_hash_size=%u", words_bag_hash_size);
    P_WARNING("init ok");
    return 0;
}

DocList *InvertIndex::trigger(const char *keystr, uint8_t type) const
{
    if (NULL == keystr || !m_types.is_valid_type(type))
    {
        P_WARNING("invalid parameter");
        return NULL;
    }
    uint32_t sign = m_types.get_sign(keystr, type);
    return this->trigger(sign);
}

#if (0)

DocList *InvertIndex::trigger(uint32_t sign) const
{
#ifdef __NOT_USE_COWBTREE__
    void **big = m_dict->find(sign);
#else
    vaddr_t *vbig = m_dict->find(sign);
    Btree *big = NULL;
    if (vbig)
    {
        big = m_btree_pool.addr(*vbig);
    }
#endif
    vaddr_t *vadd = m_add_dict->find(sign);
    SkipList *add = NULL;
    if (NULL != vadd)
    {
        add = m_skiplist_pool.addr(*vadd);
    }
    if (NULL == big && NULL == add)
    {
        return NULL;
    }
    vaddr_t *vdel = m_del_dict->find(sign);
    SkipList *del = NULL;
    if (NULL != vdel)
    {
        del = m_skiplist_pool.addr(*vdel);
    }

#ifdef __NOT_USE_COWBTREE__
    BigList *bl = NULL;
    if (big)
    {
        bl = new(std::nothrow) BigList(sign, *big);
        if (NULL == bl)
        {
            P_WARNING("failed to new BigList");
            return NULL;
        }
    }
#else
    CowBtreeList<Btree> *bl = NULL;
    if (big)
    {
        bl = new(std::nothrow) CowBtreeList<Btree>(sign, big->begin(false));
        if (NULL == bl)
        {
            P_WARNING("failed to new CowBtreeList");
            return NULL;
        }
    }
#endif
    AddListImpl *al = NULL;
    if (add)
    {
        al = new(std::nothrow) AddListImpl(sign, add->begin());
        if (NULL == al)
        {
            P_WARNING("failed to new AddListImpl");
            if (bl)
            {
                delete bl;
            }
            return NULL;
        }
    }
    DeleteListImpl *dl = NULL;
    if (del)
    {
        dl = new(std::nothrow) DeleteListImpl(del->begin());
        if (NULL == dl)
        {
            P_WARNING("failed to new DeleteListImpl");
            if (bl)
            {
                delete bl;
            }
            if (al)
            {
                delete al;
            }
            return NULL;
        }
    }
    MergeList *ml = new(std::nothrow) MergeList(bl, al, dl);
    if (ml)
    {
        return ml;
    }
    if (bl)
    {
        delete bl;
    }
    if (al)
    {
        delete al;
    }
    if (dl)
    {
        delete dl;
    }
    return NULL;
}

#else

DocList *InvertIndex::trigger(uint32_t sign) const
{
    vaddr_t *vbig = m_dict->find(sign);
    Btree *big = NULL;
    if (vbig)
    {
        big = m_btree_pool.addr(*vbig);
    }
    vaddr_t *vadd = m_add_dict->find(sign);
    SkipList *add = NULL;
    if (NULL != vadd)
    {
        add = m_skiplist_pool.addr(*vadd);
    }
    if (NULL == big && NULL == add)
    {
        return NULL;
    }
    vaddr_t *vdel = m_del_dict->find(sign);
    SkipList *del = NULL;
    if (NULL != vdel)
    {
        del = m_skiplist_pool.addr(*vdel);
        if (big && add)
        {
            return new(std::nothrow) TSMergeList<Btree, SkipList>(sign, *big, *add, *del);
        }
        if (big)
        {
            return new(std::nothrow) TSBigDiffList<Btree, SkipList>(sign, *big, *del);
        }
        return new(std::nothrow) TSAddDiffList<SkipList>(sign, *add, *del);
    }
    if (big && add)
    {
        return new(std::nothrow) TSOrList<Btree, SkipList>(sign, *big, *add);
    }
    if (big)
    {
        return new(std::nothrow) CowBtreeList<Btree>(sign, big->begin(false));
    }
    return new(std::nothrow) AddListImpl(sign, add->begin());
}
#endif

bool InvertIndex::get_signs_by_docid(int32_t docid, std::vector<uint32_t> &signs) const
{
    signs.clear();

    vaddr_t *vlist = m_words_bag->find(docid);
    if (NULL == vlist)
    {
        return false;
    }
    IDList *list = m_idlist_pool.addr(*vlist);
    signs.reserve(list->size());
    IDList::iterator it = list->begin();
    IDList::iterator end = list->end();
    while (it != end)
    {
        signs.push_back(*it);
        ++it;
    }
    return true;
}

bool InvertIndex::insert(const char *keystr, uint8_t type, int32_t docid, const cJSON *json)
{
    if (NULL == keystr)
    {
        P_WARNING("invalid parameter: keystr is NULL");
        return false;
    }
    if (!m_types.is_valid_type(type))
    {
        P_WARNING("invalid parameter: keystr[%s], type[%d] is unregisterd", keystr, int(type));
        return false;
    }
    if (m_types.types[type].payload_len > 0)
    {
        if (NULL == json)
        {
            P_WARNING("invert type[%d] must has payload", int(type));
            return false;
        }
        void *payload = m_types.types[type].parser->parse(json);
        if (NULL == payload)
        {
            P_WARNING("failed to parse payload for invert type[%d]", int(type));
            return false;
        }
        return this->insert(keystr, type, docid, payload);
    }
    else
    {
        return this->insert(keystr, type, docid, (void *)NULL);
    }
}

bool InvertIndex::insert(uint32_t sign, int32_t docid, void *payload, const char *keystr, uint8_t type)
{
    uint16_t payload_len = m_types.types[type].payload_len;
    SkipList *add_list = NULL;
    vaddr_t *vadd_list = m_add_dict->find(sign);
    if (vadd_list)
    {
        add_list = m_skiplist_pool.addr(*vadd_list);
    }
    if (add_list)
    {
        if (add_list->payload_len() != payload_len)
        {
            P_WARNING("conflicting hash value[%s:%d]", keystr, int(type));
            return false;
        }
        if (!add_list->insert(docid, payload))
        {
            P_WARNING("failed to insert docid[%d] for hash value[%s:%d]", docid, keystr, int(type));
            return false;
        }
        if (add_list->size() > m_merge_threshold)
        {
            this->merge(sign);
        }
    }
    else
    {
        vaddr_t vlist = m_skiplist_pool.alloc(&m_pool, type, payload_len);
        SkipList *tmp = m_skiplist_pool.addr(vlist);
        if (NULL == tmp)
        {
            P_WARNING("failed to alloc SkipList");
            return false;
        }
        if (!tmp->insert(docid, payload) || !m_add_dict->insert(sign, vlist))
        {
            m_skiplist_pool.free(vlist);
            P_WARNING("failed to insert docid[%d] for hash value[%s:%d]", docid, keystr, int(type));
            return false;
        }
    }
    vaddr_t *vdel_list = m_del_dict->find(sign);
    if (vdel_list)
    {
        SkipList *del_list = m_skiplist_pool.addr(*vdel_list);
        del_list->remove(docid);
        if (del_list->size() == 0)
        {
            m_del_dict->remove(sign);
        }
    }
    IDList *idlist = NULL;
    vaddr_t *vidlist = m_words_bag->find(docid);
    if (vidlist)
    {
        idlist = m_idlist_pool.addr(*vidlist);
    }
    if (idlist)
    {
        if (!idlist->insert(sign))
        {
            P_WARNING("failed to insert sign[%u] of hash value[%s:%d] for docid[%d]", sign, keystr, int(type), docid);
            return false;
        }
    }
    else
    {
        vaddr_t vlist = m_idlist_pool.alloc(&m_inode_pool);
        IDList *tmp = m_idlist_pool.addr(vlist);
        if (NULL == tmp)
        {
            P_WARNING("failed to alloc IDList");
            return false;
        }
        if (!tmp->insert(sign) || !m_words_bag->insert(docid, vlist))
        {
            m_idlist_pool.free(vlist);
            P_WARNING("failed to insert sign[%u] of hash value[%s:%d] for docid[%d]", sign, keystr, int(type), docid);
            return false;
        }
    }
    return true;
}

bool InvertIndex::remove(const char *keystr, uint8_t type, int32_t docid)
{
    uint32_t sign = m_types.record_sign(keystr, type);
    SkipList *del_list = NULL;
    vaddr_t *vdel_list = m_del_dict->find(sign);
    if (vdel_list)
    {
        del_list = m_skiplist_pool.addr(*vdel_list);
    }
    if (del_list)
    {
        if (!del_list->insert(docid, NULL))
        {
            P_WARNING("failed to remove docid[%d] for hash value[%s:%d]", docid, keystr, int(type));
            return false;
        }
        if (del_list->size() > m_merge_threshold)
        {
            this->merge(sign);
        }
    }
    else
    {
        vaddr_t vlist = m_skiplist_pool.alloc(&m_pool, 0xFF, 0);
        SkipList *tmp = m_skiplist_pool.addr(vlist);
        if (NULL == tmp)
        {
            P_WARNING("failed to alloc SkipList");
            return false;
        }
        if (!tmp->insert(docid, NULL) || !m_del_dict->insert(sign, vlist))
        {
            m_skiplist_pool.free(vlist);
            P_WARNING("failed to remove docid[%d] for hash value[%s:%d]", docid, keystr, int(type));
            return false;
        }
    }
    vaddr_t *vadd_list = m_add_dict->find(sign);
    if (vadd_list)
    {
        SkipList *add_list = m_skiplist_pool.addr(*vadd_list);
        add_list->remove(docid);
        if (add_list->size() == 0)
        {
            m_add_dict->remove(sign);
        }
    }
    vaddr_t *vidlist = m_words_bag->find(docid);
    if (vidlist)
    {
        IDList *idlist = m_idlist_pool.addr(*vidlist);
        idlist->remove(sign);
        if (idlist->size() == 0)
        {
            m_words_bag->remove(docid);
        }
    }
    return true;
}

bool InvertIndex::remove(int32_t docid)
{
    vaddr_t *vidlist = m_words_bag->find(docid);
    if (NULL == vidlist)
    {
        return true;
    }
    IDList *idlist = m_idlist_pool.addr(*vidlist);
    IDList::iterator it = idlist->begin();
    IDList::iterator end = idlist->end();
    while (it != end)
    {
        uint32_t sign = *it;
        SkipList *del_list = NULL;
        vaddr_t *vdel_list = m_del_dict->find(sign);
        if (vdel_list)
        {
            del_list = m_skiplist_pool.addr(*vdel_list);
        }
        if (del_list)
        {
            if (!del_list->insert(docid, NULL))
            {
                P_WARNING("failed to remove docid[%d] for hash value[%lu]", docid, sign);
                return false;
            }
            if (del_list->size() > m_merge_threshold)
            {
                this->merge(sign);
            }
        }
        else
        {
            vaddr_t vlist = m_skiplist_pool.alloc(&m_pool, 0xFF, 0);
            SkipList *tmp = m_skiplist_pool.addr(vlist);
            if (NULL == tmp)
            {
                P_WARNING("failed to alloc SkipList");
                return false;
            }
            if (!tmp->insert(docid, NULL) || !m_del_dict->insert(sign, vlist))
            {
                m_skiplist_pool.free(vlist);
                P_WARNING("failed to remove docid[%d] for hash value[%lu]", docid, sign);
                return false;
            }
        }
        vaddr_t *vadd_list = m_add_dict->find(sign);
        if (vadd_list)
        {
            SkipList *add_list = m_skiplist_pool.addr(*vadd_list);
            add_list->remove(docid);
            if (add_list->size() == 0)
            {
                m_add_dict->remove(sign);
            }
        }
        ++it;
    }
    m_words_bag->remove(docid);
    return true;
}

bool InvertIndex::update_docid(int32_t from, int32_t to)
{
    if (from == to) /* not changed */
    {
        return true;
    }
    vaddr_t *vidlist = m_words_bag->find(from);
    if (NULL == vidlist) /* from doesn't have words */
    {
        return true;
    }
    IDList *idlist = m_idlist_pool.addr(*vidlist);
    IDList::iterator it = idlist->begin();
    IDList::iterator end = idlist->end();
    DummyStrategy dummy;
    while (it != end)
    {
        bool ok = false;
        DocList *list = this->trigger(*it);
        if (list)
        {
            list->first();
            const int32_t id = list->find(from);
            if (id == from) /* matched */
            {
                InvertStrategy::info_t *info = list->get_strategy_data(dummy);
                if (!this->insert(*it, to, info->result, "update docid", info->type))
                {
                    P_WARNING("failed to update sign[%u] from[%d] to[%d]", *it, from, to);
                    return false;
                }
                ok = true;
            }
            delete list;
        }
        if (!ok)
        {
            P_FATAL("index has been corrupted");
        }
        ++it;
    }
    return this->remove(from);
}

uint32_t InvertIndex::merge(uint32_t sign)
{
    long us = 0;
    uint32_t docnum = 0;
    FastTimer timer;

    timer.start();
    std::string word;
    m_sign2id.find(sign, word);
    DocList *list = this->trigger(sign);
    if (list)
    {
        DummyStrategy st;

        uint8_t type = 0xFF;
        uint16_t payload_len = 0;
        int32_t docid = list->first();
#ifdef __NOT_USE_COWBTREE__
        while (docid != -1)
        {
            if (0 == docnum)
            {
                InvertStrategy::info_t *info = list->get_strategy_data(st);
                type = info->type;
                payload_len = info->length;
            }
            ++docnum;
            docid = list->next();
        }
        if (docnum > 0)
        {
            void *mem = ::malloc(sizeof(bl_head_t) + sizeof(int32_t)*docnum + payload_len*docnum);
            if (mem)
            {
                bl_head_t *head = (bl_head_t *)mem;
                head->type = type;
                head->payload_len = payload_len;
                head->doc_num = docnum;

                int32_t *docids = (int32_t *)(head + 1);
                void *payload = docids + docnum;

                docid = list->first();
                while (docid != -1)
                {
                    *docids++ = docid;
                    if (payload_len > 0)
                    {
                        ::memcpy(payload, list->get_strategy_data(st)->result, payload_len);
                        payload = ((int8_t *)payload) + payload_len;
                    }
                    docid = list->next();
                }
                if (m_dict->insert(sign, mem))
                {
                    m_add_dict->remove(sign);
                    m_del_dict->remove(sign);

                    timer.stop();
                    us = timer.timeInUs();
                    P_WARNING("merge sign[%u] ok, type[%d], word[%s], list len=%u, cost %ld us",
                            sign, int(type), word.c_str(), docnum, us);
                }
                else
                {
                    ::free(mem);
                    P_WARNING("failed to merge sign[%u], type[%d], word[%s]", sign, int(type), word.c_str());
                }
            }
            else
            {
                P_WARNING("failed to alloc mem[%d]", (sizeof(bl_head_t) + sizeof(int32_t)*docnum + payload_len*docnum));
            }
        }
#else
        if (docid != -1)
        {
            InvertStrategy::info_t *info = list->get_strategy_data(st);
            type = info->type;
            payload_len = info->length;

            bool merge2btree = false;
            while (docid != -1)
            {
                if (++docnum > (Btree::N_WIDE >> 1))
                {
                    merge2btree = true;
                    break;
                }
                docid = list->next();
            }
            char *payloads = NULL;
            do
            {
                if (merge2btree)
                {
                    vaddr_t *vbig = m_dict->find(sign);
                    if (NULL == vbig)
                    {
                        vaddr_t new_big = m_btree_pool.alloc<RPool *, uint8_t, uint16_t>
                            (&m_rpool, type, payload_len);
                        if (0 == new_big)
                        {
                            P_WARNING("failed to alloc btree, sign[%u], type[%d], word[%s]",
                                    sign, int(type), word.c_str());
                            break;
                        }
                        if (!m_dict->insert(sign, new_big))
                        {
                            m_btree_pool.free(new_big);

                            P_WARNING("failed to insert btree, sign[%u], type[%d], word[%s]",
                                    sign, int(type), word.c_str());
                            break;
                        }
                        vbig = m_dict->find(sign);
                    }
                    Btree *big = m_btree_pool.addr(*vbig);
                    if (!big->init_for_modify())
                    {
                        if (0 == big->size()) /* empty check */
                        {
                            m_dict->remove(sign);
                        }
                        P_WARNING("failed to call init_for_modify, sign[%u], type[%d], word[%s]",
                                sign, int(type), word.c_str());
                        break;
                    }
                    int del_num = 0;
                    int add_num = 0;
                    vaddr_t *vdel = m_del_dict->find(sign);
                    if (NULL != vdel)
                    {
                        SkipList *del = m_skiplist_pool.addr(*vdel);
                        SkipList::iterator it = del->begin();
                        SkipList::iterator end = del->end();
                        while (it != end)
                        {
                            big->remove(*it);
                            ++it;
                            ++del_num;
                        }
                    }
                    vaddr_t *vadd = m_add_dict->find(sign);
                    if (NULL != vadd)
                    {
                        SkipList *add = m_skiplist_pool.addr(*vadd);
                        SkipList::iterator it = add->begin();
                        SkipList::iterator end = add->end();
                        if (0 == payload_len)
                        {
                            while (it != end)
                            {
                                big->insert(*it, NULL);
                                ++it;
                                ++add_num;
                            }
                        }
                        else
                        {
                            while (it != end)
                            {
                                big->insert(*it, it.payload());
                                ++it;
                                ++add_num;
                            }
                        }
                    }
                    if (!big->end_for_modify())
                    {
                        if (0 == big->size()) /* empty check */
                        {
                            m_dict->remove(sign);
                        }
                        P_WARNING("failed to call end_for_modify, sign[%u], type[%d], word[%s]",
                                sign, int(type), word.c_str());
                        break;
                    }
                    m_add_dict->remove(sign);
                    m_del_dict->remove(sign);

                    docnum = big->size();

                    timer.stop();
                    us = timer.timeInUs();
                    P_WARNING("merge sign[%u] to btree ok, type[%d], word[%s], list len=%u, "
                            "op num=%d, del num=%d, add num=%d, cost %ld us",
                            sign, int(type), word.c_str(), docnum,
                            (del_num + add_num), del_num, add_num, us);
                }
                else
                {
                    uint32_t i = 0;
                    int32_t docids[Btree::N_WIDE >> 1];
                    if (payload_len > 0)
                    {
                        payloads = new(std::nothrow) char[payload_len * docnum];
                        if (NULL == payloads)
                        {
                            P_WARNING("failed to new payloads, docnum=%u, payload_len=%d", docnum, int(payload_len));
                            break;
                        }
                        docid = list->first();
                        while (docid != -1)
                        {
                            docids[i] = docid;

                            info = list->get_strategy_data(st);
                            ::memcpy(payloads + payload_len * i, info->result, payload_len);

                            docid = list->next();
                            ++i;
                        }
                    }
                    else
                    {
                        docid = list->first();
                        while (docid != -1)
                        {
                            docids[i++] = docid;
                            docid = list->next();
                        }
                    }
                    if (i != docnum)
                    {
                        P_FATAL("i[%d] != docnum[%u]", i, docnum);
                        break;
                    }
                    SkipList *add = NULL;
                    vaddr_t *vadd = m_add_dict->find(sign);
                    if (NULL != vadd)
                    {
                        add = m_skiplist_pool.addr(*vadd);
                    }
                    else
                    {
                        vaddr_t vlist = m_skiplist_pool.alloc(&m_pool, type, payload_len);
                        add = m_skiplist_pool.addr(vlist);
                        if (NULL == add)
                        {
                            P_WARNING("failed to alloc SkipList");
                            break;
                        }
                        if (!m_add_dict->insert(sign, vlist))
                        {
                            m_skiplist_pool.free(vlist);

                            P_WARNING("failed to insert SkipList, sign[%u], type[%d], word[%s]",
                                    sign, int(type), word.c_str());
                            break;
                        }
                    }
                    if (payload_len > 0)
                    {
                        for (i = 0; i < docnum; ++i)
                        {
                            if (!add->insert(docids[i], payloads + i * payload_len))
                            {
                                break;
                            }
                        }
                    }
                    else
                    {
                        for (i = 0; i < docnum; ++i)
                        {
                            if (!add->insert(docids[i], NULL))
                            {
                                break;
                            }
                        }
                    }
                    if (i != docnum)
                    {
                        /* partial success is ok, need not to rollback */
                        P_WARNING("i[%d] != docnum[%u], insert to SkipList failed", i, docnum);
                        break;
                    }
                    m_dict->remove(sign);
                    m_del_dict->remove(sign);

                    if (add->size() != docnum)
                    {
                        P_FATAL("add::size[%d] != docnum[%u]", int(add->size()), docnum);
                    }
                    timer.stop();
                    us = timer.timeInUs();
                    P_TRACE("merge sign[%u] to skiplist ok, type[%d], word[%s], list len=%u, cost %ld us",
                            sign, int(type), word.c_str(), docnum, us);
                }
            } while (0);
            if (payloads)
            {
                delete [] payloads;
            }
        }
#endif
        else
        {
            m_dict->remove(sign);
            m_add_dict->remove(sign);
            m_del_dict->remove(sign);

            timer.stop();
            us = timer.timeInUs();
            P_WARNING("delete sign[%u] ok, word[%s], list len=%d, cost %ld us", sign, word.c_str(), docnum, us);
        }
        delete list;
    }
    else
    {
        m_del_dict->remove(sign);

        timer.stop();
        us = timer.timeInUs();
        P_WARNING("delete sign[%u] ok, word[%s], cost %ld us", sign, word.c_str(), us);
    }
    return docnum;
}

void InvertIndex::cleanup_node(Hash::node_t *node, intptr_t arg)
{
    InvertIndex *ptr = (InvertIndex *)arg;
    if (NULL == ptr)
    {
        P_FATAL("should not run to here");
        ::abort();
    }
    if (node->value)
    {
#ifdef __NOT_USE_COWBTREE__
        ::free(node->value);
#else
        ptr->m_btree_pool.free(node->value);
#endif
    }
}

void InvertIndex::cleanup_diff_node(VHash::node_t *node, intptr_t arg)
{
    InvertIndex *ptr = (InvertIndex *)arg;
    if (NULL == ptr)
    {
        P_FATAL("should not run to here");
        ::abort();
    }
    if (node->value)
    {
        ptr->m_skiplist_pool.free(node->value);
    }
}

void InvertIndex::cleanup_id_node(VHash::node_t *node, intptr_t arg)
{
    InvertIndex *ptr = (InvertIndex *)arg;
    if (NULL == ptr)
    {
        P_FATAL("should not run to here");
        ::abort();
    }
    if (node->value)
    {
        ptr->m_idlist_pool.free(node->value);
    }
}

DocList *InvertIndex::parse(const std::string &query, const std::vector<term_t> &terms) const
{
    if(0 == query.length())
    {
        P_TRACE("query is empty when parse term query");
        return NULL;
    }
    std::vector<std::string> tokens;
    if(::infix2postfix(query, tokens) != 0)
    {
        P_WARNING("fail to parse query: %s", query.c_str());
        return NULL;
    }
    DocList *left = NULL;
    DocList *right = NULL;
    DocList *temp_result = NULL;
    InvertStrategy::data_t data; data.i64 = 0;
    std::stack<DocList *> doclist_stack;
    for (size_t i = 0; i < tokens.size(); ++i)
    {
        std::string &token = tokens[i];
        switch (token.c_str()[0])
        {
            case '&':
            case '|':
            case '-':
                {
                    if (doclist_stack.size() < 2)
                    {
                        P_WARNING("invalid post expression, query[%s]", query.c_str());
                        goto FAIL;
                    }
                    right = doclist_stack.top();
                    doclist_stack.pop();
                    left = doclist_stack.top();
                    doclist_stack.pop();
                    if (token == "&")
                    {
                        if (left && right)
                        {
                            temp_result = new (std::nothrow) AndList(left, right);
                            if (NULL == temp_result)
                            {
                                delete left;
                                delete right;
                                P_WARNING("failed to new andlist");
                                goto FAIL;
                            }
                        }
                        else
                        {
                            if (left) delete left;
                            if (right) delete right;
                            temp_result = NULL;
                        }
                    }
                    else if (token == "|")
                    {
                        if (left && right)
                        {
                            temp_result = new (std::nothrow) OrList(left, right);
                            if (NULL == temp_result)
                            {
                                delete left;
                                delete right;
                                P_WARNING("failed to new orlist");
                                goto FAIL;
                            }
                        }
                        else if (left) temp_result = left;
                        else temp_result = right;
                    }
                    else if (token == "-")
                    {
                        if (left && right)
                        {
                            temp_result = new (std::nothrow) DiffList(left, right);
                            if (NULL == temp_result)
                            {
                                delete left;
                                delete right;
                                P_WARNING("failed to new difflist");
                                goto FAIL;
                            }
                        }
                        else
                        {
                            if (right) delete right;
                            temp_result = left;
                        }
                    }
                    else
                    {
                        P_WARNING("invalid post expression, query[%s]", query.c_str());
                        goto FAIL;
                    }
                    doclist_stack.push(temp_result);
                }
                break;
            default:
                {
                    uint32_t pos = ::atoi(token.c_str());
                    if (pos >= uint32_t(terms.size()))
                    {
                        P_WARNING("pos is: %u, but array size is: %d", pos, int(terms.size()));
                        goto FAIL;
                    }
                    temp_result = this->trigger(terms[pos].word.c_str(), terms[pos].type);
                    if (temp_result)
                    {
                        data.i32 = pos;
                        temp_result->set_data(data); /* 保存触发点偏移位置 */
                    }
                    doclist_stack.push(temp_result);
                }
                break;
        }
    }
    if (doclist_stack.size() != 1)
    {
        P_WARNING("not unique doclist result");
        goto FAIL;
    }
    return doclist_stack.top();
FAIL:
    while (doclist_stack.size() > 0)
    {
        DocList *tmp = doclist_stack.top();
        if (tmp) delete tmp;
        doclist_stack.pop();
    }
    return NULL;
}

void InvertIndex::adjust_node(node_t &node)
{
    switch (node.op)
    {
        case '*':
        case '&':
        case '|':
        case '-':
            if (node.children.size() != 2)
            {
                P_FATAL("node.children.size=%d", int(node.children.size()));
                return ;
            }
            {
                node_t &left = node.children[0];
                node_t &right = node.children[1];
                adjust_node(left);
                adjust_node(right);

                if ('*' == node.op || '-' == node.op)
                {
                    return ;
                }
                /* 针对&和|进行优化 */
                node_t tmp;
                tmp.op = node.op;
                if (left.op == node.op)
                {
                    tmp.children.insert(tmp.children.end(),
                            left.children.begin(), left.children.end());
                }
                else
                {
                    tmp.children.push_back(left);
                }
                if (right.op == node.op)
                {
                    tmp.children.insert(tmp.children.end(),
                            right.children.begin(), right.children.end());
                }
                else
                {
                    tmp.children.push_back(right);
                }
                node = tmp;
            }
            return ;
        case 'T':
            return ;
        default:
            P_FATAL("invalid op type=%d", int(node.op));
            return ;
    }
}

std::string InvertIndex::print_node(const node_t &node)
{
    std::string result;
    switch (node.op)
    {
        case '&':
        case '|':
            if (node.children.size() < 2)
            {
                P_FATAL("node.children.size=%d", int(node.children.size()));
                return "FATAL";
            }
            result += "(";
            for (size_t i = 0; i < node.children.size(); ++i)
            {
                if (i > 0)
                {
                    result += node.op;
                }
                result += print_node(node.children[i]);
            }
            result += ")";
            break;
        case '*':
        case '-':
            if (node.children.size() != 2)
            {
                P_FATAL("node.children.size=%d", int(node.children.size()));
                return "FATAL";
            }
            result += "(";
            result += print_node(node.children[0]);
            result += node.op;
            result += print_node(node.children[1]);
            result += ")";
            break;
        case 'T':
            {
                char tmp[256];
                ::snprintf(tmp, sizeof(tmp), "%u", node.pos);
                result = tmp;
            }
            break;
        default:
            P_FATAL("invalid op type=%d", int(node.op));
            return "FATAL";
    }
    return result;
}

DocList *InvertIndex::trigger(const node_t &node, const std::vector<term_t> &terms) const
{
    switch (node.op)
    {
        case '&':
        case '|':
            if (node.children.size() < 2)
            {
                P_FATAL("node.children.size=%d", int(node.children.size()));
                return NULL;
            }
            {
                std::vector<DocList *> children;
                for (size_t i = 0; i < node.children.size(); ++i)
                {
                    DocList *child = this->trigger(node.children[i], terms);
                    if (child)
                    {
                        children.push_back(child);
                    }
                    else if ('&' == node.op)
                    {
                        goto CLEANUP;
                    }
                }
                if (children.size() == 0)
                {
                    return NULL;
                }
                if (children.size() == 1)
                {
                    return children[0];
                }
                if ('&' == node.op)
                {
                    Conjunction *con = new (std::nothrow) Conjunction(children);
                    if (con)
                    {
                        return con;
                    }
                    P_FATAL("failed to new Conjunction");
                }
                else
                {
                    Disjunction *dis = new (std::nothrow) Disjunction(children);
                    if (dis)
                    {
                        return dis;
                    }
                    P_FATAL("failed to new Disjunction");
                }
CLEANUP:
                /* do clean up */
                for (size_t i = 0; i < children.size(); ++i)
                {
                    delete children[i];
                }
                return NULL;
            }
        case '*':
        case '-':
            if (node.children.size() != 2)
            {
                P_FATAL("node.children.size=%d", int(node.children.size()));
                return NULL;
            }
            {
                DocList *left = this->trigger(node.children[0], terms);
                if (NULL == left)
                {
                    return NULL;
                }
                DocList *right = this->trigger(node.children[1], terms);
                if (NULL == right)
                {
                    return left;
                }
                if ('*' == node.op)
                {
                    ReqOptList *reqopt = new (std::nothrow) ReqOptList(left, right);
                    if (reqopt)
                    {
                        return reqopt;
                    }
                    P_FATAL("failed to new ReqOptList");
                }
                else
                {
                    DiffList *diff = new (std::nothrow) DiffList(left, right);
                    if (diff)
                    {
                        return diff;
                    }
                    P_FATAL("failed to new DiffList");
                }
                /* do clean up */
                delete left;
                delete right;
                return NULL;
            }
        case 'T':
            {
                DocList *list = this->trigger(terms[node.pos].word.c_str(), terms[node.pos].type);
                if (list)
                {
                    InvertStrategy::data_t data;
                    data.i64 = 0;
                    data.i32 = node.pos;
                    list->set_data(data);
                }
                return list;
            }
        default:
            P_FATAL("invalid op type=%d", int(node.op));
            return NULL;
    }
}

DocList *InvertIndex::parse_hp(const std::string &query, const std::vector<term_t> &terms,
        std::string *new_query) const
{
    if(0 == query.length())
    {
        P_TRACE("query is empty when parse term query");
        return NULL;
    }
    std::vector<std::string> tokens;
    if(::infix2postfix(query, tokens) != 0)
    {
        P_WARNING("fail to parse query: %s", query.c_str());
        return NULL;
    }
    std::stack<node_t> node_stack;
    for (size_t i = 0; i < tokens.size(); ++i)
    {
        std::string &token = tokens[i];
        switch (token.c_str()[0])
        {
            case '*':
            case '&':
            case '|':
            case '-':
                {
                    if (node_stack.size() < 2)
                    {
                        P_WARNING("invalid post expression, query[%s]", query.c_str());
                        return NULL;
                    }
                    node_t node;
                    /* op */
                    node.op = token.c_str()[0]; /* '*', '&', '|', '-' */
                    node.children.resize(2);
                    /* right node */
                    node.children[1] = node_stack.top();
                    node_stack.pop();
                    /* left node */
                    node.children[0] = node_stack.top();
                    node_stack.pop();
                    /* save */
                    node_stack.push(node);
                }
                break;
            default:
                {
                    uint32_t pos = ::atoi(token.c_str());
                    if (pos >= uint32_t(terms.size()))
                    {
                        P_WARNING("pos is: %u, but array size is: %d", pos, int(terms.size()));
                        return NULL;
                    }
                    node_t node;
                    node.op = 'T';
                    node.pos = pos; /* 保存触发点偏移位置 */
                    node_stack.push(node);
                }
                break;
        }
    }
    if (node_stack.size() != 1)
    {
        P_WARNING("not unique doclist result");
        return NULL;
    }
    InvertIndex::adjust_node(node_stack.top());
    if (new_query)
    {
        *new_query = InvertIndex::print_node(node_stack.top());
    }
    return this->trigger(node_stack.top(), terms);
}

void InvertIndex::print_meta() const
{
    m_pool.print_meta();
#ifndef __NOT_USE_COWBTREE__
    m_rpool.print_meta();
#endif

    P_WARNING("m_dict:");
    P_WARNING("    size=%lu", (uint64_t)m_dict->size());
    P_WARNING("    mem=%lu", (uint64_t)m_dict->mem_used());
    {
        size_t mem[256];
        size_t count[256];
        for (int i = 0; i < 256; ++i)
        {
            mem[i] = 0;
            count[i] = 0;
        }

#ifdef __NOT_USE_COWBTREE__
        bl_head_t *ph;
        Hash::iterator it = m_dict->begin();
        while (it)
        {
            ph = (bl_head_t *)it.value();
            mem[ph->type] += sizeof(bl_head_t) + sizeof(int32_t) * ph->doc_num + ph->payload_len * ph->doc_num;
            count[ph->type] += ph->doc_num;
            ++it;
        }
#else
        Hash::iterator it = m_dict->begin();
        while (it)
        {
            Btree *big = m_btree_pool.addr(it.value());
            mem[big->type()] += sizeof(*big) + big->count_nodes(false);
            count[big->type()] += big->size();
            ++it;
        }
#endif

        size_t total_mem = 0;
        size_t total_count = 0;
        for (int i = 0; i < 256; ++i)
        {
            if (count[i] > 0)
            {
                total_mem += mem[i];
                total_count += count[i];
                P_WARNING("    type[%d]: num=%lu, mem=%lu", i, (uint64_t)count[i], (uint64_t)mem[i]);
            }
        }
        P_WARNING("    total_mem=%lu", (uint64_t)total_mem);
        P_WARNING("    total_count=%lu", (uint64_t)total_count);
    }

    P_WARNING("m_add_dict:");
    P_WARNING("    size=%lu", (uint64_t)m_add_dict->size());
    P_WARNING("    mem=%lu", (uint64_t)m_add_dict->mem_used());
    {
        size_t total_mem = 0;
        size_t total_count = 0;

        SkipList *list;
        VHash::iterator it = m_add_dict->begin();
        while (it)
        {
            list = m_skiplist_pool.addr(it.value());
            total_mem += list->mem_used();
            total_count += list->size();
            ++it;
        }
        P_WARNING("    total_mem=%lu", (uint64_t)total_mem);
        P_WARNING("    total_count=%lu", (uint64_t)total_count);
    }

    P_WARNING("m_del_dict:");
    P_WARNING("    size=%lu", (uint64_t)m_del_dict->size());
    P_WARNING("    mem=%lu", (uint64_t)m_del_dict->mem_used());
    {
        size_t total_mem = 0;
        size_t total_count = 0;

        SkipList *list;
        VHash::iterator it = m_del_dict->begin();
        while (it)
        {
            list = m_skiplist_pool.addr(it.value());
            total_mem += list->mem_used();
            total_count += list->size();
            ++it;
        }
        P_WARNING("    total_mem=%lu", (uint64_t)total_mem);
        P_WARNING("    total_count=%lu", (uint64_t)total_count);
    }

    P_WARNING("m_words_bag:");
    P_WARNING("    size=%lu", (uint64_t)m_words_bag->size());
    P_WARNING("    mem=%lu", (uint64_t)m_words_bag->mem_used());
    {
        size_t total_mem = 0;
        size_t total_count = 0;

        IDList *list;
        VHash::iterator it = m_words_bag->begin();
        while (it)
        {
            list = m_idlist_pool.addr(it.value());
            total_mem += list->mem_used();
            total_count += list->size();
            ++it;
        }
        P_WARNING("    total_mem=%lu", (uint64_t)total_mem);
        P_WARNING("    total_count=%lu", (uint64_t)total_count);
    }

    m_sign2id.print_meta();
}

struct sign_num_t
{
    uint32_t sign;
    int32_t docnum;

    sign_num_t(uint32_t s, int32_t n): sign(s), docnum(n) { }

    bool operator < (const sign_num_t &o) const
    {
        if (docnum == o.docnum)
        {
            return sign < o.sign;
        }
        else if (docnum > o.docnum)
        {
            return true;
        }
        else
        {
            return false;
        }
    }
};

void InvertIndex::print_list_length(const char *filename) const
{
    FILE *fp = NULL;
    if (filename)
    {
        fp = ::fopen(filename, "w");
        if (NULL == fp)
        {
            P_WARNING("failed to open file[%s]", filename);
        }
    }
    size_t total_len = 0;
    std::vector<sign_num_t> signs;
    for (uint32_t i = 1; i <= m_sign2id.idnum(); ++i)
    {
        DocList *doc = this->trigger(i);
        if (doc)
        {
            int32_t doc_num = 0;
            int32_t docid = doc->first();
            while (docid != -1)
            {
                docid = doc->next();
                ++doc_num;
            }
            if (doc_num > 0)
            {
                signs.push_back(sign_num_t(i, doc_num));
            }
            total_len += doc_num;
            delete doc;
        }
    }
    if (fp)
    {
        ::fprintf(fp, "total word count=%lu, total invert length=%lu\n", (uint64_t)signs.size(), (uint64_t)total_len);
    }
    else
    {
        P_WARNING("total word count=%lu, total invert length=%lu", (uint64_t)signs.size(), (uint64_t)total_len);
    }

    std::sort(signs.begin(), signs.end());
    std::string word;
    for (size_t i = 0; i < signs.size(); ++i)
    {
        m_sign2id.find(signs[i].sign, word);
        if (fp)
        {
            ::fprintf(fp, "word[%s], length=%d\n", word.c_str(), signs[i].docnum);
        }
        else
        {
            P_WARNING("word[%s], length=%d", word.c_str(), signs[i].docnum);
        }
    }
    if (fp)
    {
        ::fclose(fp);
    }
}

void InvertIndex::mergeAll(uint32_t length)
{
#ifndef __NOT_USE_COWBTREE__
    length = 0;
#endif
    P_WARNING("start to merge all signs whose length > %u", length);

    size_t len;
    FastTimer timer;
    std::vector<uint32_t> signs;

#ifndef __NOT_USE_COWBTREE__
    timer.start();
    {
        signs.reserve(m_dict->size());
        Hash::iterator it = m_dict->begin();
        while (it)
        {
            signs.push_back(it.key());
            ++it;
        }
    }
    timer.stop();
    P_WARNING("dict signs, size=%u, time=%ld ms", (uint32_t)signs.size(), timer.timeInMs());

    timer.start();
    len = 0;
    for (size_t i = 0; i < signs.size(); ++i)
    {
        len += this->merge(signs[i]);
    }
    timer.stop();
    P_WARNING("merge signs ok, all length=%lu, time=%ld ms", (uint64_t)len, timer.timeInMs());
#endif

    timer.start();
    signs.clear();
    {
        signs.reserve(m_add_dict->size());
        VHash::iterator it = m_add_dict->begin();
        while (it)
        {
            if (m_skiplist_pool.addr(it.value())->size() > length)
            {
                signs.push_back(it.key());
            }
            ++it;
        }
    }
    timer.stop();
    P_WARNING("add signs whose length > %u, size=%u, time=%ld ms",
            length, (uint32_t)signs.size(), timer.timeInMs());

    timer.start();
    len = 0;
    size_t last_len = 0;
    uint32_t last_recycle_time = g_now_time;
    for (size_t i = 0; i < signs.size(); ++i)
    {
        if (last_len + m_merge_speed < len)
        {
            ::usleep(m_merge_sleep * 1000);     /* just sleep a while */
        }
        if (g_now_time != last_recycle_time)
        {
            this->recycle();
            last_recycle_time = g_now_time;
            last_len = len;
        }
        len += this->merge(signs[i]);
    }
    timer.stop();
    P_WARNING("merge add signs ok, all length=%lu, time=%ld ms", (uint64_t)len, timer.timeInMs());

    timer.start();
    signs.clear();
    {
        signs.reserve(m_del_dict->size());
        VHash::iterator it = m_del_dict->begin();
        while (it)
        {
            if (m_skiplist_pool.addr(it.value())->size() > length)
            {
                signs.push_back(it.key());
            }
            ++it;
        }
    }
    timer.stop();
    P_WARNING("del signs whose length > %u, size=%u, time=%ld ms",
            length, (uint32_t)signs.size(), timer.timeInMs());

    timer.start();
    len = 0;
    for (size_t i = 0; i < signs.size(); ++i)
    {
        len += this->merge(signs[i]);
    }
    timer.stop();
    P_WARNING("merge del signs ok, all length=%lu, time=%ld ms", (uint64_t)len, timer.timeInMs());
}

bool InvertIndex::dump(const char *dir, FSInterface *fs)
{
    typedef FSInterface::File File;
    if (NULL == fs)
    {
        fs = &DefaultFS::s_default;
    }

    if (NULL == dir || '\0' == *dir)
    {
        P_WARNING("empty dir error");
        return false;
    }
    P_WARNING("start to write dir[%s]", dir);
    uint32_t key;
    std::string path(dir);
    if ('/' != path[path.length() - 1])
    {
        path += "/";
    }
    {
        File meta = fs->fopen((path + "invert.meta").c_str(), "w");
        if (NULL == meta)
        {
            P_WARNING("failed to open file[%sinvert.meta] for write", path.c_str());
            return false;
        }
#ifndef __NOT_USE_COWBTREE__
        fs->fprintf(meta, "cowbtree: on\n\n");
#endif
        fs->fprintf(meta, "%s", m_types.m_meta.c_str());
        fs->fclose(meta);
    }
    this->mergeAll(m_merge_all_threshold);
    {
        File idx = fs->fopen((path + "invert.idx").c_str(), "wb");
        if (NULL == idx)
        {
            P_WARNING("failed to open file[%sinvert.idx] for write", path.c_str());
            return false;
        }
        File data = fs->fopen((path + "invert.data").c_str(), "wb");
        if (NULL == data)
        {
            fs->fclose(idx);

            P_WARNING("failed to open file[%sinvert.data] for write", path.c_str());
            return false;
        }
        File meta = fs->fopen((path + "invert_list.meta").c_str(), "w");
        if (NULL == meta)
        {
            fs->fclose(data);
            fs->fclose(idx);

            P_WARNING("failed to open file[%sinvert_list.meta] for write", path.c_str());
            return false;
        }
        std::string word;
        size_t total_len = 0;
        size_t offset = 0;
        Hash::iterator it = m_dict->begin();
        while (it)
        {
#ifdef __NOT_USE_COWBTREE__
            bl_head_t *pl = (bl_head_t *)it.value();
            const uint32_t length = (sizeof(bl_head_t) + sizeof(int32_t) * pl->doc_num + pl->payload_len * pl->doc_num);
            if (fs->fwrite(pl, length, 1, data) != 1)
            {
                P_WARNING("failed to write data, length=%u, offset=%lu", length, (uint64_t)offset);
                goto FAIL0;
            }
            const int doc_num = pl->doc_num;
#else
            Btree *big = m_btree_pool.addr(it.value());

            bl_head_t head;
            head.type = big->type();
            head.payload_len = big->payload_len();
            head.doc_num = big->size();

            int32_t docid;
            const uint32_t length = (sizeof(bl_head_t) + (sizeof(int32_t) + head.payload_len) * head.doc_num);
            const int doc_num = head.doc_num;
            Btree::iterator bt(big->begin(true));

            if (fs->fwrite(&head, sizeof(head), 1, data) != 1)
            {
                P_WARNING("failed to write bl_head_t");
                goto FAIL0;
            }
            if (head.payload_len > 0)
            {
                while (*bt != -1)
                {
                    docid = *bt;
                    if (fs->fwrite(&docid, sizeof(docid), 1, data) != 1)
                    {
                        P_WARNING("failed to write docid");
                        goto FAIL0;
                    }
                    if (fs->fwrite(bt.payload(), head.payload_len, 1, data) != 1)
                    {
                        P_WARNING("failed to write payload");
                        goto FAIL0;
                    }
                    ++bt;
                }
            }
            else
            {
                while (*bt != -1)
                {
                    docid = *bt;
                    if (fs->fwrite(&docid, sizeof(docid), 1, data) != 1)
                    {
                        P_WARNING("failed to write docid");
                        goto FAIL0;
                    }
                    ++bt;
                }
            }
#endif
            key = it.key();
            if (fs->fwrite(&key, sizeof(key), 1, idx) != 1)
            {
                P_WARNING("failed to write key to idx");
                goto FAIL0;
            }
            if (fs->fwrite(&offset, sizeof(offset), 1, idx) != 1)
            {
                P_WARNING("failed to write offset to idx");
                goto FAIL0;
            }
            if (fs->fwrite(&length, sizeof(length), 1, idx) != 1)
            {
                P_WARNING("failed to write length to idx");
                goto FAIL0;
            }
            if (0)
            {
FAIL0:
                fs->fclose(meta);
                fs->fclose(data);
                fs->fclose(idx);
                return false;
            }

            m_sign2id.find(key, word);
            fs->fprintf(meta, "%s : %d\n", word.c_str(), doc_num);

            offset += length;
            total_len += doc_num;
            ++it;
        }
        fs->fprintf(meta, "total_len : %lu\n", (uint64_t)total_len);

        fs->fclose(meta);
        fs->fclose(data);
        fs->fclose(idx);
        P_WARNING("write invert index ok");
    }
    {
        File idx = fs->fopen((path + "add.idx").c_str(), "wb");
        if (NULL == idx)
        {
            P_WARNING("failed to open file[%sadd.idx] for write", path.c_str());
            return false;
        }
        File data = fs->fopen((path + "add.data").c_str(), "wb");
        if (NULL == data)
        {
            fs->fclose(idx);

            P_WARNING("failed to open file[%sadd.data] for write", path.c_str());
            return false;
        }
        size_t offset = 0;
        VHash::iterator it = m_add_dict->begin();
        while (it)
        {
            uint32_t tmp = 0;
            uint32_t length = 0;
            int32_t docid = 0;
            SkipList *list = m_skiplist_pool.addr(it.value());
            SkipList::iterator sit = list->begin();
            SkipList::iterator end = list->end();
            if (sit == end)
            {
                continue;
            }
            while (sit != end)
            {
                docid = *sit;
                if (fs->fwrite(&docid, sizeof(docid), 1, data) != 1)
                {
                    P_WARNING("failed to write docid to data");
                    goto FAIL1;
                }
                length += sizeof(docid);
                if (list->payload_len() > 0)
                {
                    if (fs->fwrite(sit.payload(), list->payload_len(), 1, data) != 1)
                    {
                        P_WARNING("failed to write payload to data");
                        goto FAIL1;
                    }
                    length += list->payload_len();
                }
                ++sit;
            }
            key = it.key();
            if (fs->fwrite(&key, sizeof(key), 1, idx) != 1)
            {
                P_WARNING("failed to write key to idx");
                goto FAIL1;
            }
            tmp = list->type();
            if (fs->fwrite(&tmp, sizeof(tmp), 1, idx) != 1)
            {
                P_WARNING("failed to write type to idx");
                goto FAIL1;
            }
            tmp = list->payload_len();
            if (fs->fwrite(&tmp, sizeof(tmp), 1, idx) != 1)
            {
                P_WARNING("failed to write payload length to idx");
                goto FAIL1;
            }
            if (fs->fwrite(&offset, sizeof(offset), 1, idx) != 1)
            {
                P_WARNING("failed to write offset to idx");
                goto FAIL1;
            }
            if (fs->fwrite(&length, sizeof(length), 1, idx) != 1)
            {
                P_WARNING("failed to write length to idx");
                goto FAIL1;
            }
            if (0)
            {
FAIL1:
                fs->fclose(data);
                fs->fclose(idx);
                return false;
            }
            offset += length;
            ++it;
        }
        fs->fclose(data);
        fs->fclose(idx);
        P_WARNING("write add invert index ok");
    }
    {
        File idx = fs->fopen((path + "del.idx").c_str(), "wb");
        if (NULL == idx)
        {
            P_WARNING("failed to open file[%sdel.idx] for write", path.c_str());
            return false;
        }
        File data = fs->fopen((path + "del.data").c_str(), "wb");
        if (NULL == data)
        {
            fs->fclose(idx);

            P_WARNING("failed to open file[%sdel.data] for write", path.c_str());
            return false;
        }
        size_t offset = 0;
        VHash::iterator it = m_del_dict->begin();
        while (it)
        {
            uint32_t length = 0;
            int32_t docid = 0;
            SkipList *list = m_skiplist_pool.addr(it.value());
            SkipList::iterator sit = list->begin();
            SkipList::iterator end = list->end();
            if (sit == end)
            {
                continue;
            }
            while (sit != end)
            {
                docid = *sit;
                if (fs->fwrite(&docid, sizeof(docid), 1, data) != 1)
                {
                    P_WARNING("failed to write docid to data");
                    goto FAIL2;
                }
                length += sizeof(docid);
                ++sit;
            }
            key = it.key();
            if (fs->fwrite(&key, sizeof(key), 1, idx) != 1)
            {
                P_WARNING("failed to write key to idx");
                goto FAIL2;
            }
            if (fs->fwrite(&offset, sizeof(offset), 1, idx) != 1)
            {
                P_WARNING("failed to write offset to idx");
                goto FAIL2;
            }
            if (fs->fwrite(&length, sizeof(length), 1, idx) != 1)
            {
                P_WARNING("failed to write length to idx");
                goto FAIL2;
            }
            if (0)
            {
FAIL2:
                fs->fclose(data);
                fs->fclose(idx);
                return false;
            }
            offset += length;
            ++it;
        }
        fs->fclose(data);
        fs->fclose(idx);
        P_WARNING("write del invert index ok");
    }
    {
        File idx = fs->fopen((path + "words_bag.idx").c_str(), "wb");
        if (NULL == idx)
        {
            P_WARNING("failed to open file[%swords_bag.idx] for write", path.c_str());
            return false;
        }
        File data = fs->fopen((path + "words_bag.data").c_str(), "wb");
        if (NULL == data)
        {
            fs->fclose(idx);

            P_WARNING("failed to open file[%swords_bag.data] for write", path.c_str());
            return false;
        }
        size_t offset = 0;
        VHash::iterator it = m_words_bag->begin();
        while (it)
        {
            uint32_t length = 0;
            uint32_t sign = 0;
            IDList *list = m_idlist_pool.addr(it.value());
            IDList::iterator sit = list->begin();
            IDList::iterator end = list->end();
            if (sit == end)
            {
                continue;
            }
            while (sit != end)
            {
                sign = *sit;
                if (fs->fwrite(&sign, sizeof(sign), 1, data) != 1)
                {
                    P_WARNING("failed to write sign to data");
                    goto FAIL3;
                }
                length += sizeof(sign);
                ++sit;
            }
            key = it.key();
            if (fs->fwrite(&key, sizeof(key), 1, idx) != 1)
            {
                P_WARNING("failed to write key to idx");
                goto FAIL3;
            }
            if (fs->fwrite(&offset, sizeof(offset), 1, idx) != 1)
            {
                P_WARNING("failed to write offset to idx");
                goto FAIL3;
            }
            if (fs->fwrite(&length, sizeof(length), 1, idx) != 1)
            {
                P_WARNING("failed to write length to idx");
                goto FAIL3;
            }
            if (0)
            {
FAIL3:
                fs->fclose(data);
                fs->fclose(idx);
                return false;
            }
            offset += length;
            ++it;
        }
        fs->fclose(data);
        fs->fclose(idx);
        P_WARNING("write docid=>signs ok");
    }
    bool ret = this->m_sign2id.dump(dir, fs);
    if (ret)
    {
        P_WARNING("write dir[%s] ok", dir);
    }
    return ret;
}

bool InvertIndex::load(const char *dir, FSInterface *fs)
{
    typedef FSInterface::File File;
    if (NULL == fs)
    {
        fs = &DefaultFS::s_default;
    }

    if (NULL == dir || '\0' == *dir)
    {
        P_WARNING("empty dir error");
        return false;
    }
    P_WARNING("start to read dir[%s]", dir);
    uint32_t key;
    std::string path(dir);
    if ('/' != path[path.length() - 1])
    {
        path += "/";
    }
#ifndef __NOT_USE_COWBTREE__
    bool cowbtree_on = false;
    {
        Config conf(path.c_str(), "invert.meta");
        if (conf.parse() < 0)
        {
            P_WARNING("failed parse config[%s:invert.meta]", path.c_str());
            return -1;
        }
        std::string on;
        conf.get("cowbtree", on);
        cowbtree_on = ("on" == on);
    }
#endif
    if (!this->m_sign2id.load(dir, fs))
    {
        P_WARNING("failed to load signdict");
        return false;
    }
    {
        File idx = fs->fopen((path + "invert.idx").c_str(), "rb");
        if (NULL == idx)
        {
            P_WARNING("failed to open file[%sinvert.idx] for read", path.c_str());
            return false;
        }
        File data = fs->fopen((path + "invert.data").c_str(), "rb");
        if (NULL == data)
        {
            fs->fclose(idx);

            P_WARNING("failed to open file[%sinvert.data] for read", path.c_str());
            return false;
        }
        size_t offset = 0;
        size_t tmp;
        uint32_t length;
        while (1)
        {
            if (fs->fread(&key, sizeof(key), 1, idx) != 1)
            {
                break;
            }
            if (fs->fread(&tmp, sizeof(tmp), 1, idx) != 1)
            {
                P_WARNING("failed to read offset from idx");
                goto FAIL0;
            }
            if (fs->fread(&length, sizeof(length), 1, idx) != 1)
            {
                P_WARNING("failed to read length from idx");
                goto FAIL0;
            }
            if (tmp != offset)
            {
                P_WARNING("offset check error");
                goto FAIL0;
            }
            if (length < int(sizeof(bl_head_t) + sizeof(int32_t)))
            {
                P_WARNING("invalid length[%u]", length);
                goto FAIL0;
            }
            {
#ifdef __NOT_USE_COWBTREE__
                void *mem = ::malloc(length);
                bl_head_t *pl = (bl_head_t *)mem;
                if (NULL == mem)
                {
                    P_WARNING("failed to alloc mem, length=%u", length);
                    goto FAIL0;
                }
                if (fs->fread(mem, 1, length, data) != length)
                {
                    ::free(mem);
                    P_WARNING("failed to read data");
                    goto FAIL0;
                }
                if (length != (uint32_t)(sizeof(bl_head_t)
                            + sizeof(int32_t) * pl->doc_num + pl->payload_len * pl->doc_num))
                {
                    ::free(mem);
                    P_WARNING("failed to check length");
                    goto FAIL0;
                }
                if (!m_dict->insert(key, mem))
                {
                    ::free(mem);
                    P_WARNING("failed to insert into m_dict");
                    goto FAIL0;
                }
#else
                bl_head_t head;
                vaddr_t new_big = 0;
                Btree *big = NULL;
                if (fs->fread(&head, sizeof(head), 1, data) != 1)
                {
                    P_WARNING("failed to read bl_head_t");
                    goto FAIL0;
                }
                if (length != (uint32_t)(sizeof(bl_head_t)
                            + (sizeof(int32_t) + head.payload_len) * head.doc_num))
                {
                    P_WARNING("failed to check length");
                    goto FAIL0;
                }
                new_big = m_btree_pool.alloc<RPool *, uint8_t, uint16_t>
                    (&m_rpool, head.type, head.payload_len);
                if (0 == new_big)
                {
                    P_WARNING("failed to alloc btree");
                    goto FAIL0;
                }
                big = m_btree_pool.addr(new_big);
                if (!big->init_for_modify())
                {
                    P_WARNING("failed to call init_for_modify");
                    goto PRE_FAIL0;
                }
                if (cowbtree_on)
                {
                    int32_t docid;
                    if (head.payload_len > 0)
                    {
                        void *payload = ::malloc(head.payload_len);
                        if (NULL == payload)
                        {
                            P_WARNING("failed to alloc payload, payload_len=%d", int(head.payload_len));
                            goto PRE_FAIL0;
                        }
                        for (int i = 0; i < head.doc_num; ++i)
                        {
                            if (fs->fread(&docid, sizeof(docid), 1, data) != 1)
                            {
                                ::free(payload);
                                P_WARNING("failed to read docid");
                                goto PRE_FAIL0;
                            }
                            if (fs->fread(payload, head.payload_len, 1, data) != 1)
                            {
                                ::free(payload);
                                P_WARNING("failed to read payload");
                                goto PRE_FAIL0;
                            }
                            big->insert(docid, payload);
                        }
                        ::free(payload);
                    }
                    else
                    {
                        for (int i = 0; i < head.doc_num; ++i)
                        {
                            if (fs->fread(&docid, sizeof(docid), 1, data) != 1)
                            {
                                P_WARNING("failed to read docid");
                                goto PRE_FAIL0;
                            }
                            big->insert(docid, NULL);
                        }
                    }
                }
                else
                {
                    length -= sizeof(head);
                    void *mem = ::malloc(length);
                    if (NULL == mem)
                    {
                        P_WARNING("failed to alloc mem, length=%u", length);
                        goto PRE_FAIL0;
                    }
                    if (fs->fread(mem, 1, length, data) != length)
                    {
                        ::free(mem);
                        P_WARNING("failed to read data");
                        goto PRE_FAIL0;
                    }
                    int32_t *p_docids = (int32_t *)mem;
                    void *p_payloads = p_docids + head.doc_num;
                    for (int i = 0; i < head.doc_num; ++i)
                    {
                        big->insert(p_docids[i], ((char *)p_payloads) + i * head.payload_len);
                    }
                    ::free(mem);
                    length += sizeof(head);
                }
                if (!big->end_for_modify())
                {
                    P_WARNING("failed to call end_for_modify");
                    goto PRE_FAIL0;
                }
                if (!m_dict->insert(key, new_big))
                {
                    P_WARNING("failed to insert btree to m_dict");
                    goto PRE_FAIL0;
                }
                if (0)
                {
PRE_FAIL0:
                    m_btree_pool.free(new_big);
                    goto FAIL0;
                }
#endif
            }
            offset += length;
            if (0)
            {
FAIL0:
                fs->fclose(data);
                fs->fclose(idx);
                return false;
            }
        }
        fs->fclose(data);
        fs->fclose(idx);
        P_WARNING("read invert index ok");
    }
    {
        File idx = fs->fopen((path + "add.idx").c_str(), "rb");
        if (NULL == idx)
        {
            P_WARNING("failed to open file[%sadd.idx] for read", path.c_str());
            return false;
        }
        File data = fs->fopen((path + "add.data").c_str(), "rb");
        if (NULL == data)
        {
            fs->fclose(idx);

            P_WARNING("failed to open file[%sadd.data] for read", path.c_str());
            return false;
        }
        size_t offset = 0;
        size_t tmp;
        uint32_t type;
        uint32_t payload_len;
        uint32_t length;
        int32_t docid;
        char *payload = NULL;
        vaddr_t vlist = 0;
        SkipList *list = NULL;
        while (1)
        {
            if (fs->fread(&key, sizeof(key), 1, idx) != 1)
            {
                break;
            }
            if (fs->fread(&type, sizeof(type), 1, idx) != 1)
            {
                P_WARNING("failed to read type from idx");
                goto FAIL1;
            }
            if (fs->fread(&payload_len, sizeof(payload_len), 1, idx) != 1)
            {
                P_WARNING("failed to read payload length from idx");
                goto FAIL1;
            }
            if (fs->fread(&tmp, sizeof(tmp), 1, idx) != 1)
            {
                P_WARNING("failed to read offset from idx");
                goto FAIL1;
            }
            if (fs->fread(&length, sizeof(length), 1, idx) != 1)
            {
                P_WARNING("failed to read length from idx");
                goto FAIL1;
            }
            if (tmp != offset)
            {
                P_WARNING("offset check error");
                goto FAIL1;
            }
            payload = NULL;
            if (payload_len > 0)
            {
                payload = new (std::nothrow) char[payload_len];
                if (NULL == payload)
                {
                    P_WARNING("failed to alloc payload, payload length is %u", payload_len);
                    goto FAIL1;
                }
            }
            vlist = m_skiplist_pool.alloc(&m_pool, type, payload_len);
            list = m_skiplist_pool.addr(vlist);
            if (NULL == list)
            {
                P_WARNING("failed to alloc skiplist");
                goto FAIL1;
            }
            if (!m_add_dict->insert(key, vlist))
            {
                m_skiplist_pool.free(vlist);

                P_WARNING("failed to alloc skiplist");
                goto FAIL1;
            }
            for (uint32_t i = 0; i < length; i += sizeof(docid) + payload_len)
            {
                if (fs->fread(&docid, sizeof(docid), 1, data) != 1)
                {
                    P_WARNING("failed to read docid from data");
                    goto FAIL1;
                }
                if (payload_len > 0 && fs->fread(payload, payload_len, 1, data) != 1)
                {
                    P_WARNING("failed to read payload from data");
                    goto FAIL1;
                }
                if (!list->insert(docid, payload))
                {
                    P_WARNING("failed to insert docid to skiplist");
                    goto FAIL1;
                }
            }
            if (payload)
            {
                delete [] payload;
                payload = NULL;
            }
            offset += length;
            if (0)
            {
FAIL1:
                if (payload)
                {
                    delete [] payload;
                }
                fs->fclose(data);
                fs->fclose(idx);
                return false;
            }
        }
        fs->fclose(data);
        fs->fclose(idx);
        P_WARNING("read add invert index ok");
    }
    {
        File idx = fs->fopen((path + "del.idx").c_str(), "rb");
        if (NULL == idx)
        {
            P_WARNING("failed to open file[%sdel.idx] for read", path.c_str());
            return false;
        }
        File data = fs->fopen((path + "del.data").c_str(), "rb");
        if (NULL == data)
        {
            fs->fclose(idx);

            P_WARNING("failed to open file[%sdel.data] for read", path.c_str());
            return false;
        }
        size_t offset = 0;
        size_t tmp;
        uint32_t length;
        int32_t docid;
        vaddr_t vlist = 0;
        SkipList *list = NULL;
        while (1)
        {
            if (fs->fread(&key, sizeof(key), 1, idx) != 1)
            {
                break;
            }
            if (fs->fread(&tmp, sizeof(tmp), 1, idx) != 1)
            {
                P_WARNING("failed to read offset from idx");
                goto FAIL2;
            }
            if (fs->fread(&length, sizeof(length), 1, idx) != 1)
            {
                P_WARNING("failed to read length from idx");
                goto FAIL2;
            }
            if (tmp != offset)
            {
                P_WARNING("offset check error");
                goto FAIL2;
            }
            vlist = m_skiplist_pool.alloc(&m_pool, 0xFF, 0);
            list = m_skiplist_pool.addr(vlist);
            if (NULL == list)
            {
                P_WARNING("failed to alloc skiplist");
                goto FAIL2;
            }
            if (!m_del_dict->insert(key, vlist))
            {
                m_skiplist_pool.free(vlist);

                P_WARNING("failed to alloc skiplist");
                goto FAIL2;
            }
            for (uint32_t i = 0; i < length; i += sizeof(docid))
            {
                if (fs->fread(&docid, sizeof(docid), 1, data) != 1)
                {
                    P_WARNING("failed to read docid from data");
                    goto FAIL2;
                }
                if (!list->insert(docid, NULL))
                {
                    P_WARNING("failed to insert docid to skiplist");
                    goto FAIL2;
                }
            }
            offset += length;
            if (0)
            {
FAIL2:
                fs->fclose(data);
                fs->fclose(idx);
                return false;
            }
        }
        fs->fclose(data);
        fs->fclose(idx);
        P_WARNING("read del invert index ok");
    }
    {
        File idx = fs->fopen((path + "words_bag.idx").c_str(), "rb");
        if (NULL == idx)
        {
            P_WARNING("failed to open file[%swords_bag.idx] for read", path.c_str());
            return false;
        }
        File data = fs->fopen((path + "words_bag.data").c_str(), "rb");
        if (NULL == data)
        {
            fs->fclose(idx);

            P_WARNING("failed to open file[%swords_bag.data] for read", path.c_str());
            return false;
        }
        size_t offset = 0;
        size_t tmp;
        uint32_t length;
        uint32_t num;
        std::vector<uint32_t> signs;
        vaddr_t vlist = 0;
        IDList *list = NULL;
        while (1)
        {
            if (fs->fread(&key, sizeof(key), 1, idx) != 1)
            {
                break;
            }
            if (fs->fread(&tmp, sizeof(tmp), 1, idx) != 1)
            {
                P_WARNING("failed to read offset from idx");
                goto FAIL3;
            }
            if (fs->fread(&length, sizeof(length), 1, idx) != 1)
            {
                P_WARNING("failed to read length from idx");
                goto FAIL3;
            }
            if (length % sizeof(uint32_t) != 0)
            {
                P_WARNING("length check error");
                goto FAIL3;
            }
            if (tmp != offset)
            {
                P_WARNING("offset check error");
                goto FAIL3;
            }
            vlist = m_idlist_pool.alloc(&m_inode_pool);
            list = m_idlist_pool.addr(vlist);
            if (NULL == list)
            {
                P_WARNING("failed to alloc idlist");
                goto FAIL3;
            }
            if (!m_words_bag->insert(key, vlist))
            {
                m_idlist_pool.free(vlist);

                P_WARNING("failed to insert idlist");
                goto FAIL3;
            }
            num = length / sizeof(uint32_t);
            if (num > 0)
            {
                signs.reserve(num);
                if (fs->fread(&signs[0], length, 1, data) != 1)
                {
                    P_WARNING("failed to read signs from data");
                    goto FAIL3;
                }
                for (uint32_t i = num; i >= 1; --i)
                {
                    if (!list->insert(signs[i - 1]))
                    {
                        P_WARNING("failed to insert sign to idlist");
                        goto FAIL3;
                    }
                }
            }
            offset += length;
            if (0)
            {
FAIL3:
                fs->fclose(data);
                fs->fclose(idx);
                return false;
            }
        }
        fs->fclose(data);
        fs->fclose(idx);
        P_WARNING("read docid=>signs ok");
    }
    P_WARNING("read dir[%s] ok", dir);
    return true;
}

void InvertIndex::try_exc_cmd()
{
    if (m_exc_cmd_fw.check_and_update_timestamp() > 0)
    {
        this->exc_cmd();
    }
}

void InvertIndex::exc_cmd() const
{
#ifndef P_LOG_CMD
#define P_LOG_CMD( _fmt_, args... )
#endif
    int type;
    std::string cmd;
    std::string key;

    std::ifstream in(m_exc_cmd_file.c_str());
    while (in>>cmd>>key>>type)
    {
        if (type > INT8_MAX || type < INT8_MIN)
        {
            P_LOG_CMD("invalid invert type(out of range), cmd[%s %s %d]", cmd.c_str(), key.c_str(), type);
            continue;
        }
        P_LOG_CMD("executing cmd[%s %s %d]", cmd.c_str(), key.c_str(), type);
        if (cmd == "print_list")
        {
            int32_t doc_num = 0;
            DocList *list = this->trigger(key.c_str(), type);
            if (list)
            {
                int32_t docid = list->first();
                while (docid != -1)
                {
                    P_LOG_CMD("    [%05d] docid[%d]", doc_num, docid);
                    ++doc_num;
                    docid = list->next();
                }
                delete list;
            }
            P_LOG_CMD("length of list[%s:%d] = %d", key.c_str(), type, doc_num);
        }
        else if (cmd == "print_list_len")
        {
            int32_t doc_num = 0;
            DocList *list = this->trigger(key.c_str(), type);
            if (list)
            {
                int32_t docid = list->first();
                while (docid != -1)
                {
                    ++doc_num;
                    docid = list->next();
                }
                delete list;
            }
            P_LOG_CMD("length of list[%s:%d] = %d", key.c_str(), type, doc_num);
        }
        else
        {
            P_LOG_CMD("unsupported cmd[%s]", cmd.c_str());
        }
    }
    in.close();
#undef P_LOG_CMD
}
