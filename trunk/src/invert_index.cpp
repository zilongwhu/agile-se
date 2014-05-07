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

#include <stack>
#include "log.h"
#include "parser.h"
#include "orlist.h"
#include "andlist.h"
#include "difflist.h"
#include "configure.h"
#include "invert_index.h"
#include "fast_timer.h"

struct bl_head_t
{
    uint8_t type;
    uint16_t payload_len;
    int doc_num;
};

/* bl_head_t + docids + payloads */
class BigList: public DocList
{
    public:
        BigList(uint32_t sign, void *data)
        {
            m_sign = sign;
            memcpy(&m_head, data, sizeof m_head);
            m_docids = (int32_t *)(((int8_t *)data) + sizeof m_head);
            m_payloads = (int8_t *)(m_docids + m_head.doc_num);
            m_pos = 0;
        }

        int32_t first()
        {
            m_pos = 0;
            if (m_pos < m_head.doc_num)
            {
                return m_docids[m_pos];
            }
            return -1;
        }
        int32_t next()
        {
            if (++m_pos < m_head.doc_num)
            {
                return m_docids[m_pos];
            }
            return -1;
        }
        int32_t curr()
        {
            if (m_pos < m_head.doc_num)
            {
                return m_docids[m_pos];
            }
            return -1;
        }
        int32_t find(int32_t docid)
        {
            if (m_pos >= m_head.doc_num)
            {
                return -1;
            }
            if (m_docids[m_pos] >= docid)
            {
                return m_docids[m_pos];
            }
            else if (docid - m_docids[m_pos] < 16/* 前方不远处 */
                    || m_head.doc_num - m_pos < 16)/* 拉链快走完了 */
            {
                /* 遍历查找 */
                for (++m_pos; m_pos < m_head.doc_num; ++m_pos)
                {
                    if (m_docids[m_pos] >= docid)
                    {
                        return m_docids[m_pos];
                    }
                }
                return -1;
            }
            else
            {
                /* 二分查找 */
                int beg = m_pos;
                int end = m_head.doc_num - 1;
                int mid;
                while (end >= beg)
                {
                    mid = beg + ((end - beg) >> 1);
                    if (docid == m_docids[mid])
                    {   /* 找到了 */
                        m_pos = mid;
                        return docid;
                    }
                    else if (docid < m_docids[mid])
                    {
                        end = mid - 1;
                    }
                    else
                    {
                        beg = mid + 1;
                    }
                }
                m_pos = beg; /* beg为第一个大于docid的元素的偏移量 */
                if (m_pos >= m_head.doc_num)
                {
                    return -1;
                }
                return m_docids[m_pos];
            }
        }

        InvertStrategy::info_t *get_strategy_data(InvertStrategy &st)
        {
            if (m_pos < m_head.doc_num)
            {
                m_strategy_data.data = m_data;
                m_strategy_data.sign = m_sign;
                m_strategy_data.type = m_head.type;
                m_strategy_data.length = m_head.payload_len;
                if (m_head.payload_len > 0)
                {
                    memcpy(m_strategy_data.result, m_payloads + m_pos * m_head.payload_len, m_head.payload_len);
                }
                st.work(&m_strategy_data);
                return &m_strategy_data;
            }
            return NULL;
        }
    private:
        uint32_t m_sign;
        bl_head_t m_head;
        int32_t *m_docids;
        int8_t *m_payloads;
        int m_pos;
};

class AddList: public DocList
{
    public:
        typedef InvertIndex::SkipList::iterator Iterator;

        AddList(uint32_t sign, uint8_t type, uint16_t payload_len, Iterator it)
            : m_it(it), m_it_c(it), m_end(0, it.list())
        {
            m_sign = sign;
            m_type = type;
            m_payload_len = payload_len;
        }

        int32_t first()
        {
            m_it = m_it_c;
            return curr();
        }
        int32_t next()
        {
            ++m_it;
            return curr();
        }
        int32_t curr()
        {
            if (m_it != m_end)
                return *m_it;
            else
                return -1;
        }
        int32_t find(int32_t docid)
        {
            m_it.find(docid);
            return curr();
        }

        InvertStrategy::info_t *get_strategy_data(InvertStrategy &st)
        {
            if (m_it != m_end)
            {
                m_strategy_data.data = m_data;
                m_strategy_data.sign = m_sign;
                m_strategy_data.type = m_type;
                m_strategy_data.length = m_payload_len;
                if (m_payload_len > 0)
                {
                    ::memcpy(m_strategy_data.result, m_it.payload(), m_payload_len);
                }
                st.work(&m_strategy_data);
                return &m_strategy_data;
            }
            return NULL;
        }
    private:
        uint32_t m_sign;
        uint8_t m_type;
        uint16_t m_payload_len;
        Iterator m_it;
        Iterator m_it_c;
        Iterator m_end;
};

class DeleteList: public DocList
{
    public:
        typedef InvertIndex::SkipList::iterator Iterator;

        DeleteList(Iterator it): m_it(it), m_it_c(it), m_end(0, it.list()) { }

        int32_t first()
        {
            m_it = m_it_c;
            return curr();
        }
        int32_t next()
        {
            ++m_it;
            return curr();
        }
        int32_t curr()
        {
            if (m_it != m_end)
                return *m_it;
            else
                return -1;
        }
        int32_t find(int32_t docid)
        {
            m_it.find(docid);
            return curr();
        }

        InvertStrategy::info_t *get_strategy_data(InvertStrategy &/* st */)
        {
            return NULL;
        }
    private:
        Iterator m_it;
        Iterator m_it_c;
        Iterator m_end;
};

class MergeList: public DocList
{
    public:
        /* big和add不能同时为NULL */
        MergeList(DocList *big, DocList *add, DocList *del)
        {
            m_big = big;
            m_add = add;
            m_del = del;
            if (m_big)
            {
                if (m_add)
                {
                    m_flag = 0;
                    if (m_del)
                    {
                        m_list = new OrList(new DiffList(m_big, m_del), m_add);
                    }
                    else
                    {
                        m_list = new OrList(m_big, m_add);
                    }
                }
                else
                {
                    m_flag = 1;
                    if (m_del)
                    {
                        m_list = new DiffList(m_big, m_del);
                    }
                    else
                    {
                        m_list = m_big;
                    }
                }
            }
            else
            {
                m_flag = 2;
                /* m_add is not NULL */
                if (m_del)
                {
                    m_list = new DiffList(m_add, m_del);
                }
                else
                {
                    m_list = m_add;
                }
            }
        }
        ~MergeList()
        {
            if (m_list)
            {
                delete m_list;
                m_list = NULL;
            }
        }
        void set_data(InvertStrategy::data_t data)
        {
            this->m_data = data;
            if (m_big)
            {
                m_big->set_data(data);
            }
            if (m_add)
            {
                m_add->set_data(data);
            }
        }
        int32_t first()
        {
            return m_list->first();
        }
        int32_t next()
        {
            return m_list->next();
        }
        int32_t curr()
        {
            return m_list->curr();
        }
        int32_t find(int32_t docid)
        {
            return m_list->find(docid);
        }
        InvertStrategy::info_t *get_strategy_data(InvertStrategy &st)
        {
            switch (m_flag)
            {
                case 0:
                    if (m_list->curr() == m_add->curr())
                    {
                        return m_add->get_strategy_data(st);
                    }
                    else
                    {
                        return m_big->get_strategy_data(st);
                    }
                case 1:
                    return m_big->get_strategy_data(st);
                case 2:
                    return m_add->get_strategy_data(st);
            }
            return NULL;
        }
    private:
        DocList *m_list;
        DocList *m_big;
        DocList *m_add;
        DocList *m_del;
        int m_flag;
};

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
    if (m_docid2signs)
    {
        delete m_docid2signs;
        m_docid2signs = NULL;
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
    if (NodePool::init_pool(&m_pool) < 0
            || VNodePool::init_pool(&m_pool) < 0
            || SNodePool::init_pool(&m_pool) < 0
            || INodePool::init_pool(&m_pool) < 0
            || SkipListPool::init_pool(&m_pool) < 0
            || IDListPool::init_pool(&m_pool) < 0)
    {
        WARNING("failed to call init_pool");
        return -1;
    }
    if (SkipList::init_pool(&m_pool, 0) < 0)
    {
        WARNING("failed to register item for deleted list");
        return -1;
    }
    for (int i = 0; i < 0xFF; ++i)
    {
        if (m_types.is_valid_type(i))
        {
            if (SkipList::init_pool(&m_pool, m_types.types[i].payload_len) < 0)
            {
                WARNING("failed to register element size for invert type: %d", i);
                return -1;
            }
        }
    }
    if (!config.get("merge_threshold", m_merge_threshold) || m_merge_threshold <= 0)
    {
        WARNING("failed to get merge_threshold");
        return -1;
    }
    if (m_pool.register_item(sizeof(NodePool::ObjectType)) < 0)
    {
        WARNING("failed to register node to mempool");
        return -1;
    }
    if (m_pool.register_item(sizeof(VNodePool::ObjectType)) < 0)
    {
        WARNING("failed to register vnode to mempool");
        return -1;
    }
    if (m_pool.register_item(sizeof(SNodePool::ObjectType)) < 0)
    {
        WARNING("failed to register snode to mempool");
        return -1;
    }
    if (m_pool.register_item(sizeof(INodePool::ObjectType)) < 0)
    {
        WARNING("failed to register inode to mempool");
        return -1;
    }
    if (m_pool.register_item(sizeof(SkipListPool::ObjectType)) < 0)
    {
        WARNING("failed to register skiplist to mempool");
        return -1;
    }
    if (m_pool.register_item(sizeof(IDListPool::ObjectType)) < 0)
    {
        WARNING("failed to register idlist to mempool");
        return -1;
    }
    int max_items_num;
    if (!config.get("max_items_num", max_items_num) || max_items_num <= 0)
    {
        WARNING("failed to get max_items_num");
        return -1;
    }
    if (m_pool.init(max_items_num) < 0)
    {
        WARNING("failed to init mempool");
        return -1;
    }
    m_node_pool.init(&m_pool);
    m_vnode_pool.init(&m_pool);
    m_snode_pool.init(&m_pool);
    m_inode_pool.init(&m_pool);
    m_skiplist_pool.init(&m_pool);
    m_idlist_pool.init(&m_pool);
    int signdict_hash_size;
    if (!config.get("signdict_hash_size", signdict_hash_size) || signdict_hash_size <= 0)
    {
        WARNING("failed to get signdict_hash_size");
        return -1;
    }
    int signdict_buffer_size;
    if (!config.get("signdict_buffer_size", signdict_buffer_size) || signdict_buffer_size <= 0)
    {
        WARNING("failed to get signdict_buffer_size");
        return -1;
    }
    if (m_sign2id.init(&m_snode_pool, signdict_hash_size, signdict_buffer_size) < 0)
    {
        WARNING("failed to init sign2id dict");
        return -1;
    }
    int dict_hash_size;
    if (!config.get("dict_hash_size", dict_hash_size) || dict_hash_size <= 0)
    {
        WARNING("failed to get dict_hash_size");
        return -1;
    }
    m_dict = new Hash(dict_hash_size);
    if (NULL == m_dict)
    {
        WARNING("failed to new m_dict");
        return -1;
    }
    m_dict->set_pool(&m_node_pool);
    m_dict->set_cleanup(cleanup_node, (intptr_t)this);
    int add_dict_hash_size;
    if (!config.get("add_dict_hash_size", add_dict_hash_size) || add_dict_hash_size <= 0)
    {
        WARNING("failed to get add_dict_hash_size");
        return -1;
    }
    m_add_dict = new VHash(add_dict_hash_size);
    if (NULL == m_add_dict)
    {
        WARNING("failed to new m_add_dict");
        return -1;
    }
    m_add_dict->set_pool(&m_vnode_pool);
    m_add_dict->set_cleanup(cleanup_diff_node, (intptr_t)this);
    int del_dict_hash_size;
    if (!config.get("del_dict_hash_size", del_dict_hash_size) || del_dict_hash_size <= 0)
    {
        WARNING("failed to get del_dict_hash_size");
        return -1;
    }
    m_del_dict = new VHash(del_dict_hash_size);
    if (NULL == m_del_dict)
    {
        WARNING("failed to new m_del_dict");
        return -1;
    }
    m_del_dict->set_pool(&m_vnode_pool);
    m_del_dict->set_cleanup(cleanup_diff_node, (intptr_t)this);
    int docid2signs_hash_size;
    if (!config.get("docid2signs_hash_size", docid2signs_hash_size) || docid2signs_hash_size <= 0)
    {
        WARNING("failed to get docid2signs_hash_size");
        return -1;
    }
    m_docid2signs = new VHash(docid2signs_hash_size);
    if (NULL == m_docid2signs)
    {
        WARNING("failed to new m_docid2signs");
        return -1;
    }
    m_docid2signs->set_pool(&m_vnode_pool);
    m_docid2signs->set_cleanup(cleanup_id_node, (intptr_t)this);

    m_types.set_sign_dict(&m_sign2id);

    WARNING("signdict_hash_size=%u", signdict_hash_size);
    WARNING("signdict_buffer_size=%u", signdict_buffer_size);
    WARNING("dict_hash_size=%u", dict_hash_size);
    WARNING("add_dict_hash_size=%u", add_dict_hash_size);
    WARNING("del_dict_hash_size=%u", del_dict_hash_size);
    WARNING("docid2signs_hash_size=%u", docid2signs_hash_size);
    WARNING("init ok");
    return 0;
}

DocList *InvertIndex::trigger(const char *keystr, uint8_t type) const
{
    if (NULL == keystr || !m_types.is_valid_type(type))
    {
        WARNING("invalid parameter");
        return NULL;
    }
    uint32_t sign = m_types.get_sign(keystr, type);
    return this->trigger(sign, type);
}

DocList *InvertIndex::trigger(uint32_t sign, uint8_t type) const
{
    void **big = m_dict->find(sign);
    vaddr_t *vadd = m_add_dict->find(sign);

    if (NULL == big && NULL == vadd)
    {
        return NULL;
    }
    SkipList *add = m_skiplist_pool.addr(*vadd);
    SkipList *del = NULL;

    vaddr_t *vdel = m_del_dict->find(sign);
    if (NULL != vdel)
    {
        del = m_skiplist_pool.addr(*vdel);
    }

    BigList *bl = NULL;
    if (big)
    {
        bl = new(std::nothrow) BigList(sign, *big);
        if (NULL == bl)
        {
            WARNING("failed to new BigList");
            return NULL;
        }
    }
    AddList *al = NULL;
    if (add)
    {
        al = new(std::nothrow) AddList(sign, type,
                m_types.types[type].payload_len, add->begin());
        if (NULL == al)
        {
            WARNING("failed to new AddList");
            if (bl)
            {
                delete bl;
            }
            return NULL;
        }
    }
    DeleteList *dl = NULL;
    if (del)
    {
        dl = new(std::nothrow) DeleteList(del->begin());
        if (NULL == dl)
        {
            WARNING("failed to new DeleteList");
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

bool InvertIndex::get_signs_by_docid(int32_t docid, std::vector<uint32_t> &signs) const
{
    signs.clear();

    vaddr_t *vlist = m_docid2signs->find(docid);
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

bool InvertIndex::insert(const char *keystr, uint8_t type, int32_t docid, const std::string &json)
{
    if (json.length() == 0)
    {
        return this->insert(keystr, type, docid, (cJSON *)NULL);
    }
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

bool InvertIndex::insert(const char *keystr, uint8_t type, int32_t docid, cJSON *json)
{
    if (NULL == keystr || !m_types.is_valid_type(type))
    {
        WARNING("invalid parameter: keystr[%p], type[%d]", keystr, int(type));
        return false;
    }
    if (m_types.types[type].payload_len > 0)
    {
        if (NULL == json)
        {
            WARNING("invert type[%d] must has payload", int(type));
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
    else
    {
        return this->insert(keystr, type, docid, (void *)NULL);
    }
}

bool InvertIndex::insert(const char *keystr, uint8_t type, int32_t docid, void *payload)
{
    uint32_t sign = m_types.record_sign(keystr, type);
    const uint32_t payload_len = m_types.types[type].payload_len;
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
            WARNING("conflicting hash value[%s:%d]", keystr, int(type));
            return false;
        }
        if (!add_list->insert(docid, payload))
        {
            WARNING("failed to insert docid[%d] for hash value[%s:%d]", docid, keystr, int(type));
            return false;
        }
        if (add_list->size() > (uint32_t)m_merge_threshold)
        {
            this->merge(sign, type);
        }
    }
    else
    {
        vaddr_t vlist = m_skiplist_pool.alloc(&m_pool, payload_len);
        SkipList *tmp = m_skiplist_pool.addr(vlist);
        if (NULL == tmp)
        {
            WARNING("failed to alloc SkipList");
            return false;
        }
        if (!tmp->insert(docid, payload) || !m_add_dict->insert(sign, vlist))
        {
            m_skiplist_pool.free(vlist);
            WARNING("failed to insert docid[%d] for hash value[%s:%d]", docid, keystr, int(type));
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
    vaddr_t *vidlist = m_docid2signs->find(docid);
    if (vidlist)
    {
        idlist = m_idlist_pool.addr(*vidlist);
    }
    if (idlist)
    {
        if (!idlist->insert(sign))
        {
            WARNING("failed to insert sign[%u] of hash value[%s:%d] for docid[%d]", sign, keystr, int(type), docid);
            return false;
        }
    }
    else
    {
        vaddr_t vlist = m_idlist_pool.alloc(&m_inode_pool);
        IDList *tmp = m_idlist_pool.addr(vlist);
        if (NULL == tmp)
        {
            WARNING("failed to alloc IDList");
            return false;
        }
        if (!tmp->insert(sign) || !m_docid2signs->insert(docid, vlist))
        {
            m_idlist_pool.free(vlist);
            WARNING("failed to insert sign[%u] of hash value[%s:%d] for docid[%d]", sign, keystr, int(type), docid);
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
            WARNING("failed to remove docid[%d] for hash value[%s:%d]", docid, keystr, int(type));
            return false;
        }
        if (del_list->size() > (uint32_t)m_merge_threshold)
        {
            this->merge(sign, type);
        }
    }
    else
    {
        vaddr_t vlist = m_skiplist_pool.alloc(&m_pool, 0);
        SkipList *tmp = m_skiplist_pool.addr(vlist);
        if (NULL == tmp)
        {
            WARNING("failed to alloc SkipList");
            return false;
        }
        if (!tmp->insert(docid, NULL) || !m_del_dict->insert(sign, vlist))
        {
            m_skiplist_pool.free(vlist);
            WARNING("failed to remove docid[%d] for hash value[%s:%d]", docid, keystr, int(type));
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
    vaddr_t *vidlist = m_docid2signs->find(docid);
    if (vidlist)
    {
        IDList *idlist = m_idlist_pool.addr(*vidlist);
        idlist->remove(sign);
        if (idlist->size() == 0)
        {
            m_docid2signs->remove(docid);
        }
    }
    return true;
}

bool InvertIndex::remove(int32_t docid)
{
    vaddr_t *vidlist = m_docid2signs->find(docid);
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
                WARNING("failed to remove docid[%d] for hash value[%lu]", docid, sign);
                return false;
            }
        }
        else
        {
            vaddr_t vlist = m_skiplist_pool.alloc(&m_pool, 0);
            SkipList *tmp = m_skiplist_pool.addr(vlist);
            if (NULL == tmp)
            {
                WARNING("failed to alloc SkipList");
                return false;
            }
            if (!tmp->insert(docid, NULL) || !m_del_dict->insert(sign, vlist))
            {
                m_skiplist_pool.free(vlist);
                WARNING("failed to remove docid[%d] for hash value[%lu]", docid, sign);
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
    m_docid2signs->remove(docid);
    return true;
}

void InvertIndex::merge(uint32_t sign, uint8_t type)
{
    long us = 0;
    FastTimer timer;

    timer.start();
    std::string word;
    m_sign2id.find(sign, word);
    const uint32_t payload_len = m_types.types[type].payload_len;
    DocList *list = this->trigger(sign, type);
    if (list)
    {
        int docnum = 0;
        int32_t docid = list->first();
        while (docid != -1)
        {
            ++docnum;
            docid = list->next();
        }
        if (docnum > 0)
        {
            void *mem = ::malloc(sizeof(bl_head_t) + sizeof(int32_t)*docnum + payload_len*docnum);
            if (mem)
            {
                DummyStrategy st;

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
                    WARNING("merge sign[%u] ok, type[%d], word[%s], list len=%d, cost %ld us", sign, int(type), word.c_str(), docnum, us);
                }
                else
                {
                    ::free(mem);
                    WARNING("failed to merge sign[%u], type[%d], word[%s]", sign, int(type), word.c_str());
                }
            }
            else
            {
                WARNING("failed to alloc mem[%d]", (sizeof(bl_head_t)
                            + sizeof(int32_t)*docnum + payload_len*docnum));
            }
        }
        else
        {
            m_dict->remove(sign);
            m_add_dict->remove(sign);
            m_del_dict->remove(sign);

            timer.stop();
            us = timer.timeInUs();
            WARNING("merge sign[%u] ok, type[%d], word[%s], list len=%d, cost %ld us", sign, int(type), word.c_str(), docnum, us);
        }
        delete list;
    }
}

void InvertIndex::cleanup_node(Hash::node_t *node, intptr_t arg)
{
    InvertIndex *ptr = (InvertIndex *)arg;
    if (NULL == ptr)
    {
        FATAL("should not run to here");
        ::abort();
    }
    if (node->value)
    {
        ::free(node->value);
    }
}

void InvertIndex::cleanup_diff_node(VHash::node_t *node, intptr_t arg)
{
    InvertIndex *ptr = (InvertIndex *)arg;
    if (NULL == ptr)
    {
        FATAL("should not run to here");
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
        FATAL("should not run to here");
        ::abort();
    }
    if (node->value)
    {
        ptr->m_idlist_pool.free(node->value);
    }
}

DocList *InvertIndex::parse(const std::string &query, const std::vector<term_t> terms) const
{
    if(0 == query.length())
    {
        TRACE("query is empty when parse term query");
        return NULL;
    }
    std::vector<std::string> tokens;
    if(::infix2postfix(query, tokens) != 0)
    {
        WARNING("fail to parse query: %s", query.c_str());
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
                        WARNING("invalid post expression, query[%s]", query.c_str());
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
                                WARNING("failed to new andlist");
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
                                WARNING("failed to new orlist");
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
                                WARNING("failed to new difflist");
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
                        WARNING("invalid post expression, query[%s]", query.c_str());
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
                        WARNING("pos is: %u, but array size is: %d", pos, int(terms.size()));
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
        WARNING("not unique doclist result");
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
