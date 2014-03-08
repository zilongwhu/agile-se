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
#include "orlist.h"
#include "difflist.h"
#include "configure.h"
#include "invert_index.h"

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
        BigList(uint64_t sign, void *data)
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
        uint64_t m_sign;
        bl_head_t m_head;
        int32_t *m_docids;
        int8_t *m_payloads;
        int m_pos;
};

class AddList: public DocList
{
    public:
        typedef IDList::iterator Iterator;

        AddList(uint64_t sign, uint8_t type, uint16_t payload_len, Iterator it)
            : m_it(it), m_it_c(it), m_end(NULL)
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
            int32_t c = curr();
            if (c == -1)
                return -1;
            else if (c >= docid)
                return c;
            for (++m_it; m_it != m_end; ++m_it)
            {
                c = *m_it;
                if (c >= docid)
                    return c;
            }
            return -1;
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
        uint64_t m_sign;
        uint8_t m_type;
        uint16_t m_payload_len;
        Iterator m_it;
        Iterator m_it_c;
        Iterator m_end;
};

class DeleteList: public DocList
{
    public:
        typedef IDList::iterator Iterator;

        DeleteList(Iterator it): m_it(it), m_it_c(it), m_end(NULL) { }

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
            int32_t c = curr();
            if (c == -1)
                return -1;
            else if (c >= docid)
                return c;
            for (++m_it; m_it != m_end; ++m_it)
            {
                c = *m_it;
                if (c >= docid)
                    return c;
            }
            return -1;
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

DocList *InvertIndex::trigger(const char *keystr, uint8_t type) const
{
    if (NULL == keystr || !m_types.is_valid_type(type))
    {
        WARNING("invalid parameter");
        return NULL;
    }
    uint64_t sign = m_types.get_sign(keystr, type);
    return this->trigger(sign, type);
}

DocList *InvertIndex::trigger(uint64_t sign, uint8_t type) const
{
    void **big = m_dict->find(sign);
    IDList **add = m_add_dict->find(sign);

    if (NULL == big && NULL == add)
    {
        return NULL;
    }

    IDList **del = m_del_dict->find(sign);

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
                m_types.types[type].payload_len, (*add)->begin());
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
        dl = new(std::nothrow) DeleteList((*del)->begin());
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

bool InvertIndex::insert(const char *keystr, uint8_t type, int32_t docid, const std::string &json)
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

bool InvertIndex::insert(const char *keystr, uint8_t type, int32_t docid, cJSON *json)
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

bool InvertIndex::insert(const char *keystr, uint8_t type, int32_t docid, void *payload)
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
            this->merge(sign, type);
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

bool InvertIndex::reomve(const char *keystr, uint8_t type, int32_t docid)
{
    uint64_t sign = m_types.get_sign(keystr, type);
    IDList **add_list = m_add_dict->find(sign);
    if (add_list)
    {
        (*add_list)->remove(docid);
        if ((*add_list)->size() == 0)
        {
            m_add_dict->remove(sign);
        }
    }
    IDList **del_list = m_del_dict->find(sign);
    if (del_list)
    {
        if (!(*del_list)->insert(docid, NULL))
        {
            WARNING("failed to remove docid[%d] for hash value[%s:%d]", docid, keystr, int(type));
            return false;
        }
        if ((*del_list)->size() > 32)
        {
            this->merge(sign, type);
        }
    }
    else
    {
        IDList *tmp = m_list_pool.alloc(m_types.types[type].pool, 0);
        if (NULL == tmp)
        {
            WARNING("failed to alloc IDList");
            return false;
        }
        if (!tmp->insert(docid, NULL)
                || !m_del_dict->insert(sign, tmp))
        {
            m_list_pool.free(tmp);
            WARNING("failed to remove docid[%d] for hash value[%s:%d]", docid, keystr, int(type));
            return false;
        }
    }
    return true;
}

void InvertIndex::merge(uint64_t sign, uint8_t type)
{
    const int payload_len = m_types.types[type].payload_len;
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
                m_add_dict->remove(sign);
                m_del_dict->remove(sign);
                if (!m_dict->insert(sign, mem))
                {
                    ::free(mem);
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
        }
        delete list;
    }
}
