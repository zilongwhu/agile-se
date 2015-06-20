#ifndef __AGILE_SE_FORWARD_INDEX_H__
#define __AGILE_SE_FORWARD_INDEX_H__

#include <deque>
#include <vector>
#include <string>
#include <utility>
#include <ext/hash_map>
#include "pool/mempool.h"
#include "pool/delaypool.h"
#include "pool/objectpool.h"
#include "index/hashtable.h"
#include "index/idmap.h"
#include "cJSON.h"
#include "fsint.h"
#include <google/protobuf/message.h>

struct forward_data_t
{
    const char *key;
    cJSON *value;

    bool set(cJSON *value)
    {
        key = value->string;
        if (NULL == key || '\0' == key[0])
        {
            P_WARNING("invalid key");
            return false;
        }
        this->value = value;
        return true;
    }
};
cJSON *parse_forward_json(const std::string &json, std::vector<forward_data_t> &values);

class ForwardIndex;
class FieldIterator
{
    public:
        struct value_t
        {
            int type;
            union {
                int intvalue;
                float floatvalue;
                const google::protobuf::Message *message;
                struct 
                {
                    void *binary;
                    int binarylen;
                };
            };
        };
        FieldIterator(const ForwardIndex *idx, void *info)
            : m_idx(idx)
        {
            m_pos = 0;
            m_info = info;
        }
        bool next(std::string &field_name, value_t &value);
    private:
        size_t m_pos;
        void *m_info;
        const ForwardIndex *m_idx;
};

class ForwardIndex
{
    private:
        friend class FieldIterator;
    public:
        enum { INT_TYPE = 0, FLOAT_TYPE, PROTO_TYPE, BINARY_TYPE };
        struct ids_t
        {
            int32_t old_id;
            int32_t new_id;

            ids_t() { old_id = new_id = -1; }
        };
    private:
        typedef TDelayPool<Mempool> Pool;
    public:
        typedef Pool::vaddr_t vaddr_t;
    private:
        struct value_t
        {
            int32_t oid;
            vaddr_t addr;
        };
        typedef HashTable<int32_t, value_t> Hash;  /* id => oid, vaddr */
        typedef HashTable<int32_t, int32_t> IDMap; /* oid => id */
        typedef Hash::ObjectPool NodePool;
        typedef IDMap::ObjectPool IDPool;

        struct FieldDes
        {
            int offset;
            int array_offset;
            int type; /* 0->int, 1->float, 2->google::protobuf::Message * */
            union
            {
                double default_value;
                const google::protobuf::Message *default_message;
            };
        };
    public:
        class iterator
        {
            public:
                iterator(const ForwardIndex *idx)
                    : m_idx(idx), m_it(m_idx->m_dict->begin())
                { }

                bool next(int32_t *docid, void** info = NULL)
                {
                    if (!m_it)
                    {
                        return false;
                    }
                    if (docid)
                    {
                        *docid = m_it.value().oid;
                    }
                    if (info)
                    {
                        *info = m_idx->m_pool.addr(m_it.value().addr);
                    }
                    ++m_it;
                    return true;
                }

                bool next(int32_t *docid, FieldIterator *it = NULL)
                {
                    if (it)
                    {
                        void *info = NULL;
                        const bool ret = this->next(docid, &info);
                        *it = m_idx->get_field_iterator(info);
                        return ret;
                    }
                    else
                    {
                        return this->next(docid, (void **)NULL);
                    }
                }
            private:
                const ForwardIndex *m_idx;
                Hash::iterator m_it;
        };
    private:
        ForwardIndex(const ForwardIndex &);
        ForwardIndex &operator =(const ForwardIndex &);
    public:
        ForwardIndex();
        ~ForwardIndex();

        int init(const char *path, const char *file);
        bool load(const char *dir, FSInterface *fs = NULL);
        bool dump(const char *dir, FSInterface *fs = NULL) const;

        bool has_id_mapper() const { return m_map; }

        iterator begin() const { return iterator(this); }
        size_t doc_num() const { return m_dict->size(); }

        int get_offset_by_name(const char *name) const;
        int get_array_offset_by_name(const char *name) const;

        /* get info[outerid] by innerid(triggered by invert index) */
        void *get_info_by_id(int32_t id, int32_t *oid = NULL) const;
        FieldIterator get_field_iterator(void *info) const
        {
            return FieldIterator(this, info);
        }
        void *unpack(vaddr_t add) const
            /* binary: real_len[int] + alloced_len[int] + ...  */
        {
            return m_pool.addr(add);
        }
        /* map outerid to innerid */
        int32_t get_id_by_oid(int32_t oid) const;

        /*
         * update by outerid, by the way return innerids[old&new] to caller
         * (may do some updates on invert index)
         * */
        bool update(int32_t oid, const std::vector<forward_data_t> &fields, ids_t *p_ids = NULL);
        /*
         * remove by outerid, by the way return innerid to caller
         * (remove it from invert index)
         * */
        bool remove(int32_t oid, int32_t *p_id = NULL);

        void recycle()
        {
            m_pool.recycle();
        }
        void print_meta() const;
    private:
        struct cleanup_data_t
        {
            void *mem;
            Pool::vaddr_t addr;
            std::vector<int> binary_fields;
            std::vector<int> protobuf_fields;

            void clean(Pool *pool);
        };
        static void cleanup(Hash::node_t *node, intptr_t arg);
    private:
        Pool m_pool;
        NodePool m_node_pool;
        IDPool m_id_pool;

        IDMap *m_idmap;
        Hash *m_dict;

        IDMapper *m_map;

        __gnu_cxx::hash_map<std::string, FieldDes> m_fields;
        std::vector<std::pair<uint32_t, std::string> > m_field_names;
        std::vector<std::pair<uint32_t, double> > m_default_values;
        std::vector<const google::protobuf::Message *> m_default_messages;
        size_t m_info_size;
        std::string m_meta;

        cleanup_data_t m_cleanup_data;
        std::deque<cleanup_data_t> m_delayed_list;

        std::vector<uint32_t> m_binary_size;
        char m_binary_buffer[5*1024*1024];      /* 5M */
};

#endif
