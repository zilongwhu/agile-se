// =====================================================================================
//
//       Filename:  forward_index.h
//
//    Description:  
//
//        Version:  1.0
//        Created:  03/05/2014 04:53:01 PM
//       Revision:  none
//       Compiler:  g++
//
//         Author:  Zilong.Zhu (), zilong.whu@gmail.com
//        Company:  edu.whu
//
// =====================================================================================

#ifndef __AGILE_SE_FORWARD_INDEX_H__
#define __AGILE_SE_FORWARD_INDEX_H__

#include <deque>
#include <vector>
#include <string>
#include <utility>
#include <ext/hash_map>
#include "mempool2.h"
#include "delaypool.h"
#include "objectpool.h"
#include "hashtable.h"
#include "cJSON.h"
#include <google/protobuf/message.h>

class ForwardIndex
{
    private:
        typedef TDelayPool<VMemoryPool> Pool;
        typedef Pool::vaddr_t vaddr_t;
        typedef HashTable<int32_t, vaddr_t> Hash;
        typedef Hash::ObjectPool NodePool;
    public:
        enum { INT_TYPE = 0, FLOAT_TYPE, SELF_DEFINE_TYPE };

        class FieldIterator
        {
            public:
                FieldIterator(const ForwardIndex *idx, void *info)
                    : m_idx(idx)
                {
                    m_pos = 0;
                    m_info = info;
                }

                bool next(std::string &field_name, int &type, int *iv, float *fv, void **pv)
                {
                    *iv = 0;
                    *fv = 0;
                    *pv = NULL;
                    if (m_pos >= m_idx->m_field_names.size())
                    {
                        return false;
                    }
                    field_name = m_idx->m_field_names[m_pos].second;
                    __gnu_cxx::hash_map<std::string, FieldDes>::const_iterator it = m_idx->m_fields.find(field_name);
                    if (it != m_idx->m_fields.end())
                    {
                        if (it->second.type == INT_TYPE)
                        {
                            *iv = ((int *)m_info)[it->second.array_offset];
                        }
                        else if (it->second.type == FLOAT_TYPE)
                        {
                            *fv = ((float *)m_info)[it->second.array_offset];
                        }
                        else
                        {
                            *pv = ((void **)m_info)[it->second.array_offset];
                        }
                    }
                    ++m_pos;
                    return true;
                }
            private:
                size_t m_pos;
                void *m_info;
                const ForwardIndex *m_idx;
        };

        class iterator
        {
            public:
                iterator(const ForwardIndex *idx)
                    : m_idx(idx), m_it(m_idx->m_dict->begin())
                { }

                bool next(int32_t *docid, FieldIterator *it)
                {
                    if (!m_it)
                    {
                        return false;
                    }
                    if (docid)
                    {
                        *docid = m_it.key();
                    }
                    if (it)
                    {
                        *it = m_idx->get_field_iterator(m_idx->m_pool.addr(m_it.value()));
                    }
                    ++m_it;
                    return true;
                }
            private:
                const ForwardIndex *m_idx;
                Hash::iterator m_it;
        };

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
    private:
        ForwardIndex(const ForwardIndex &);
        ForwardIndex &operator =(const ForwardIndex &);
    public:
        ForwardIndex() { m_dict = NULL; }
        ~ForwardIndex();

        int init(const char *path, const char *file);

        int get_offset_by_name(const char *name) const;

        void *get_info_by_id(int32_t id) const;

        bool update(int32_t id, const std::string &key, const std::string &value)
        {
            std::vector<std::pair<std::string, std::string> > kvs;
            kvs.push_back(std::make_pair(key, value));
            return this->update(id, kvs);
        }
        bool update(int32_t id, const std::vector<std::pair<std::string, std::string> > &kvs);
        bool update(int32_t id, const std::vector<std::pair<std::string, cJSON *> > &kvs);

        void remove(int32_t id);

        void recycle()
        {
            m_pool.recycle();
        }
        void print_meta() const;

        bool load(const char *dir);
        bool dump(const char *dir) const;

        FieldIterator get_field_iterator(void *info) const
        {
            return FieldIterator(this, info);
        }
        iterator begin() const { return iterator(this); }
    private:
        struct cleanup_data_t
        {
            void *mem;
            Pool::vaddr_t addr;
            std::vector<int> fields_need_free;

            void clean()
            {
                if (NULL == mem)
                {
                    return ;
                }
                for (size_t i = 0; i < fields_need_free.size(); ++i)
                {
                    google::protobuf::Message *message =
                        (google::protobuf::Message *)((void **)mem)[fields_need_free[i]];
                    if (message)
                    {
                        delete message;
                    }
                }
            }
        };
        static void cleanup(Hash::node_t *node, intptr_t arg);
    private:
        Pool m_pool;
        NodePool m_node_pool;

        Hash *m_dict;

        __gnu_cxx::hash_map<std::string, FieldDes> m_fields;
        std::vector<std::pair<uint32_t, std::string> > m_field_names;
        std::vector<std::pair<uint32_t, double> > m_default_values;
        std::vector<const google::protobuf::Message *> m_default_messages;
        size_t m_info_size;
        std::string m_meta;

        cleanup_data_t m_cleanup_data;
        std::deque<cleanup_data_t> m_delayed_list;
};

#endif
