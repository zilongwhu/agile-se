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
#include "mempool.h"
#include "field_parser.h"
#include "hashtable.h"

class ForwardIndex
{
    public:
        enum { INT_TYPE = 0, FLOAT_TYPE, SELF_DEFINE_TYPE };

        struct FieldDes
        {
            int offset;
            int type; /* 0->int, 1->float, 2->void * */
            FieldParser *parser;
        };
    private:
        ForwardIndex(const ForwardIndex &);
        ForwardIndex &operator =(const ForwardIndex &);
    public:
        ForwardIndex() { m_dict = NULL; }
        ~ForwardIndex();

        int init(const char *path, const char *file);

        int get_offset_by_name(const char *name) const;

        void *get_info_by_id(long id) const;

        bool update(long id, const std::string &key, const std::string &value)
        {
            std::vector<std::pair<std::string, std::string> > kvs;
            kvs.push_back(std::make_pair(key, value));
            return this->update(id, kvs);
        }
        bool update(long id, const std::vector<std::pair<std::string, std::string> > &kvs);
        bool update(long id, const std::vector<std::pair<std::string, cJSON *> > &kvs);

        void remove(long id);
    private:
        struct cleanup_data_t
        {
            void *mem;
            std::vector<std::pair<int, FieldParser *> > fields_need_free;

            void clean()
            {
                if (NULL == mem)
                {
                    return ;
                }
                for (size_t i = 0; i < fields_need_free.size(); ++i)
                {
                    std::pair<int, FieldParser *> &tmp = fields_need_free[i];
                    void *ptr = ((void **)mem)[tmp.first >> 3];
                    if (ptr)
                    {
                        tmp.second->destroy(ptr);
                    }
                }
            }
        };
        static void cleanup(HashTable<long, void *>::node_t *node, void *arg)
        {
            ForwardIndex *ptr = (ForwardIndex *)arg;
            if (NULL == ptr || ptr->m_delayed_list.size() == 0)
            {
                ::abort();
            }
            cleanup_data_t &cd = ptr->m_delayed_list.front();
            if (node->value != cd.mem)
            {
                ::abort();
            }
            cd.clean();
            ptr->m_pool.free(cd.mem);
            ptr->m_delayed_list.pop_front();
        }
    private:
        MemoryPool m_pool;
        ObjectPool<HashTable<long, void *>::node_t> m_node_pool;

        HashTable<long, void *> *m_dict;
        __gnu_cxx::hash_map<std::string, FieldDes> m_fields;
        size_t m_info_size;

        std::deque<cleanup_data_t> m_delayed_list;
};

#endif
