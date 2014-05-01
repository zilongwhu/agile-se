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
#include "field_parser.h"

class ForwardIndex
{
    public:
        enum { INT_TYPE = 0, FLOAT_TYPE, SELF_DEFINE_TYPE };

        struct FieldDes
        {
            int offset;
            int array_offset;
            int type; /* 0->int, 1->float, 2->void * */
            FieldParser *parser;
        };
    private:
        typedef TDelayPool<VMemoryPool> Pool;
        typedef Pool::vaddr_t vaddr_t;
        typedef HashTable<long, vaddr_t> Hash;
        typedef Hash::ObjectPool NodePool;
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

        void recycle()
        {
            m_pool.recycle();
        }
    private:
        struct cleanup_data_t
        {
            void *mem;
            Pool::vaddr_t addr;
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
                    void *ptr = ((void **)mem)[tmp.first];
                    if (ptr)
                    {
                        tmp.second->destroy(ptr);
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
        size_t m_info_size;

        cleanup_data_t m_cleanup_data;
        std::deque<cleanup_data_t> m_delayed_list;
};

#endif
