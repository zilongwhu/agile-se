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
    public:
        enum { INT_TYPE = 0, FLOAT_TYPE, SELF_DEFINE_TYPE };

        struct FieldDes
        {
            int offset;
            int array_offset;
            int type; /* 0->int, 1->float, 2->google::protobuf::Message * */
            const google::protobuf::Message *default_message;
        };
    private:
        typedef TDelayPool<VMemoryPool> Pool;
        typedef Pool::vaddr_t vaddr_t;
        typedef HashTable<int32_t, vaddr_t> Hash;
        typedef Hash::ObjectPool NodePool;
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
        size_t m_info_size;

        cleanup_data_t m_cleanup_data;
        std::deque<cleanup_data_t> m_delayed_list;
};

#endif
