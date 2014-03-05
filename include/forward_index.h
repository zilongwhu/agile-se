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
        ForwardIndex() { }
        ~ForwardIndex() { }

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

        void remove(long id);
    private:
        MemoryPool m_pool;
        ObjectPool<HashTable<long, void *>::node_t> m_node_pool;

        HashTable<long, void *> *m_dict;
        __gnu_cxx::hash_map<std::string, FieldDes> m_fields;
};

#endif
