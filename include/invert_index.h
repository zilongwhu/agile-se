// =====================================================================================
//
//       Filename:  invert_index.h
//
//    Description:  
//
//        Version:  1.0
//        Created:  03/06/2014 08:39:45 PM
//       Revision:  none
//       Compiler:  g++
//
//         Author:  Zilong.Zhu (), zilong.whu@gmail.com
//        Company:  edu.whu
//
// =====================================================================================

#ifndef __AGILE_SE_INVERT_INDEX_H__
#define __AGILE_SE_INVERT_INDEX_H__

#include <stdint.h>
#include "mempool.h"
#include "idlist.h"
#include "hashtable.h"

class InvertIndex
{
    private:
        InvertIndex(const InvertIndex &);
        InvertIndex &operator =(const InvertIndex &);
    public:
        InvertIndex() { }
        ~ InvertIndex() { }
    private:
        ObjectPool<HashTable<uint64_t, void *>::node_t> m_node_pool;
        ObjectPool<HashTable<uint64_t, IDList *>::node_t> m_diff_node_pool;

        ObjectPool<IDList> m_list_pool;
        __gnu_cxx::hash_map<size_t, DelayPool *> m_list_pools;

        HashTable<uint64_t, void *> *m_dict;
        HashTable<uint64_t, IDList *> *m_add_dict;
        HashTable<uint64_t, IDList *> *m_del_dict;
};

#endif
