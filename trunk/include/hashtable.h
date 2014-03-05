// =====================================================================================
//
//       Filename:  hashtable.h
//
//    Description:  
//
//        Version:  1.0
//        Created:  03/05/2014 11:03:36 AM
//       Revision:  none
//       Compiler:  g++
//
//         Author:  Zilong.Zhu (), zilong.whu@gmail.com
//        Company:  edu.whu
//
// =====================================================================================

#ifndef __AGILE_SE_HASH_TABLE_H__
#define __AGILE_SE_HASH_TABLE_H__

#include <functional>
#include <ext/hash_fun.h>
#include "mempool.h"

template<typename Key, typename Value, typename HashFun = __gnu_cxx::hash<Key>, typename EqualFun = std::equal_to<Key> >
class HashTable
{
    public:
        struct node_t
        {
            node_t *next;
            Key key;
            Value value;

            node_t() : next(NULL) { }
            node_t(const Key &k, const Value &v) : next(NULL), key(k), value(v) { }
        };
    public:
        HashTable(ObjectPool<node_t> *pool, size_t bucket_size)
        {
            m_pool = pool;
            m_buckets = NULL;
            if (bucket_size > 0)
            {
                m_buckets = new node_t *[bucket_size];
                if (m_buckets)
                {
                    ::memset(m_buckets, 0, sizeof(node_t *) * bucket_size);
                }
            }
            m_bucket_size = bucket_size;
        }
        ~HashTable()
        {
            if (m_buckets)
            {
                delete [] m_buckets;
                m_buckets = NULL;
            }
            m_bucket_size = 0;
        }

        bool get(const Key &key, Value **pv = NULL) const
        {
            if (NULL == m_buckets)
            {
                return false;
            }
            size_t off = m_hash(key) % m_bucket_size;
            node_t *cur = m_buckets[off];
            while (cur)
            {
                if (m_equal(key, cur->key))
                {
                    if (pv)
                    {
                        *pv = &cur->value;
                    }
                    return true;
                }
                cur = cur->next;
            }
            return false;
        }
        bool set(const Key &key, const Value &v)
        {
            if (NULL == m_buckets)
            {
                return false;
            }
            size_t off = m_hash(key) % m_bucket_size;
            node_t *cur = m_buckets[off];
            while (cur)
            {
                if (m_equal(key, cur->key))
                {
                    cur->value = v;
                    return true;
                }
                cur = cur->next;
            }
            cur = m_pool->template alloc<const Key &, const Value &>(key, v);
            if (NULL == cur)
            {
                return false;
            }
            cur->next = m_buckets[off];
            m_buckets[off] = cur;
            return true;
        }
        bool remove(const Key &key, Value *pv = NULL)
        {
            if (NULL == m_buckets)
            {
                return false;
            }
            size_t off = m_hash(key) % m_bucket_size;
            node_t *pre = NULL;
            node_t *cur = m_buckets[off];
            while (cur)
            {
                if (m_equal(key, cur->key))
                {
                    if (pre)
                    {
                        pre->next = cur->next;
                    }
                    else
                    {
                        m_buckets[off] = cur->next;
                    }
                    if (pv)
                    {
                        *pv = cur->value;
                    }
                    m_pool->delay_free(cur);
                    return true;
                }
                pre = cur;
                cur = cur->next;
            }
            return false;
        }
    private:
        ObjectPool<node_t> *m_pool;

        node_t **m_buckets;
        size_t m_bucket_size;

        HashFun m_hash;
        EqualFun m_equal;
};

#endif
