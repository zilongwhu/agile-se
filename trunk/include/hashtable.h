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

#include <string>
#include <functional>
#include <ext/hash_fun.h>
#include "mempool2.h"
#include "objectpool.h"

namespace __gnu_cxx
{
    template<> struct hash<std::string>
    {
        size_t operator()(const std::string &__s) const
        {
            hash<const char *> tmp;
            return tmp(__s.c_str());
        }
    };
};

namespace std
{
    template <> struct equal_to<const char *>:
        public binary_function<const char *, const char *, bool>
        {
            bool operator()(const char *const & __x, const char *const & __y) const
            { return ::strcmp(__x, __y) == 0; }
        };
}

template<typename Key, typename Value,
    typename HashFun = __gnu_cxx::hash<Key>,
    typename EqualFun = std::equal_to<Key>,
    typename TMemoryPool = VMemoryPool>
class HashTable
{
    public:
        typedef typename TMemoryPool::vaddr_t vaddr_t;

        struct node_t
        {
            vaddr_t next;
            Key key;
            Value value;

            node_t() : next(0) { }
            node_t(const Key &k, const Value &v) : next(0), key(k), value(v) { }
        };

        typedef TObjectPool<node_t, TMemoryPool> ObjectPool;
        typedef typename ObjectPool::cleanup_fun_t cleanup_fun_t;
    public:
        class iterator
        {
            public:
                iterator(const HashTable *table)
                    : m_table(table)
                {
                    m_pos = 0;
                    m_cur = 0;
                    if (m_table)
                    {
                        for (; m_pos < m_table->m_bucket_size; ++m_pos)
                        {
                            if (m_table->m_buckets[m_pos])
                            {
                                m_cur = m_table->m_buckets[m_pos];
                                break;
                            }
                        }
                    }
                }
                ~iterator()
                {
                    m_pos = 0;
                    m_cur = 0;
                }
                iterator & operator ++()
                {
                    if (m_table)
                    {
                        if (0 != m_cur)
                        {
                            node_t *node = m_table->m_pool->addr(m_cur);
                            m_cur = node->next;
                            if (0 == m_cur)
                            {
                                ++m_pos;
                                for (; m_pos < m_table->m_bucket_size; ++m_pos)
                                {
                                    if (m_table->m_buckets[m_pos])
                                    {
                                        m_cur = m_table->m_buckets[m_pos];
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    return *this;
                }
                iterator operator ++(int)
                {
                    iterator tmp(*this);
                    this->operator ++();
                    return tmp;
                }
                operator bool () const
                {
                    return 0 != m_cur;
                }
                const Key &key() const
                {
                    return m_table->m_pool->addr(m_cur)->key;
                }
                Value &value() const
                {
                    return m_table->m_pool->addr(m_cur)->value;
                }
            private:
                const HashTable *const m_table;
                size_t m_pos;
                vaddr_t m_cur;
        };
    private:
        HashTable(const HashTable &);
        HashTable &operator =(const HashTable &);
    public:
        HashTable(size_t bucket_size)
        {
            m_pool = NULL;
            m_cleanup_fun = NULL;
            m_cleanup_arg = 0;
            m_buckets = NULL;
            if (bucket_size > 0)
            {
                m_buckets = new vaddr_t [bucket_size];
                if (m_buckets)
                {
                    ::memset(m_buckets, 0, sizeof(vaddr_t) * bucket_size);
                }
            }
            m_bucket_size = bucket_size;
            m_size = 0;
        }
        ~HashTable()
        {
            if (m_buckets)
            {
                if (m_size > 0)
                {
                    vaddr_t cur;
                    node_t *node;
                    for (size_t i = 0; i < m_bucket_size; ++i)
                    {
                        while (0 != m_buckets[i])
                        {
                            cur = m_buckets[i];
                            node = m_pool->addr(cur);
                            m_buckets[i] = node->next;
                            m_pool->delay_free(cur, m_cleanup_fun, m_cleanup_arg);
                        }
                    }
                }
                delete [] m_buckets;
                m_buckets = NULL;
            }
            m_bucket_size = 0;
            m_size = 0;
            m_pool = NULL;
            m_cleanup_fun = NULL;
            m_cleanup_arg = 0;
        }

        void set_pool(ObjectPool *pool) { m_pool = pool; }
        void set_cleanup(cleanup_fun_t fun, intptr_t arg)
        {
            m_cleanup_fun = fun;
            m_cleanup_arg = arg;
        }

        size_t bucket_size() const { return m_bucket_size; }
        size_t size() const { return m_size; }
        size_t mem_used() const
        {
            return sizeof(*this)
                + sizeof(vaddr_t) * m_bucket_size
                + m_size * sizeof(node_t);
        }

        iterator begin() const
        {
            return iterator(this);
        }

        Value *find(const Key &key) const
        {
            if (NULL == m_buckets)
            {
                return NULL;
            }
            size_t off = m_hash(key) % m_bucket_size;
            node_t *node;
            vaddr_t cur = m_buckets[off];
            while (0 != cur)
            {
                node = m_pool->addr(cur);
                if (m_equal(key, node->key))
                {
                    return &node->value;
                }
                cur = node->next;
            }
            return NULL;
        }

        bool insert(const Key &key, const Value &v)
        {
            if (NULL == m_buckets)
            {
                return false;
            }
            vaddr_t vnew = m_pool->template alloc<const Key &, const Value &>(key, v);
            if (0 == vnew)
            {
                return false;
            }
            size_t off = m_hash(key) % m_bucket_size;

            node_t *node;
            node_t *pre = NULL;
            bool overwrite = false;
            vaddr_t cur = m_buckets[off];
            while (0 != cur)
            {
                node = m_pool->addr(cur);
                if (m_equal(key, node->key))
                {
                    if (pre)
                    {
                        pre->next = node->next;
                    }
                    else
                    {
                        m_buckets[off] = node->next;
                    }
                    overwrite = true;
                    break;
                }
                pre = node;
                cur = node->next;
            }
            node = m_pool->addr(vnew);
            if (pre)
            {
                node->next = pre->next;
                pre->next = vnew;
            }
            else
            {
                node->next = m_buckets[off];
                m_buckets[off] = vnew;
            }
            if (overwrite)
            {
                m_pool->delay_free(cur, m_cleanup_fun, m_cleanup_arg);
            }
            else
            {
                ++m_size;
            }
            return true;
        }

        bool remove(const Key &key, Value *pv = NULL)
        {
            if (NULL == m_buckets)
            {
                return false;
            }
            size_t off = m_hash(key) % m_bucket_size;
            node_t *node;
            node_t *pre = NULL;
            vaddr_t cur = m_buckets[off];
            while (0 != cur)
            {
                node = m_pool->addr(cur);
                if (m_equal(key, node->key))
                {
                    if (pre)
                    {
                        pre->next = node->next;
                    }
                    else
                    {
                        m_buckets[off] = node->next;
                    }
                    if (pv)
                    {
                        *pv = node->value;
                    }
                    m_pool->delay_free(cur, m_cleanup_fun, m_cleanup_arg);
                    --m_size;
                    return true;
                }
                pre = node;
                cur = node->next;
            }
            return false;
        }
    private:
        ObjectPool *m_pool;
        cleanup_fun_t m_cleanup_fun;
        intptr_t m_cleanup_arg;

        vaddr_t *m_buckets;
        size_t m_bucket_size;
        size_t m_size;

        HashFun m_hash;
        EqualFun m_equal;
};

#endif
