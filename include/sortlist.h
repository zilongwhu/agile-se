// =====================================================================================
//
//       Filename:  sortlist.h
//
//    Description:  
//
//        Version:  1.0
//        Created:  03/04/2014 02:35:29 PM
//       Revision:  none
//       Compiler:  g++
//
//         Author:  Zilong.Zhu (), zilong.whu@gmail.com
//        Company:  edu.whu
//
// =====================================================================================

#ifndef __AGILE_SE_SORTLIST_H__
#define __AGILE_SE_SORTLIST_H__

#include <utility>
#include "mempool2.h"
#include "objectpool.h"

template<typename T, typename TMemoryPool = VMemoryPool>
class SortList
{
    public:
        typedef typename TMemoryPool::vaddr_t vaddr_t;

        struct node_t
        {
            vaddr_t next;
            T value;

            node_t(): next(0) { }
            node_t(const T &v): next(0), value(v) { }
        };

        typedef TObjectPool<node_t, TMemoryPool> ObjectPool;

        class iterator
        {
            public:
                iterator(vaddr_t cur, ObjectPool *pool)
                {
                    m_cur = cur;
                    m_pool = pool;
                }
                iterator & operator ++()
                {
                    if (0 != m_cur)
                    {
                        node_t *node = m_pool->addr(m_cur);
                        m_cur = node->next;
                    }
                    return *this;
                }
                iterator operator ++(int)
                {
                    iterator tmp(*this);
                    this->operator ++();
                    return tmp;
                }
                T &operator *() const
                {
                    return m_pool->addr(m_cur)->value;
                }
                T *operator ->() const
                {
                    return &m_pool->addr(m_cur)->value;
                }
                bool operator ==(const iterator &o) const
                {
                    return m_cur == o.m_cur;
                }
                bool operator !=(const iterator &o) const
                {
                    return m_cur != o.m_cur;
                }
            private:
                vaddr_t m_cur;
                ObjectPool *m_pool;
        };
    private:
        SortList(const SortList &);
        SortList &operator =(const SortList &);
    public:
        SortList(ObjectPool *pool)
        {
            m_pool = pool;
            m_head = 0;
            m_size = 0;
        }
        ~SortList()
        {
            vaddr_t cur;
            node_t *node;
            while (0 != m_head)
            {
                cur = m_head;
                node = m_pool->addr(cur);
                m_head = node->next;
                m_pool->delay_free(cur);
            }
            m_pool = NULL;
            m_size = 0;
        }

        size_t size() const { return m_size; }

        iterator begin() const
        {
            return iterator(m_head, m_pool);
        }
        iterator end() const
        {
            return iterator(0, m_pool);
        }

        T *find(const T &v) const
        {
            node_t *node;
            vaddr_t cur = m_head;
            while (0 != cur)
            {
                node = m_pool->addr(cur);
                if (!(node->value < v))
                {
                    break;
                }
                cur = node->next;
            }
            if (0 != cur && node->value == v)
            {
                return &node->value;
            }
            return NULL;
        }

        bool insert(const T &v)
        {
            vaddr_t vnew = m_pool->template alloc<const T &>(v);
            if (0 == vnew)
            {
                return false;
            }
            node_t *node;
            node_t *pre = NULL;
            vaddr_t cur = m_head;
            while (0 != cur)
            {
                node = m_pool->addr(cur);
                if (!(node->value < v))
                {
                    break;
                }
                pre = node;
                cur = node->next;
            }
            if (0 != cur && node->value == v)
            {
                if (pre)
                {
                    pre->next = node->next;
                }
                else
                {
                    m_head = node->next;
                }
                m_pool->delay_free(cur);
                --m_size;
            }
            node = m_pool->addr(vnew);
            if (pre)
            {
                node->next = pre->next;
                pre->next = vnew;
            }
            else
            {
                node->next = m_head;
                m_head = vnew;
            }
            ++m_size;
            return true;
        }

        void remove(const T &v)
        {
            node_t *node;
            node_t *pre = NULL;
            vaddr_t cur = m_head;
            while (0 != cur)
            {
                node = m_pool->addr(cur);
                if (!(node->value < v))
                {
                    break;
                }
                pre = node;
                cur = node->next;
            }
            if (0 != cur && node->value == v)
            {
                if (pre)
                {
                    pre->next = node->next;
                }
                else
                {
                    m_head = node->next;
                }
                m_pool->delay_free(cur);
                --m_size;
            }
        }
    private:
        vaddr_t m_head;
        size_t m_size;

        ObjectPool *m_pool;
};

#endif
