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
#include "mempool.h"

template<typename T>
class SortList
{
    public:
        struct node_t
        {
            node_t *next;
            T value;

            node_t(): next(NULL) { }
            node_t(const T &v): next(NULL), value(v) { }
        };

        class iterator
        {
            public:
                iterator(node_t *node)
                {
                    m_cur = node;
                }
                iterator & operator ++()
                {
                    if (m_cur)
                    {
                        m_cur = m_cur->next;
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
                    return m_cur->value;
                }
                T *operator ->() const
                {
                    return &m_cur->value;
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
                node_t *m_cur;
        };
    private:
        SortList(const SortList &);
        SortList &operator =(const SortList &);
    public:
        SortList(ObjectPool<node_t> *pool)
        {
            m_pool = pool;
            m_head = NULL;
            m_size = 0;
        }
        ~SortList()
        {
            while (m_head)
            {
                node_t *cur = m_head;
                m_head = m_head->next;
                m_pool->delay_free(cur);
            }
            m_pool = NULL;
            m_size = 0;
        }

        size_t size() const { return m_size; }

        iterator begin() const
        {
            return iterator(m_head);
        }
        iterator end() const
        {
            return iterator(NULL);
        }

        T *find(const T &v) const
        {
            node_t *cur = m_head;
            while (cur && cur->value < v)
            {
                cur = cur->next;
            }
            if (cur && cur->value == v)
            {
                return &cur->value;
            }
            return NULL;
        }

        bool insert(const T &v)
        {
            node_t *pre = NULL;
            node_t *cur = m_head;
            while (cur && cur->value < v)
            {
                pre = cur;
                cur = cur->next;
            }
            if (cur && cur->value == v)
            {
                if (pre)
                {
                    pre->next = cur->next;
                }
                else
                {
                    m_head = cur->next;
                }
                m_pool->delay_free(cur);
                --m_size;
            }
            node_t *tmp = m_pool->template alloc<const T &>(v);
            if (NULL == tmp)
            {
                return false;
            }
            if (pre)
            {
                tmp->next = pre->next;
                pre->next = tmp;
            }
            else
            {
                tmp->next = m_head;
                m_head = tmp;
            }
            ++m_size;
            return true;
        }

        void remove(const T &v)
        {
            node_t *pre = NULL;
            node_t *cur = m_head;
            while (cur && cur->value < v)
            {
                pre = cur;
                cur = cur->next;
            }
            if (cur && cur->value == v)
            {
                if (pre)
                {
                    pre->next = cur->next;
                }
                else
                {
                    m_head = cur->next;
                }
                m_pool->delay_free(cur);
                --m_size;
            }
        }

        void recycle()
        {
            m_pool->recycle();
        }
    private:
        node_t *m_head;
        size_t m_size;

        ObjectPool<node_t> *m_pool;
};

#endif