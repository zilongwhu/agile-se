// =====================================================================================
//
//       Filename:  idlist.h
//
//    Description:  
//
//        Version:  1.0
//        Created:  03/04/2014 04:42:33 PM
//       Revision:  none
//       Compiler:  g++
//
//         Author:  Zilong.Zhu (), zilong.whu@gmail.com
//        Company:  edu.whu
//
// =====================================================================================

#ifndef __AGILE_SE_IDLIST_H__
#define __AGILE_SE_IDLIST_H__

#include <stdint.h>
#include <string.h>
#include "mempool.h"

class IDList
{
    private:
        struct node_t
        {
            node_t *next;
            int id;
            uint8_t value[];
        };
    public:
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
                int operator *() const
                {
                    return m_cur->id;
                }
                void *payload()
                {
                    return m_cur->value;
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
    public:
        IDList(DelayPool *pool, size_t payload_len)
        {
            m_pool = pool;
            m_payload_len = payload_len;
            m_head = NULL;
            m_size = 0;
        }
        ~IDList()
        {
            while (m_head)
            {
                node_t *cur = m_head;
                m_head = m_head->next;
                m_pool->delay_free(cur);
            }
            m_pool = NULL;
            m_payload_len = 0;
            m_size = 0;
        }

        size_t payload_len() const { return m_payload_len; }
        size_t size() const { return m_size; }

        iterator begin() const
        {
            return iterator(m_head);
        }
        iterator end() const
        {
            return iterator(NULL);
        }

        bool find(int id, void **payload = NULL) const
        {
            node_t *cur = m_head;
            while (cur && cur->id < id)
            {
                cur = cur->next;
            }
            if (cur && cur->id == id)
            {
                if (payload)
                {
                    *payload = &cur->value;
                }
                return true;
            }
            return false;
        }
        bool insert(int id, void *payload)
        {
            node_t *pre = NULL;
            node_t *cur = m_head;
            while (cur && cur->id < id)
            {
                pre = cur;
                cur = cur->next;
            }
            if (cur && cur->id == id)
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
            node_t *tmp = (node_t *)m_pool->alloc();
            if (NULL == tmp)
            {
                return false;
            }
            tmp->id = id;
            if (m_payload_len > 0)
            {
                ::memcpy(tmp->value, payload, m_payload_len);
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
        void remove(int id)
        {
            node_t *pre = NULL;
            node_t *cur = m_head;
            while (cur && cur->id < id)
            {
                pre = cur;
                cur = cur->next;
            }
            if (cur && cur->id == id)
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
    public:
        static size_t element_size(size_t payload_len)
        {
            return payload_len + sizeof(int) + sizeof(void *);
        }
    private:
        DelayPool *m_pool;
        size_t m_payload_len;

        node_t *m_head;
        size_t m_size;
};

#endif
