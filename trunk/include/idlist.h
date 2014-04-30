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
#include "mempool2.h"
#include "delaypool.h"

template<typename TMemoryPool = VMemoryPool>
class TIDList
{
    private:
        typedef typename TMemoryPool::vaddr_t vaddr_t;

        struct node_t
        {
            vaddr_t next;
            int id;
            uint8_t value[];
        };

        typedef TDelayPool<TMemoryPool> DelayPool;
    public:
        class iterator
        {
            public:
                iterator(vaddr_t cur, DelayPool *pool)
                {
                    m_cur = cur;
                    m_pool = pool;
                }
                iterator & operator ++()
                {
                    if (0 != m_cur)
                    {
                        node_t *node = (node_t *)m_pool->addr(m_cur);
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
                int operator *() const
                {
                    return ((node_t *)m_pool->addr(m_cur))->id;
                }
                void *payload()
                {
                    return ((node_t *)m_pool->addr(m_cur))->value;
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
                DelayPool *m_pool;
        };
    private:
        TIDList(const TIDList &);
        TIDList &operator =(const TIDList &);
    public:
        TIDList(DelayPool *pool, uint32_t payload_len)
        {
            m_pool = pool;
            m_payload_len = payload_len;
            m_element_size = element_size(payload_len);
            m_head = 0;
            m_size = 0;
        }
        ~TIDList()
        {
            node_t *node;
            vaddr_t cur;
            while (0 != m_head)
            {
                cur = m_head;
                node = (node_t *)m_pool->addr(cur);
                m_head = node->next;
                m_pool->delay_free(cur, m_element_size);
            }
            m_pool = NULL;
            m_payload_len = 0;
            m_element_size = 0;
            m_size = 0;
        }

        uint32_t payload_len() const { return m_payload_len; }
        uint32_t size() const { return m_size; }

        iterator begin() const
        {
            return iterator(m_head, m_pool);
        }
        iterator end() const
        {
            return iterator(0, m_pool);
        }

        bool find(int id, void **payload = NULL) const
        {
            node_t *node;
            vaddr_t cur = m_head;
            while (0 != cur)
            {
                node = (node_t *)m_pool->addr(cur);
                if (node->id < id)
                {
                    cur = node->next;
                }
                else if (node->id == id)
                {
                    if (payload)
                    {
                        *payload = &node->value;
                    }
                    return true;
                }
                else
                {
                    return false;
                }
            }
            return false;
        }
        bool insert(int id, void *payload)
        {
            vaddr_t vnew = m_pool->alloc(m_element_size);
            if (0 == vnew)
            {
                return false;
            }
            node_t *node;
            node_t *pre = NULL;
            vaddr_t cur = m_head;
            while (0 != cur)
            {
                node = (node_t *)m_pool->addr(cur);
                if (node->id < id)
                {
                    pre = node;
                    cur = node->next;
                }
                else if (node->id == id)
                {
                    if (pre)
                    {
                        pre->next = node->next;
                    }
                    else
                    {
                        m_head = node->next;
                    }
                    m_pool->delay_free(cur, m_element_size);
                    --m_size;
                    break;
                }
                else
                {
                    break;
                }
            }
            node = (node_t *)m_pool->addr(vnew);
            node->id = id;
            if (m_payload_len > 0)
            {
                ::memcpy(node->value, payload, m_payload_len);
            }
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
        void remove(int id)
        {
            node_t *node;
            node_t *pre = NULL;
            vaddr_t cur = m_head;
            while (0 != cur)
            {
                node = (node_t *)m_pool->addr(cur);
                if (node->id < id)
                {
                    pre = node;
                    cur = node->next;
                }
                else if (node->id == id)
                {
                    if (pre)
                    {
                        pre->next = node->next;
                    }
                    else
                    {
                        m_head = node->next;
                    }
                    m_pool->delay_free(cur, m_element_size);
                    --m_size;
                    return ;
                }
                else
                {
                    return ;
                }
            }
        }
    public:
        static uint32_t element_size(uint32_t payload_len)
        {
            return payload_len + sizeof(int) + sizeof(vaddr_t);
        }
    private:
        DelayPool *m_pool;
        uint32_t m_payload_len;
        uint32_t m_element_size;

        vaddr_t m_head;
        uint32_t m_size;
};

#endif
