// =====================================================================================
//
//       Filename:  delaypool.h
//
//    Description:  delay memory pool
//
//        Version:  1.0
//        Created:  04/23/2014 06:14:03 PM
//       Revision:  none
//       Compiler:  g++
//
//         Author:  Zilong.Zhu (), zilong.whu@gmail.com
//        Company:  edu.whu
//
// =====================================================================================

#ifndef __AGILE_SE_DELAY_MEMORY_POOL_H__
#define __AGILE_SE_DELAY_MEMORY_POOL_H__

#include <time.h>
#include <stdint.h>
#include "log.h"

template<typename TMemoryPool>
class TDelayPool
{
    public:
        typedef typename TMemoryPool::vaddr_t vaddr_t;
        const static vaddr_t null;
    public:
        typedef void (*destroy_fun_t)(void *ptr, void *arg);
    private:
        struct node_t
        {
            vaddr_t ptr;
            uint32_t push_time;
            uint32_t elem_size;
            destroy_fun_t fun;
            void *arg;
        };
        struct element_t
        {
            element_t *next;
        };
        struct queue_t
        {
            element_t *head;
            element_t *tail;
        };
    public:
        TDelayPool()
        {
            m_delayed_num = 0;
            m_delayed_time = 5;
            m_delayed_list.head = m_delayed_list.tail = NULL;
        }

        ~TDelayPool()
        {
            m_delayed_num = 0;
            m_delayed_time = 5;
            m_delayed_list.head = m_delayed_list.tail = NULL;
        }

        int register_item(uint32_t elem_size, uint32_t page_size)
        {
            return m_pool.register_item(elem_size, page_size);
        }

        int init(uint32_t max_items_num)
        {
            int ret = m_pool.init(max_items_num);
            if (ret < 0)
            {
                WARNING("failed to init m_pool");
                return -1;
            }
            ret = m_list_pool.register_item(sizeof(element_t) + sizeof(node_t), 1024*1024);
            if (ret < 0)
            {
                WARNING("failed to register item for m_list_pool");
                return -1;
            }
            ret = m_list_pool.init(1024*1024);
            if (ret < 0)
            {
                WARNING("failed to init m_list_pool");
                return -1;
            }
            WARNING("init ok");
            return 0;
        }

        void set_delayed_time(int delayed_seconds)
        {
            if (delayed_seconds > 0)
            {
                m_delayed_time = delayed_seconds;
            }
        }

        void *addr(vaddr_t ptr) const { return m_pool.addr(ptr); }
        vaddr_t alloc(uint32_t elem_size) { return m_pool.alloc(elem_size); }
        void free(vaddr_t ptr, uint32_t elem_size) { m_pool.free(ptr, elem_size); }

        int delay_free(vaddr_t ptr, uint32_t elem_size, destroy_fun_t fun = NULL, void *arg = NULL)
        {
            if (null == ptr)
            {
                return -1;
            }
            element_t *elem = (element_t *)m_list_pool.alloc(sizeof(element_t) + sizeof(node_t));
            if (NULL == elem)
            {
                WARNING("failed to alloc list node");
                return -2;
            }
            elem->next = NULL;
            node_t *node = (node_t *)(elem + 1);
            node->ptr = ptr;
            node->push_time = ::time(NULL);
            node->elem_size = elem_size;
            node->fun = fun;
            node->arg = arg;
            if (m_delayed_list.tail)
            {
                m_delayed_list.tail->next = elem;
                m_delayed_list.tail = elem;
            }
            else
            {
                m_delayed_list.head = m_delayed_list.tail = elem;
            }
            ++m_delayed_num;
            return 0;
        }

        void recycle()
        {
            const int now = ::time(NULL);
            node_t *node;
            MemoryPool::element_t *head;
            while (m_delayed_list.head)
            {
                head = m_delayed_list.head;
                node = (node_t *)(head + 1);
                if (node->push_time + m_delayed_time >= now)
                {
                    break;
                }
                if (node->fun)
                {
                    node->fun(this->addr(node->ptr), node->arg);
                }
                m_pool.free(node->ptr, node->elem_size);
                m_delayed_list.head = head->next;
                if (NULL == m_delayed_list.head)
                {
                    m_delayed_list.tail = NULL;
                }
                m_list_pool.free(head, sizeof(element_t) + sizeof(node_t));
                --m_delayed_num;
            }
        }
    private:
        TMemoryPool m_pool;
        TMemoryPool m_list_pool;

        size_t m_delayed_num;
        int m_delayed_time;

        queue_t m_delayed_list;
};

template<typename TMemoryPool> const typename TDelayPool<TMemoryPool>::vaddr_t TDelayPool<TMemoryPool>::null = TMemoryPool::null;

#endif
