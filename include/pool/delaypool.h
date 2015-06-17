#ifndef __AGILE_SE_DELAY_MEMORY_POOL_H__
#define __AGILE_SE_DELAY_MEMORY_POOL_H__

#include <stdint.h>
#include "utils/log.h"

extern volatile uint32_t g_now_time;

template<typename TMemoryPool>
class TDelayPool
{
    public:
        typedef typename TMemoryPool::vaddr_t vaddr_t;
    public:
        typedef void (*destroy_fun_t)(void *ptr, intptr_t arg);
    private:
        struct node_t
        {
            vaddr_t next;
            vaddr_t ptr;
            uint32_t push_time;
            uint32_t elem_size;
            destroy_fun_t fun;
            intptr_t arg;
        };
        struct queue_t
        {
            vaddr_t head;
            vaddr_t tail;
        };
    public:
        TDelayPool()
        {
            m_delayed_num = 0;
            m_delayed_time = 5;
            m_delayed_list.head = m_delayed_list.tail = 0;
        }

        ~TDelayPool()
        {
            m_delayed_num = 0;
            m_delayed_time = 5;
            m_delayed_list.head = m_delayed_list.tail = 0;
        }

        int register_item(uint32_t elem_size)
        {
            return m_pool.register_item(elem_size);
        }

        int init(uint32_t max_items_num)
        {
            int ret = m_pool.register_item(sizeof(node_t));
            if (ret < 0)
            {
                P_WARNING("failed to register for node_t");
                return -1;
            }
            ret = m_pool.init(max_items_num);
            if (ret < 0)
            {
                P_WARNING("failed to init m_pool");
                return -1;
            }
            P_WARNING("init ok");
            return 0;
        }

        void set_delayed_time(uint32_t delayed_seconds)
        {
            m_delayed_time = delayed_seconds;
        }

        void *addr(vaddr_t ptr) const { return m_pool.addr(ptr); }
        vaddr_t alloc(uint32_t elem_size) { return m_pool.alloc(elem_size); }
        void free(vaddr_t ptr, uint32_t elem_size) { m_pool.free(ptr, elem_size); }

        int delay_free(vaddr_t ptr, uint32_t elem_size, destroy_fun_t fun = NULL, intptr_t arg = 0)
        {
            if (0 == ptr)
            {
                return -1;
            }
            if (!m_delayed_time) /* 等于0时，立即回收 */
            {
                if (fun)
                {
                    (*fun)(m_pool.addr(ptr), arg);
                }
                m_pool.free(ptr, elem_size);
                return 0;
            }
            vaddr_t addr = m_pool.alloc(sizeof(node_t));
            node_t *node = (node_t *)m_pool.addr(addr);
            if (NULL == node)
            {
                P_WARNING("failed to alloc list node");
                return -2;
            }
            node->next = 0;
            node->ptr = ptr;
            node->push_time = g_now_time;
            node->elem_size = elem_size;
            node->fun = fun;
            node->arg = arg;
            if (0 != m_delayed_list.tail)
            {
                node = (node_t *)m_pool.addr(m_delayed_list.tail);
                if (NULL == node)
                {
                    P_WARNING("internal error");

                    m_pool.free(addr, sizeof(node_t));
                    return -3;
                }
                node->next = addr;
                m_delayed_list.tail = addr;
            }
            else
            {
                m_delayed_list.head = m_delayed_list.tail = addr;
            }
            ++m_delayed_num;
            return 0;
        }

        void recycle()
        {
            const uint32_t now = g_now_time;
            vaddr_t cur;
            node_t *node;
            while (0 != m_delayed_list.head)
            {
                cur = m_delayed_list.head;
                node = (node_t *)m_pool.addr(cur);
                if (m_delayed_time /* 等于0时，立即回收 */
                        && node->push_time + m_delayed_time >= now)
                {
                    break;
                }
                if (node->fun)
                {
                    node->fun(m_pool.addr(node->ptr), node->arg);
                }
                m_pool.free(node->ptr, node->elem_size);
                m_delayed_list.head = node->next;
                if (0 == m_delayed_list.head)
                {
                    m_delayed_list.tail = 0;
                }
                m_pool.free(cur, sizeof(node_t));
                --m_delayed_num;
            }
        }

        void print_meta() const
        {
            m_pool.print_meta();
            P_WARNING("delayed elem num=%lu", (uint64_t)m_delayed_num);
        }
    private:
        TMemoryPool m_pool;

        size_t m_delayed_num;
        uint32_t m_delayed_time;

        queue_t m_delayed_list;
};

#endif
