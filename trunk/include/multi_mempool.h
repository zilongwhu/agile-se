// =====================================================================================
//
//       Filename:  multi_mempool.h
//
//    Description:  
//
//        Version:  1.0
//        Created:  04/28/2014 10:46:12 AM
//       Revision:  none
//       Compiler:  g++
//
//         Author:  Zilong.Zhu (), zilong.whu@gmail.com
//        Company:  edu.whu
//
// =====================================================================================

#ifndef __AGILE_SE_MULTI_MEMORY_POOL_H__
#define __AGILE_SE_MULTI_MEMORY_POOL_H__

#include <map>
#include "mempool.h"

class MultiMemoryPool
{
    public:
        typedef MemoryPool::vaddr_t vaddr_t;
    private:
        MultiMemoryPool(const MultiMemoryPool &);
        MultiMemoryPool &operator =(const MultiMemoryPool &);
    public:
        MultiMemoryPool()
        {
            m_max_items_num = 0;
        }

        ~MultiMemoryPool()
        {
            std::map<uint32_t, MemoryPool *>::iterator it = m_pools.begin();
            while (it != m_pools.end())
            {
                if (it->second)
                {
                    delete it->second;
                }
                ++it;
            }
            m_pools.clear();
            m_max_items_num = 0;
        }

        int register_item(uint32_t elem_size, uint32_t page_size = 0)
        {
            if (elem_size <= 0)
            {
                WARNING("invalid args: elem_size=%u", elem_size);
                return -1;
            }
            m_pools.insert(std::make_pair(elem_size, (MemoryPool *)NULL));
            WARNING("register item: elem_size=%u", elem_size);
            return 0;
        }

        int init(uint32_t max_items_num)
        {
            if (max_items_num <= 0)
            {
                WARNING("invalid args: max_items_num=%u", max_items_num);
                return -1;
            }
            std::map<uint32_t, MemoryPool *>::iterator it = m_pools.begin();
            while (it != m_pools.end())
            {
                if (NULL == it->second)
                {
                    it->second = new MemoryPool();
                }
                if (NULL == it->second)
                {
                    WARNING("failed to alloc MemoryPool");
                    return -1;
                }
                if (0 > it->second->init(it->first, MemoryPool::adjust_elem_size(it->first) * max_items_num))
                {
                    WARNING("failed to init MemoryPool, elem_size=%u, max_items_num=%u", it->first, max_items_num);
                    return -1;
                }
                ++it;
            }
            WARNING("init ok, elems num=%u, max_items_num=%u", (uint32_t)m_pools.size(), max_items_num);
            return 0;
        }

        vaddr_t alloc(uint32_t elem_size)
        {
            std::map<uint32_t, MemoryPool *>::iterator it = m_pools.find(elem_size);
            if (it == m_pools.end())
            {
                WARNING("unregistered elem size: %u", elem_size);
                return NULL;
            }
            if (NULL == it->second)
            {
                WARNING("not init elem size: %u", elem_size);
                return NULL;
            }
            return it->second->alloc();
        }

        void free(vaddr_t ptr, uint32_t elem_size)
        {
            if (NULL == ptr)
            {
                return ;
            }
            std::map<uint32_t, MemoryPool *>::iterator it = m_pools.find(elem_size);
            if (it == m_pools.end())
            {
                WARNING("unregistered elem size: %u", elem_size);
                return ;
            }
            if (NULL == it->second)
            {
                WARNING("not init elem size: %u", elem_size);
                return ;
            }
            it->second->free(ptr);
        }

        void *addr(vaddr_t ptr) const { return ptr; }

        void clear()
        {
            std::map<uint32_t, MemoryPool *>::iterator it = m_pools.begin();
            while (it != m_pools.end())
            {
                if (it->second)
                {
                    it->second->clear();
                }
                ++it;
            }
        }
    private:
        std::map<uint32_t, MemoryPool *> m_pools;
        uint32_t m_max_items_num;
};

#endif
