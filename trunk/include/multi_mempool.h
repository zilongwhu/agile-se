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

#include <ext/hash_map>
#include "mempool.h"

#ifndef AGILE_SE_PATE_SIZE
#define AGILE_SE_PAGE_SIZE  (1024*1024) /* 1M */
#endif

class MultiMemoryPool
{
    public:
        typedef MemoryPool::vaddr_t vaddr_t;
    private:
        typedef __gnu_cxx::hash_map<uint32_t, MemoryPool *> Map;
    private:
        MultiMemoryPool(const MultiMemoryPool &);
        MultiMemoryPool &operator =(const MultiMemoryPool &);
    public:
        MultiMemoryPool() { }

        ~MultiMemoryPool()
        {
            Map::iterator it = m_pools.begin();
            while (it != m_pools.end())
            {
                if (it->second)
                {
                    delete it->second;
                }
                ++it;
            }
            m_pools.clear();
        }

        int register_item(uint32_t elem_size)
        {
            if (elem_size <= 0)
            {
                WARNING("invalid args: elem_size=%u", elem_size);
                return -1;
            }
            m_pools[elem_size] = NULL;
            WARNING("register item: elem_size=%u", elem_size);
            return 0;
        }

        int init(uint32_t /*max_items_num*/)
        {
            m_pools_end = m_pools.end();
            Map::iterator it = m_pools.begin();
            while (it != m_pools_end)
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
                if (0 > it->second->init(it->first, AGILE_SE_PAGE_SIZE))
                {
                    WARNING("failed to init MemoryPool, elem_size=%u, page_size=%u", it->first, AGILE_SE_PAGE_SIZE);
                    return -1;
                }
                ++it;
            }
            WARNING("init ok, elems num=%u, page_size=%u", (uint32_t)m_pools.size(), AGILE_SE_PAGE_SIZE);
            return 0;
        }

        vaddr_t alloc(uint32_t elem_size)
        {
            Map::iterator it = m_pools.find(elem_size);
            if (it == m_pools_end)
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
            Map::iterator it = m_pools.find(elem_size);
            if (it == m_pools_end)
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
            Map::iterator it = m_pools.begin();
            while (it != m_pools_end)
            {
                if (it->second)
                {
                    it->second->clear();
                }
                ++it;
            }
        }

        void print_meta() const
        {
            int i = 0;
            size_t mem_used;
            size_t total_used = 0;
            Map::const_iterator it = m_pools.begin();
            while (it != m_pools.end())
            {
                if (it->second)
                {
                    mem_used = it->second->mem();
                    total_used += mem_used;
                    WARNING("pool[%d]: elem_size=%u, mem used=%lu", i, it->first, (uint64_t)mem_used);
                    ++i;
                }
                ++it;
            }
            WARNING("total mem used=%lu", (uint64_t)total_used);
        }
    private:
        Map m_pools;
        Map::iterator m_pools_end;
};

#endif
