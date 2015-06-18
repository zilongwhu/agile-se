#ifndef __AGILE_SE_MEMPOOL2_H__
#define __AGILE_SE_MEMPOOL2_H__

#include <stdlib.h>
#include <malloc.h>
#include <new>
#include <vector>
#include <ext/hash_map>
#include "log_utils.h"

namespace mem_detail
{
    class MemoryPool
    {
        public:
            typedef void *vaddr_t;
        public:
            struct element_t
            {
                element_t *next;
            };
        private:
            MemoryPool(const MemoryPool &);
            MemoryPool &operator =(const MemoryPool &);
        public:
            MemoryPool()
            {
                m_page_size = 0;
                m_elem_size = 0;
                m_alloc_num = 0;
                m_free_num = 0;
                m_free_list = NULL;
            }
    
            ~MemoryPool()
            {
                this->clear();
            }
    
            int init(size_t elem_size, size_t page_size)
            {
                this->clear();
                if (0 == elem_size || 0 ==  page_size)
                {
                    P_WARNING("invalid args: elem_size=%u, page_size=%u", elem_size, page_size);
                    return -1;
                }
                m_elem_size = adjust_elem_size(elem_size);
                if (m_elem_size > page_size)
                {
                    P_WARNING("page_size is too small: elem_size=%u, page_size=%u", elem_size, page_size);
                    return -2;
                }
                m_page_size = page_size;
                P_WARNING("init ok, elem_size=%u, page_size=%u", m_elem_size, m_page_size);
                return 0;
            }
    
            vaddr_t alloc()
            {
                if (m_free_list)
                {
                    vaddr_t ptr = (vaddr_t *)m_free_list;
                    m_free_list = m_free_list->next;
                    ++m_alloc_num;
                    --m_free_num;
                    return ptr;
                }
#if (1)
                void *page = ::memalign(64, m_page_size); /* cpu cache line alginment */
#else
                void *page = ::malloc(m_page_size);
#endif
                if (NULL == page)
                {
                    P_WARNING("failed to alloc new page, page_size=%u", m_page_size);
                    return NULL;
                }
                try
                {
                    m_pages.push_back(page);
                }
                catch (...)
                {
                    ::free(page);
    
                    P_WARNING("failed to push back new page, pages_size=%u", (uint32_t)m_pages.size());
                    return NULL;
                }
                const int num = m_page_size / m_elem_size;
                for (int i = 0; i < num; ++i)
                {
                    element_t *elem = (element_t *)(((char *)page) + m_elem_size * i);
                    elem->next = m_free_list;
                    m_free_list = elem;
                }
                m_free_num += num;
                return this->alloc();
            }
    
            void free(vaddr_t ptr)
            {
                if (NULL == ptr)
                {
                    return ;
                }
                element_t *elem = (element_t *)this->addr(ptr);
                elem->next = m_free_list;
                m_free_list = elem;
                --m_alloc_num;
                ++m_free_num;
            }
    
            void *addr(vaddr_t ptr) const { return ptr; }
    
            size_t page_size() const { return m_page_size; }
            size_t elem_size() const { return m_elem_size; }
            size_t alloc_num() const { return m_alloc_num; }
            size_t free_num() const { return m_free_num; }
    
            size_t mem() const
            {
                return (sizeof(void *) + m_page_size) * m_pages.size();
            }
    
            void clear()
            {
                for (size_t i = 0; i < m_pages.size(); ++i)
                {
                    ::free(m_pages[i]);
                }
                m_pages.clear();
                m_alloc_num = 0;
                m_free_num = 0;
                m_free_list = NULL;
            }
        public:
            static size_t adjust_elem_size(size_t elem_size)
            {
                return (elem_size + sizeof(void *) - 1)/sizeof(void *)*sizeof(void *);
            }
        private:
            std::vector<void *> m_pages;
            size_t m_page_size;
            size_t m_elem_size;
    
            size_t m_alloc_num;
            size_t m_free_num;
    
            element_t *m_free_list;
    };
};

#ifndef AGILE_SE_MEM_PAGE_SIZE
#define AGILE_SE_MEM_PAGE_SIZE   (1024*1024u)
#endif

class MultiMemoryPool
{
    public:
        typedef mem_detail::MemoryPool::vaddr_t vaddr_t;
    private:
        typedef __gnu_cxx::hash_map<uint32_t, mem_detail::MemoryPool *> Map;
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
                P_WARNING("invalid args: elem_size=%u", elem_size);
                return -1;
            }
            m_pools[elem_size] = NULL;
            P_WARNING("register item: elem_size=%u", elem_size);
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
                    it->second = new mem_detail::MemoryPool();
                }
                if (NULL == it->second)
                {
                    P_WARNING("failed to alloc MemoryPool");
                    return -1;
                }
                if (0 > it->second->init(it->first, AGILE_SE_MEM_PAGE_SIZE))
                {
                    P_WARNING("failed to init MemoryPool, elem_size=%u, page_size=%u", it->first, AGILE_SE_MEM_PAGE_SIZE);
                    return -1;
                }
                ++it;
            }
            P_WARNING("init ok, elems num=%u, page_size=%u", (uint32_t)m_pools.size(), AGILE_SE_MEM_PAGE_SIZE);
            return 0;
        }

        vaddr_t alloc(uint32_t elem_size)
        {
            Map::iterator it = m_pools.find(elem_size);
            if (it == m_pools_end)
            {
                P_WARNING("unregistered elem size: %u", elem_size);
                return NULL;
            }
            if (NULL == it->second)
            {
                P_WARNING("not init elem size: %u", elem_size);
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
                P_WARNING("unregistered elem size: %u", elem_size);
                return ;
            }
            if (NULL == it->second)
            {
                P_WARNING("not init elem size: %u", elem_size);
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
                    P_WARNING("pool[%d]: elem_size=%u, mem used=%lu", i, it->first, (uint64_t)mem_used);
                    ++i;
                }
                ++it;
            }
            P_WARNING("total mem used=%lu", (uint64_t)total_used);
        }
    private:
        Map m_pools;
        Map::iterator m_pools_end;
};

#endif
