// =====================================================================================
//
//       Filename:  mempool.h
//
//    Description:  memory pool
//
//        Version:  1.0
//        Created:  03/03/2014 02:11:18 PM
//       Revision:  none
//       Compiler:  g++
//
//         Author:  Zilong.Zhu (), zilong.whu@gmail.com
//        Company:  edu.whu
//
// =====================================================================================

#ifndef __AGILE_SE_MEMORY_POOL_H__
#define __AGILE_SE_MEMORY_POOL_H__

#include <stdlib.h>
#include <new>
#include <vector>
#include "log.h"

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
            m_block_size = 0;
            m_elem_size = 0;
            m_alloc_num = 0;
            m_free_num = 0;
            m_free_list = NULL;
        }

        ~MemoryPool()
        {
            this->clear();
        }

        int init(size_t elem_size, size_t block_size)
        {
            this->clear();
            if (0 == elem_size || 0 ==  block_size)
            {
                WARNING("invalid args: elem_size=%u, block_size=%u", elem_size, block_size);
                return -1;
            }
            m_elem_size = adjust_elem_size(elem_size);
            if (m_elem_size > block_size)
            {
                WARNING("block_size is too small: elem_size=%u, block_size=%u", elem_size, block_size);
                return -2;
            }
            m_block_size = block_size;
            WARNING("init ok, elem_size=%u, block_size=%u", m_elem_size, m_block_size);
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
            void *block = ::malloc(m_block_size);
            if (NULL == block)
            {
                WARNING("failed to alloc new block, block_size=%u", m_block_size);
                return NULL;
            }
            try
            {
                m_blocks.push_back(block);
            }
            catch (...)
            {
                ::free(block);

                WARNING("failed to push back new block, blocks_size=%u", (uint32_t)m_blocks.size());
                return NULL;
            }
            const int num = m_block_size / m_elem_size;
            for (int i = 0; i < num; ++i)
            {
                element_t *elem = (element_t *)(((char *)block) + m_elem_size * i);
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

        size_t block_size() const { return m_block_size; }
        size_t elem_size() const { return m_elem_size; }
        size_t alloc_num() const { return m_alloc_num; }
        size_t free_num() const { return m_free_num; }

        size_t mem() const
        {
            return (sizeof(void *) + m_block_size) * m_blocks.size();
        }

        void clear()
        {
            for (size_t i = 0; i < m_blocks.size(); ++i)
            {
                ::free(m_blocks[i]);
            }
            m_blocks.clear();
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
        std::vector<void *> m_blocks;
        size_t m_block_size;
        size_t m_elem_size;

        size_t m_alloc_num;
        size_t m_free_num;

        element_t *m_free_list;
};

#endif
