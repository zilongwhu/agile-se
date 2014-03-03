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

#include <deque>

class MemoryPool
{
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
            this->clear();
        }
        ~MemoryPool()
        {
            this->destroy();
        }

        int init(size_t elem_size, size_t block_size, size_t pool_size)
        {
            this->destroy();
            if (0 == elem_size || 0 ==  block_size || 0 == pool_size)
            {
                return -1;
            }
            m_elem_size = (elem_size + sizeof(void *) - 1)/sizeof(void *)*sizeof(void *);
            if (m_elem_size > block_size || block_size > pool_size)
            {
                return -2;
            }
            m_block_size = block_size;
            m_max_blocks_num = (pool_size + block_size - 1)/block_size;
            m_blocks = (void **)::malloc(sizeof(void *) * m_max_blocks_num);
            if (NULL == m_blocks)
            {
                return -3;
            }
            return 0;
        }

        void *alloc()
        {
            if (m_free_list)
            {
                void *ptr = (void *)m_free_list;
                m_free_list = m_free_list->next;
                ++m_alloc_num;
                return ptr;
            }
            if (m_cur_blocks_num >= m_max_blocks_num)
            {
                return NULL;
            }
            void *block = ::malloc(m_block_size);
            if (NULL == block)
            {
                return NULL;
            }
            m_blocks[m_cur_blocks_num++] = block;
            const int num = m_block_size / m_elem_size;
            for (int i = 0; i < num; ++i)
            {
                element_t *elem = (element_t *)(((char *)block) + m_elem_size * i);
                elem->next = m_free_list;
                m_free_list = elem;
            }
            return this->alloc();
        }
        void free(void *ptr)
        {
            if (NULL == ptr)
            {
                return ;
            }
            element_t *elem = (element_t *)ptr;
            elem->next = m_free_list;
            m_free_list = elem;
            --m_alloc_num;
            ++m_free_num;
        }
        void delay_free(void *ptr)
        {
            if (NULL == ptr)
            {
                return ;
            }
            m_delayed_list.push_back(ptr);
            ++m_delayed_num;
        }
        void recycle()
        {
            while (m_delayed_list.size() > 0)
            {
                this->free(m_delayed_list.front());
                m_delayed_list.pop_front();
                --m_delayed_num;
            }
        }
    private:
        void clear()
        {
            m_blocks = NULL;
            m_cur_blocks_num = 0;
            m_max_blocks_num = 0;
            m_block_size = 0;
            m_elem_size = 0;
            m_alloc_num = 0;
            m_free_num = 0;
            m_delayed_num = 0;
            m_free_list = NULL;
            m_delayed_list.clear();
        }
        void destroy()
        {
            if (m_blocks)
            {
                for (size_t i = 0; i < m_cur_blocks_num; ++i)
                {
                    ::free(m_blocks[i]);
                }
                ::free(m_blocks);
            }
            this->clear();
        }
    private:
        void **m_blocks;
        size_t m_cur_blocks_num;
        size_t m_max_blocks_num;
        size_t m_block_size;
        size_t m_elem_size;

        size_t m_alloc_num;
        size_t m_free_num;
        size_t m_delayed_num;

        element_t *m_free_list;
        std::deque<void *> m_delayed_list;
};

#endif
