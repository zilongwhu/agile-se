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

        size_t cur_blocks_num() const { return m_cur_blocks_num; }
        size_t max_blocks_num() const { return m_max_blocks_num; }
        size_t block_size() const { return m_block_size; }
        size_t elem_size() const { return m_elem_size; }
        size_t alloc_num() const { return m_alloc_num; }
        size_t free_num() const { return m_free_num; }

        size_t mem() const
        {
            return sizeof(void *) * m_max_blocks_num
                + m_cur_blocks_num * m_block_size;
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
            m_free_list = NULL;
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

        element_t *m_free_list;
};

class DelayPool
{
    public:
        typedef void (*destroy_fun_t)(void *ptr, void *arg);
    private:
        struct node_t
        {
            void *ptr;
            int push_time;
        };
        struct queue_t
        {
            MemoryPool::element_t *head;
            MemoryPool::element_t *tail;
        };
    public:
        DelayPool()
        {
            m_delayed_num = 0;
            m_delayed_time = 5;
            m_delayed_list.head = m_delayed_list.tail = NULL;
        }

        ~DelayPool()
        {
            m_delayed_num = 0;
            m_delayed_time = 5;
            m_delayed_list.head = m_delayed_list.tail = NULL;
        }

        int init(size_t elem_size, size_t block_size, size_t pool_size)
        {
            int ret = m_pool.init(elem_size, block_size, pool_size);
            if (ret < 0)
            {
                return ret;
            }
            int total = m_pool.block_size() / m_pool.elem_size() * m_pool.max_blocks_num();
            ret = m_list_pool.init(sizeof(node_t), 1024*1024, total * (sizeof(void *) + sizeof(node_t)));
            if (ret < 0)
            {
                return ret;
            }
            return 0;
        }

        void set_delayed_time(int delayed_seconds)
        {
            if (delayed_seconds > 0)
            {
                m_delayed_time = delayed_seconds;
            }
        }

        void *alloc()
        {
            return m_pool.alloc();
        }

        void free(void *ptr)
        {
            m_pool.free(ptr);
        }

        int delay_free(void *ptr)
        {
            if (NULL == ptr)
            {
                return -1;
            }
            MemoryPool::element_t *elem = (MemoryPool::element_t *)m_list_pool.alloc();
            if (NULL == elem)
            {
                return -2;
            }
            elem->next = NULL;
            node_t *node = (node_t *)(elem + 1);
            node->ptr = ptr;
            node->push_time = s_now;
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

        void recycle(destroy_fun_t fun = NULL, void *arg = NULL)
        {
            const int now = s_now;
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
                if (fun)
                {
                    fun(node->ptr, arg);
                }
                m_pool.free(node->ptr);
                m_delayed_list.head = head->next;
                if (NULL == m_delayed_list.head)
                {
                    m_delayed_list.tail = NULL;
                }
                m_list_pool.free(head);
                --m_delayed_num;
            }
        }

        size_t cur_blocks_num() const { return m_pool.cur_blocks_num(); }
        size_t max_blocks_num() const { return m_pool.max_blocks_num(); }
        size_t block_size() const { return m_pool.block_size(); }
        size_t elem_size() const { return m_pool.elem_size(); }
        size_t alloc_num() const { return m_pool.alloc_num(); }
        size_t free_num() const { return m_pool.free_num(); }
        size_t delayed_num() const { return m_delayed_num; }

        size_t mem() const
        {
            return m_pool.mem() + m_list_pool.mem();
        }
    private:
        MemoryPool m_pool;
        MemoryPool m_list_pool;

        size_t m_delayed_num;
        int m_delayed_time;

        queue_t m_delayed_list;
    public:
        static void init_time_updater();
        static int now()
        {
            return s_now;
        }
    private:
        static volatile int s_now;
};

template<typename T>
class ObjectPool
{
    public:
        ObjectPool() { }
        ~ObjectPool() { }

        int init(size_t block_size, size_t pool_size)
        {
            return m_pool.init(sizeof(T), block_size, pool_size);
        }

        T *alloc()
        {
            T *ptr = (T *)m_pool.alloc();
            if (ptr)
            {
                new (ptr) T();
            }
            return ptr;
        }

        template<typename Arg1>
            T *alloc(Arg1 arg1)
            {
                T *ptr = (T *)m_pool.alloc();
                if (ptr)
                {
                    new (ptr) T(arg1);
                }
                return ptr;
            }

        template<typename Arg1, typename Arg2>
            T *alloc(Arg1 arg1, Arg2 arg2)
            {
                T *ptr = (T *)m_pool.alloc();
                if (ptr)
                {
                    new (ptr) T(arg1, arg2);
                }
                return ptr;
            }

        template<typename Arg1, typename Arg2, typename Arg3>
            T *alloc(Arg1 arg1, Arg2 arg2, Arg3 arg3)
            {
                T *ptr = (T *)m_pool.alloc();
                if (ptr)
                {
                    new (ptr) T(arg1, arg2, arg3);
                }
                return ptr;
            }

        void free(T *ptr)
        {
            if (ptr)
            {
                ptr->~T();
                m_pool.free(ptr);
            }
        }

        int delay_free(T *ptr)
        {
            return m_pool.delay_free(ptr);
        }

        void recycle()
        {
            m_pool.recycle(destroy, NULL);
        }

        void set_delayed_time(int delayed_seconds)
        {
            m_pool.set_delayed_time(delayed_seconds);
        }
        size_t cur_blocks_num() const { return m_pool.cur_blocks_num(); }
        size_t max_blocks_num() const { return m_pool.max_blocks_num(); }
        size_t block_size() const { return m_pool.block_size(); }
        size_t elem_size() const { return m_pool.elem_size(); }
        size_t alloc_num() const { return m_pool.alloc_num(); }
        size_t free_num() const { return m_pool.free_num(); }
        size_t delayed_num() const { return m_pool.delayed_num(); }
        size_t mem() const { return m_pool.mem(); }
    private:
        static void destroy(void *ptr, void *arg)
        {
            if (ptr)
            {
                T *p = (T *)ptr;
                p->~T();
            }
        }
    private:
        DelayPool m_pool;
};

#endif
