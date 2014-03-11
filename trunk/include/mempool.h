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

        int init(size_t elem_size, size_t block_size)
        {
            this->destroy();
            if (0 == elem_size || 0 ==  block_size)
            {
                return -1;
            }
            m_elem_size = (elem_size + sizeof(void *) - 1)/sizeof(void *)*sizeof(void *);
            if (m_elem_size > block_size)
            {
                return -2;
            }
            m_block_size = block_size;
            return 0;
        }

        void *alloc()
        {
            if (m_free_list)
            {
                void *ptr = (void *)m_free_list;
                m_free_list = m_free_list->next;
                ++m_alloc_num;
                --m_free_num;
                return ptr;
            }
            void *block = ::malloc(m_block_size);
            if (NULL == block)
            {
                return NULL;
            }
            try
            {
                m_blocks.push_back(block);
            }
            catch (...)
            {
                ::free(block);
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

        size_t block_size() const { return m_block_size; }
        size_t elem_size() const { return m_elem_size; }
        size_t alloc_num() const { return m_alloc_num; }
        size_t free_num() const { return m_free_num; }

        size_t mem() const
        {
            return (sizeof(void *) + m_block_size) * m_blocks.size();
        }
    private:
        void clear()
        {
            m_blocks.clear();
            m_block_size = 0;
            m_elem_size = 0;
            m_alloc_num = 0;
            m_free_num = 0;
            m_free_list = NULL;
        }

        void destroy()
        {
            for (size_t i = 0; i < m_blocks.size(); ++i)
            {
                ::free(m_blocks[i]);
            }
            this->clear();
        }
    private:
        std::vector<void *> m_blocks;
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

        int init(size_t elem_size, size_t block_size)
        {
            int ret = m_pool.init(elem_size, block_size);
            if (ret < 0)
            {
                return ret;
            }
            ret = m_list_pool.init(sizeof(node_t), 1024*1024);
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
        typedef void (*cleanup_fun_t)(T *ptr, void *arg);
    private:
        struct cleanup_bag_t
        {
            cleanup_fun_t fun;
            void *arg;
        };
    public:
        ObjectPool() { }
        ~ObjectPool() { }

        int init(size_t block_size)
        {
            return m_pool.init(sizeof(T), block_size);
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

        void recycle(cleanup_fun_t fun = NULL, void *arg = NULL)
        {
            if (fun)
            {
                cleanup_bag_t bag;

                bag.fun = fun;
                bag.arg = arg;

                m_pool.recycle(destroy_with_cleanup, &bag);
            }
            else
            {
                m_pool.recycle(destroy, NULL);
            }
        }

        void set_delayed_time(int delayed_seconds)
        {
            m_pool.set_delayed_time(delayed_seconds);
        }
        size_t block_size() const { return m_pool.block_size(); }
        size_t elem_size() const { return m_pool.elem_size(); }
        size_t alloc_num() const { return m_pool.alloc_num(); }
        size_t free_num() const { return m_pool.free_num(); }
        size_t delayed_num() const { return m_pool.delayed_num(); }
        size_t mem() const { return m_pool.mem(); }
    private:
        static void destroy_with_cleanup(void *ptr, void *arg)
        {
            if (ptr)
            {
                T *p = (T *)ptr;
                cleanup_bag_t *bag = (cleanup_bag_t *)arg;
                bag->fun(p, bag->arg);
                p->~T();
            }
        }
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
