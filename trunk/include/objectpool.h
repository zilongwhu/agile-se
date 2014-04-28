// =====================================================================================
//
//       Filename:  objectpool.h
//
//    Description:  
//
//        Version:  1.0
//        Created:  04/28/2014 04:26:04 PM
//       Revision:  none
//       Compiler:  g++
//
//         Author:  Zilong.Zhu (), zilong.whu@gmail.com
//        Company:  edu.whu
//
// =====================================================================================

#ifndef __AGILE_SE_OBJECT_MEMORY_POOL_H__
#define __AGILE_SE_OBJECT_MEMORY_POOL_H__

#include "delaypool.h"

template<typename T, typename TMemoryPool>
class TObjectPool
{
    public:
        typedef TDelayPool<TMemoryPool> Pool;
        typedef void (*cleanup_fun_t)(T *ptr, void *arg);
    private:
        struct cleanup_bag_t
        {
            cleanup_fun_t fun;
            void *arg;
            TMemoryPool *pool;
        };
    public:
        TObjectPool() { m_pool = NULL; }
        ~TObjectPool() { m_pool = NULL; }

        int init(Pool *pool)
        {
            m_pool = pool;
            int ret = m_bag_pool.register_item(sizeof(cleanup_bag_t), 1024*1024);
            if (ret < 0)
            {
                WARNING("failed to register item for m_bag_pool");
                return -1;
            }
            ret = m_bag_pool.init(1024*1024);
            if (ret < 0)
            {
                WARNING("failed to init m_bag_pool");
                return -1;
            }
            WARNING("init ok");
            return 0;
        }

        T *addr(typename Pool::vaddr_t ptr) const { return m_pool->addr(ptr); }

        typename Pool::vaddr_t alloc()
        {
            typename Pool::vaddr_t ptr = m_pool->alloc(sizeof(T));
            if (Pool::null != ptr)
            {
                new (this->addr(ptr)) T();
            }
            return ptr;
        }

        template<typename Arg1>
            typename Pool::vaddr_t alloc(Arg1 arg1)
            {
                typename Pool::vaddr_t ptr = m_pool->alloc(sizeof(T));
                if (Pool::null != ptr)
                {
                    new (this->addr(ptr)) T(arg1);
                }
                return ptr;
            }

        template<typename Arg1, typename Arg2>
            typename Pool::vaddr_t alloc(Arg1 arg1, Arg2 arg2)
            {
                typename Pool::vaddr_t ptr = m_pool->alloc(sizeof(T));
                if (Pool::null != ptr)
                {
                    new (this->addr(ptr)) T(arg1, arg2);
                }
                return ptr;
            }

        template<typename Arg1, typename Arg2, typename Arg3>
            typename Pool::vaddr_t alloc(Arg1 arg1, Arg2 arg2, Arg3 arg3)
            {
                typename Pool::vaddr_t ptr = m_pool->alloc(sizeof(T));
                if (Pool::null != ptr)
                {
                    new (this->addr(ptr)) T(arg1, arg2, arg3);
                }
                return ptr;
            }

        template<typename Arg1, typename Arg2, typename Arg3, typename Arg4>
            typename Pool::vaddr_t alloc(Arg1 arg1, Arg2 arg2, Arg3 arg3, Arg4 arg4)
            {
                typename Pool::vaddr_t ptr = m_pool->alloc(sizeof(T));
                if (Pool::null != ptr)
                {
                    new (this->addr(ptr)) T(arg1, arg2, arg3, arg4);
                }
                return ptr;
            }

        template<typename Arg1, typename Arg2, typename Arg3, typename Arg4, typename Arg5>
            typename Pool::vaddr_t alloc(Arg1 arg1, Arg2 arg2, Arg3 arg3, Arg4 arg4, Arg5 arg5)
            {
                typename Pool::vaddr_t ptr = m_pool->alloc(sizeof(T));
                if (Pool::null != ptr)
                {
                    new (this->addr(ptr)) T(arg1, arg2, arg3, arg4, arg5);
                }
                return ptr;
            }

        void free(typename Pool::vaddr_t ptr)
        {
            if (Pool::null != ptr)
            {
                this->addr(ptr)->~T();
                m_pool->free(ptr, sizeof(T));
            }
        }

        int delay_free(typename Pool::vaddr_t ptr, cleanup_fun_t fun = NULL, void *arg = NULL)
        {
            if (fun)
            {
                typename Pool::vaddr_t bp = m_bag_pool.alloc(sizeof(cleanup_bag_t));
                cleanup_bag_t *bag = m_bag_pool.addr(bp);
                if (NULL == bag)
                {
                    WARNING("failed to alloc cleanup_bag_t");
                    return -1;
                }
                bag->fun = fun;
                bag->arg = arg;
                bag->pool = &m_bag_pool;
                int ret = m_pool->delay_free(ptr, sizeof(T), destroy_with_cleanup, bag);
                if (ret < 0)
                {
                    m_bag_pool.free(bp, sizeof(cleanup_bag_t));
                }
                return ret;
            }
            else
            {
                return m_pool->delay_free(ptr, sizeof(T), destroy, NULL);
            }
        }
    private:
        static void destroy_with_cleanup(void *ptr, void *arg)
        {
            if (ptr)
            {
                T *p = (T *)ptr;
                cleanup_bag_t *bag = (cleanup_bag_t *)arg;
                bag->fun(p, bag->arg);
                bag->pool->free(bag, sizeof(cleanup_bag_t));
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
        TDelayPool<TMemoryPool> *m_pool;
        TMemoryPool m_bag_pool;
};

#endif
