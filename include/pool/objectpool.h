#ifndef __AGILE_SE_OBJECT_MEMORY_POOL_H__
#define __AGILE_SE_OBJECT_MEMORY_POOL_H__

#include "pool/delaypool.h"

template<typename T, typename TMemoryPool>
class TObjectPool
{
    public:
        typedef T ObjectType;
        typedef TDelayPool<TMemoryPool> Pool;
        typedef typename Pool::vaddr_t vaddr_t;

        typedef void (*cleanup_fun_t)(T *ptr, intptr_t arg);
    private:
        struct cleanup_bag_t
        {
            cleanup_fun_t fun;
            intptr_t arg;
            vaddr_t addr;
            Pool *pool;
        };
    public:
        static int init_pool(Pool *pool)
        {
            return pool->register_item(sizeof(cleanup_bag_t));
        }
    public:
        TObjectPool() { m_pool = NULL; }
        ~TObjectPool() { m_pool = NULL; }

        int init(Pool *pool)
        {
            if (NULL != m_pool)
            {
                P_WARNING("ignore duplicate init call");
                return -1;
            }
            m_pool = pool;
            if (NULL == m_pool)
            {
                m_pool = new Pool;
                if (NULL == m_pool)
                {
                    P_WARNING("failed to new Pool");
                    return -1;
                }
                if (init_pool(m_pool) < 0)
                {
                    P_WARNING("failed to register cleanup_bat_t");
                }
                else if (m_pool->register_item(sizeof(T)) < 0)
                {
                    P_WARNING("failed to register item");
                }
                else if (m_pool->init(1024*1024) < 0)
                {
                    P_WARNING("failed to init pool");
                }
                else
                {
                    P_WARNING("use private pool");
                    return 0;
                }
                delete m_pool;
                m_pool = NULL;
                return -1;
            }
            return 0;
        }

        T *addr(vaddr_t ptr) const { return (T *)m_pool->addr(ptr); }

        vaddr_t alloc()
        {
            vaddr_t ptr = m_pool->alloc(sizeof(T));
            if (0 != ptr)
            {
                new (this->addr(ptr)) T();
            }
            return ptr;
        }

        template<typename Arg1>
            vaddr_t alloc(Arg1 arg1)
            {
                vaddr_t ptr = m_pool->alloc(sizeof(T));
                if (0 != ptr)
                {
                    new (this->addr(ptr)) T(arg1);
                }
                return ptr;
            }

        template<typename Arg1, typename Arg2>
            vaddr_t alloc(Arg1 arg1, Arg2 arg2)
            {
                vaddr_t ptr = m_pool->alloc(sizeof(T));
                if (0 != ptr)
                {
                    new (this->addr(ptr)) T(arg1, arg2);
                }
                return ptr;
            }

        template<typename Arg1, typename Arg2, typename Arg3>
            vaddr_t alloc(Arg1 arg1, Arg2 arg2, Arg3 arg3)
            {
                vaddr_t ptr = m_pool->alloc(sizeof(T));
                if (0 != ptr)
                {
                    new (this->addr(ptr)) T(arg1, arg2, arg3);
                }
                return ptr;
            }

        template<typename Arg1, typename Arg2, typename Arg3, typename Arg4>
            vaddr_t alloc(Arg1 arg1, Arg2 arg2, Arg3 arg3, Arg4 arg4)
            {
                vaddr_t ptr = m_pool->alloc(sizeof(T));
                if (0 != ptr)
                {
                    new (this->addr(ptr)) T(arg1, arg2, arg3, arg4);
                }
                return ptr;
            }

        template<typename Arg1, typename Arg2, typename Arg3, typename Arg4, typename Arg5>
            vaddr_t alloc(Arg1 arg1, Arg2 arg2, Arg3 arg3, Arg4 arg4, Arg5 arg5)
            {
                vaddr_t ptr = m_pool->alloc(sizeof(T));
                if (0 != ptr)
                {
                    new (this->addr(ptr)) T(arg1, arg2, arg3, arg4, arg5);
                }
                return ptr;
            }

        void free(vaddr_t ptr)
        {
            if (0 != ptr)
            {
                this->addr(ptr)->~T();
                m_pool->free(ptr, sizeof(T));
            }
        }

        int delay_free(vaddr_t ptr, cleanup_fun_t fun = NULL, intptr_t arg = 0)
        {
            if (0 == ptr)
            {
                return -1;
            }
            if (fun)
            {
                vaddr_t bp = m_pool->alloc(sizeof(cleanup_bag_t));
                cleanup_bag_t *bag = (cleanup_bag_t *)m_pool->addr(bp);
                if (NULL == bag)
                {
                    P_WARNING("failed to alloc cleanup_bag_t");
                    return -1;
                }
                bag->fun = fun;
                bag->arg = arg;
                bag->addr = bp;
                bag->pool = m_pool;
                int ret = m_pool->delay_free(ptr, sizeof(T), destroy_with_cleanup, (intptr_t)bag);
                if (ret < 0)
                {
                    m_pool->free(bp, sizeof(cleanup_bag_t));
                }
                return ret;
            }
            else
            {
                return m_pool->delay_free(ptr, sizeof(T), destroy, 0);
            }
        }
    private:
        static void destroy_with_cleanup(void *ptr, intptr_t arg)
        {
            if (ptr)
            {
                T *p = (T *)ptr;
                cleanup_bag_t *bag = (cleanup_bag_t *)arg;
                bag->fun(p, bag->arg);
                bag->pool->free(bag->addr, sizeof(cleanup_bag_t));
                p->~T();
            }
        }
        static void destroy(void *ptr, intptr_t /* arg */)
        {
            if (ptr)
            {
                T *p = (T *)ptr;
                p->~T();
            }
        }
    private:
        Pool *m_pool;
};

#endif
