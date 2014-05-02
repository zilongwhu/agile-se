// =====================================================================================
//
//       Filename:  skiplist.h
//
//    Description:  
//
//        Version:  1.0
//        Created:  05/02/2014 01:00:18 PM
//       Revision:  none
//       Compiler:  g++
//
//         Author:  Zilong.Zhu (), zilong.whu@gmail.com
//        Company:  edu.whu
//
// =====================================================================================

#ifndef __AGILE_SE_SKIPLIST_H__
#define __AGILE_SE_SKIPLIST_H__

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "mempool2.h"
#include "delaypool.h"

template<typename TMemoryPool = VMemoryPool, uint32_t M = 20>
class TSkipList
{
    private:
        typedef typename TMemoryPool::vaddr_t vaddr_t;

        struct node_t
        {
            int id;
            uint32_t level;
            vaddr_t next[1];
        };

        typedef TDelayPool<TMemoryPool> DelayPool;
    public:
        static int init_pool(DelayPool *pool, uint32_t payload_len)
        {
            for (uint32_t i = 0; i < M; ++i)
            {
                uint32_t size = sizeof(node_t) + sizeof(vaddr_t) * i
                    + (payload_len + sizeof(vaddr_t) - 1) / sizeof(vaddr_t) * sizeof(vaddr_t);
                if (pool->register_item(size, 1024*1024) < 0)
                {
                    WARNING("failed to register node, level=%d, size=%u, payload_len=%u", i, size, payload_len);
                    return -1;
                }
                WARNING("register node ok, level=%d, size=%u, payload_len=%u", i, size, payload_len);
            }
            WARNING("init pool ok, payload_len=%u", payload_len);
            return 0;
        }
    private:
        TSkipList(const TSkipList &);
        TSkipList &operator =(const TSkipList &);
    public:
        TSkipList(DelayPool *pool, uint32_t payload_len)
            : m_pool(pool), m_payload_len(payload_len)
        {
            m_size = 0;
            m_cur_level = 0;
            for (uint32_t i = 0; i < M; ++i)
            {
                m_head[i] = 0;
            }
        }
        ~TSkipList()
        {
            node_t *node;
            vaddr_t cur;
            while (0 != m_head[0])
            {
                cur = m_head[0];
                node = (node_t *)m_pool->addr(cur);
                m_head[0] = node->next[0];
                m_pool->delay_free(cur, this->node_size(node->level));
            }
            m_cur_level = 0;
            m_size = 0;
        }

        uint32_t payload_len() const { return m_payload_len; }
        uint32_t cur_level() const { return m_cur_level; }
        uint32_t size() const { return m_size; }

        vaddr_t find(int id, void **payload = NULL, vaddr_t *path = NULL) const
        {
            if (path)
            {
                for (uint32_t i = m_cur_level + 1; i < M; ++i)
                {
                    path[i] = 0;
                }
            }
            node_t *node;
            node_t *next;
            vaddr_t vcur;
            int32_t cur_level = m_cur_level;
            while (cur_level >= 0)
            {
                if (0 == m_head[cur_level]) /* go down */
                {
                    if (path)
                    {
                        path[cur_level] = 0;
                    }
                    --cur_level;
                }
                else
                {
                    vcur = m_head[cur_level];
                    node = (node_t *)m_pool->addr(vcur);
                    if (id == node->id) /* matched */
                    {
                        if (path)
                        {
                            path[cur_level] = 0;
                        }
                        if (payload)
                        {
                            *payload = ((char *)node) + this->payload_offset(node->level);
                        }
                        return vcur;
                    }
                    else if (id < node->id) /* go down */
                    {
                        if (path)
                        {
                            path[cur_level] = 0;
                        }
                        --cur_level;
                    }
                    else /* get head node */
                    {
                        break;
                    }
                }
            }
            while (cur_level >= 0)
            {
                if (0 == node->next[cur_level]) /* go down */
                {
                    if (path)
                    {
                        path[cur_level] = vcur;
                    }
                    --cur_level;
                }
                else
                {
                    next = (node_t *)m_pool->addr(node->next[cur_level]);
                    if (id == next->id) /* matched */
                    {
                        if (path)
                        {
                            path[cur_level] = vcur;
                        }
                        if (payload)
                        {
                            *payload = ((char *)next) + this->payload_offset(next->level);
                        }
                        return node->next[cur_level];
                    }
                    else if (id < next->id) /* go down */
                    {
                        if (path)
                        {
                            path[cur_level] = vcur;
                        }
                        --cur_level;
                    }
                    else /* go forward */
                    {
                        vcur = node->next[cur_level];
                        node = next;
                    }
                }
            }
            return 0;
        }
        bool insert(int id, void *payload)
        {
            vaddr_t path[M];
            void *old_payload;
            const vaddr_t vcur = this->find(id, &old_payload, path);
            if (vcur)
            {
                if (m_payload_len > 0 && ::memcmp(payload, old_payload, m_payload_len))
                {
                    node_t *pold = (node_t *)m_pool->addr(vcur);
                    const uint32_t level = pold->level;
                    const uint32_t node_size = this->node_size(level);
                    vaddr_t vnew = m_pool->alloc(node_size);
                    if (0 == vnew)
                    {
                        WARNING("failed to alloc renew node, id=%d, level=%u, node_size=%u", id, level, node_size);
                        return false;
                    }
                    node_t *pnew = (node_t *)m_pool->addr(vnew);
                    pnew->id = id;
                    pnew->level = level;
                    for (int32_t i = level; i >=0; --i)
                    {
                        pnew->next[i] = pold->next[i];
                    }
                    ::memcpy(((char *)pnew) + this->payload_offset(level), payload, m_payload_len);
                    if (0 == path[level])
                    {
                        for (int32_t i = level; i >= 0; --i)
                        {
                            if (m_head[i] == vcur)
                            {
                                m_head[i] = vnew;
                            }
                            else
                            {
                                node_t *pre = (node_t *)m_pool->addr(m_head[i]);
                                while (i >= 0)
                                {
                                    while (pre->next[i] != vcur) /* go forward */
                                    {
                                        pre = (node_t *)m_pool->addr(pre->next[i]);
                                    }
                                    pre->next[i] = vnew;
                                    --i; /* go down */
                                }
                                break;
                            }
                        }
                    }
                    else
                    {
                        node_t *pre = (node_t *)m_pool->addr(path[level]);
                        for (int32_t i = level; i >= 0; --i) /* go down */
                        {
                            while (pre->next[i] != vcur) /* go forward */
                            {
                                pre = (node_t *)m_pool->addr(pre->next[i]);
                            }
                            pre->next[i] = vnew;
                        }
                    }
                    m_pool->delay_free(vcur, node_size);
                    DEBUG("overwrite node ok, id=%d, level=%u, node_size=%u", id, level, node_size);
                }
            }
            else
            {
                const uint32_t level = this->rand_level();
                const uint32_t node_size = this->node_size(level);
                vaddr_t vnew = m_pool->alloc(node_size);
                if (0 == vnew)
                {
                    WARNING("failed to alloc new node, id=%d, level=%u, node_size=%u", id, level, node_size);
                    return false;
                }
                node_t *pnew = (node_t *)m_pool->addr(vnew);
                pnew->id = id;
                pnew->level = level;
                if (m_payload_len > 0)
                {
                    ::memcpy(((char *)pnew) + this->payload_offset(level), payload, m_payload_len);
                }
                for (int32_t i = level; i >= 0; --i)
                {
                    if (0 == path[i])
                    {
                        pnew->next[i] = m_head[i];
                    }
                    else
                    {
                        node_t *pre = (node_t *)m_pool->addr(path[i]);
                        pnew->next[i] = pre->next[i];
                    }
                }
                for (int32_t i = level; i >= 0; --i)
                {
                    if (0 == path[i])
                    {
                        m_head[i] = vnew;
                    }
                    else
                    {
                        node_t *pre = (node_t *)m_pool->addr(path[i]);
                        pre->next[i] = vnew;
                    }
                }
                ++m_size;
                if (level > m_cur_level)
                {
                    m_cur_level = level;
                }
                DEBUG("insert new node ok, id=%d, level=%u, node_size=%u", id, level, node_size);
            }
            return true;
        }
        void remove(int id)
        {
        }
    public:
        uint32_t rand_level() const
        {
            uint32_t level = 0;
            for (uint32_t i = 0; i < M; ++i)
            {
                if (::rand() & 0x1)
                {
                    ++level;
                }
                else break;
            }
            return level;
        }
        uint32_t payload_offset(uint32_t level) const
        {
            return sizeof(node_t) + sizeof(vaddr_t) * level;
        }
        uint32_t node_size(uint32_t level) const
        {
            return this->payload_offset(level)
                + (m_payload_len + sizeof(vaddr_t) - 1) / sizeof(vaddr_t) * sizeof(vaddr_t);
        }
    private:
        DelayPool *const m_pool;
        const uint32_t m_payload_len;

        uint32_t m_size;
        uint32_t m_cur_level;
        vaddr_t m_head[M];
};

#endif
