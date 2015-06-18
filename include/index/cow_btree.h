#ifndef __AGILE_SE_COW_B_TREE_H__
#define __AGILE_SE_COW_B_TREE_H__

#include <stdint.h>
#include <limits.h>
#include <unistd.h>
#include <string.h>
#include <deque>
#include <vector>
#include <set>
#include "pool/mempool.h"
#include "pool/delaypool.h"

#ifndef offsetof
#define offsetof(TYPE, MEMBER) ((size_t) &((TYPE *)0)->MEMBER)
#endif

#ifndef likely
#define likely(x)	__builtin_expect(!!(x), 1)
#endif
#ifndef unlikely
#define unlikely(x)	__builtin_expect(!!(x), 0)
#endif

template<size_t Base, size_t AntiLog>
struct LogUtil
{
    const static int R = 1 + LogUtil<Base, (AntiLog + Base - 1)/Base>::R;
};

template<size_t Base>
struct LogUtil<Base, 1>
{
    const static int R = 0;
};

template<typename TMemoryPool = Mempool,
    int32_t WIDE = 32, uint32_t ALIGNMENT = sizeof(typename TMemoryPool::vaddr_t)>
class CowBtree
{
    public:
        typedef int32_t Key;
        typedef TDelayPool<TMemoryPool> DelayPool;

        enum { MAX_HEIGHT = LogUtil<(WIDE + 1)/2, UINT_MAX>::R };
        enum { N_WIDE = WIDE };
        enum { N_ALIGNMENT = ALIGNMENT };
    private:
        typedef typename TMemoryPool::vaddr_t vaddr_t;

        struct node_t
        {
            int8_t leaf_flag;
            int8_t new_flag;
            short key_num;
            Key keys[WIDE];
            vaddr_t childs[WIDE];

            inline bool is_leaf() const { return leaf_flag; }
            inline void set_leaf(bool flag) { leaf_flag = flag; }

            inline bool is_new() const { return new_flag; }
            inline void set_new(bool flag) { new_flag = flag; }

            inline int count() const
            {
                return key_num;
            }
            void set_count(int count)
            {
                key_num = count;
            }

            inline Key get_key(int pos) const
            {
                return keys[pos];
            }
            inline Key get_key(Key *ptr) const
            {
                return *ptr;
            }
            void set_key(int pos, const Key id)
            {
                keys[pos] = id;
            }

            inline void set_child(int pos, vaddr_t child)
            {
                childs[pos] = child;
            }
            inline vaddr_t get_child(int pos) const
            {
                return childs[pos];
            }

            inline int search(int beg, const Key id) const
            {
                return this->search2(beg, id);
            }

            inline int search1(int beg, const Key id) const
            {
                const int num = this->count();
                int end = num - 1;
                while (end >= beg)
                {
                    int mid = ((beg + end) >> 1);
                    Key key = get_key(mid);
                    if (id == key)
                    {
                        return mid;
                    }
                    else if (id < key)
                    {
                        end = mid - 1;
                    }
                    else
                    {
                        beg = mid + 1;
                    }
                }
                if (beg >= num)
                {
                    return -1;
                }
                return beg;
            }

            inline int search2(int beg, const Key id) const
            {
                const int num = this->count();
                while (beg < num)
                {
                    if (get_key(beg) >= id)
                    {
                        return beg;
                    }
                    ++beg;
                }
                return -1;
            }

            inline int search3(int beg, const Key id) const
            {
                const int num = this->count();
                if (beg + 16 < num)
                {
                    int end = num - 1;
                    while (end >= beg)
                    {
                        int mid = ((beg + end) >> 1);
                        Key key = get_key(mid);
                        if (id == key)
                        {
                            return mid;
                        }
                        else if (id < key)
                        {
                            end = mid - 1;
                        }
                        else
                        {
                            beg = mid + 1;
                        }
                    }
                    if (beg >= num)
                    {
                        return -1;
                    }
                    return beg;
                }
                else
                {
                    while (beg < num)
                    {
                        if (get_key(beg) >= id)
                        {
                            return beg;
                        }
                        ++beg;
                    }
                    return -1;
                }
            }

            inline Key *search(Key *beg, Key *const end, const Key id) const
            {
                return this->search2(beg, end, id);
            }

            inline Key *search1(Key *beg, Key *const ed, const Key id) const
            {
                Key *end = ed - 1;
                while (end >= beg)
                {
                    Key *mid = beg + (int(end - beg) >> 1);
                    Key key = get_key(mid);
                    if (id == key)
                    {
                        return mid;
                    }
                    else if (id < key)
                    {
                        end = mid - 1;
                    }
                    else
                    {
                        beg = mid + 1;
                    }
                }
                if (beg >= ed)
                {
                    return NULL;
                }
                return beg;
            }

            inline Key *search2(Key *beg, Key *const ed, const Key id) const
            {
                while (beg < ed)
                {
                    if (get_key(beg) >= id)
                    {
                        return beg;
                    }
                    ++beg;
                }
                return NULL;
            }

            inline Key *search3(Key *beg, Key *const ed, const Key id) const
            {
                if (beg + 16 < ed)
                {
                    Key *end = ed - 1;
                    while (end >= beg)
                    {
                        Key *mid = beg + (int(end - beg) >> 1);
                        Key key = get_key(mid);
                        if (id == key)
                        {
                            return mid;
                        }
                        else if (id < key)
                        {
                            end = mid - 1;
                        }
                        else
                        {
                            beg = mid + 1;
                        }
                    }
                    if (beg >= ed)
                    {
                        return NULL;
                    }
                    return beg;
                }
                else
                {
                    while (beg < ed)
                    {
                        if (get_key(beg) >= id)
                        {
                            return beg;
                        }
                        ++beg;
                    }
                    return NULL;
                }
            }
        };
    public:
        static uint32_t node_size()
        {
            return (sizeof(node_t)
                    + ALIGNMENT - 1)
                & ~(ALIGNMENT - 1);
        }
        static uint32_t leaf_size(uint16_t payload_len)
        {
            return (offsetof(node_t, childs)
                    + payload_len * WIDE
                    + ALIGNMENT - 1)
                & ~(ALIGNMENT - 1);
        }
    public:
        class iterator
        {
            private:
                enum { DELTA = LogUtil<(WIDE + 1)/2, 10000>::R - 1 };
            private:
                const CowBtree *m_tree;
                const DelayPool *m_pool;
                Key m_cur;
                int m_height;
                int m_delta;
                struct path_t
                {
                    Key *cur;
                    Key *end;
                    node_t *ptr;
                } *m_leaf, m_path[MAX_HEIGHT];
                int m_len[WIDE];
            public:
                iterator(const CowBtree &tree, bool call_first)
                    : m_tree(&tree), m_pool(tree.m_pool)
                {
                    if (call_first)
                    {
                        this->first();
                    }
                    else /* call first is caller's responsibility */
                    {
                        m_height = 0; /* for copy constructor & operator = */
                    }
                    this->init_lens();
                }

                iterator(const CowBtree &tree, const Key id)
                    : m_tree(&tree), m_pool(tree.m_pool)
                {
                    m_leaf = &m_path[0];
                    m_leaf->ptr = (node_t *)m_pool->addr(m_tree->m_root);
                    if (m_leaf->ptr)
                    {
                        while (1)
                        {
                            m_leaf->end = m_leaf->ptr->keys + m_leaf->ptr->count();
                            m_leaf->cur = m_leaf->ptr->search(m_leaf->ptr->keys, m_leaf->end, id);
                            if (NULL == m_leaf->cur)
                            { /* meeting end */
                                m_cur = -1;
                                m_height = 0;
                                goto RETURN;
                            }
                            if (m_leaf->ptr->is_leaf())
                            { /* find it */
                                m_cur = m_leaf->ptr->get_key(m_leaf->cur);
                                m_height = m_leaf - m_path + 1;
                                goto RETURN;
                            }
#ifndef __NOT_DEBUG_COW_BTREE__
                            if (m_leaf - m_path + 1 >= MAX_HEIGHT)
                            {
                                P_WARNING("cannot come to here too!!!~~~");

                                ::sleep(1);
                                ::abort();
                            }
#endif
                            m_leaf[1].ptr = (node_t *)m_pool->addr(m_leaf->ptr->get_child(
                                        m_leaf->cur - m_leaf->ptr->keys)); /* load child node */
                            ++m_leaf;
                        }
                    }
                    else
                    {
                        m_cur = -1;
                        m_height = 0;
                    }
RETURN:
                    m_delta = m_height / 3;
                    if (m_delta > DELTA)
                    {
                        m_delta = DELTA;
                    }
                    this->init_lens();
                }

                iterator(const iterator &o)
                {
                    m_tree = o.m_tree;
                    m_pool = o.m_pool;
                    m_cur = o.m_cur;
                    m_height = o.m_height;
                    m_delta = o.m_delta;
                    if (m_height > 0)
                    {
                        for (int i = 0; i < m_height; ++i)
                        {
                            m_path[i] = o.m_path[i];
                        }
                        m_leaf = &m_path[m_height - 1];
                    }
                    else
                    {
                        m_leaf = NULL;
                    }
                    this->init_lens();
                }

                iterator &operator =(const iterator &o)
                {
                    if (this == &o)
                    {
                        return *this;
                    }
                    m_tree = o.m_tree;
                    m_pool = o.m_pool;
                    m_cur = o.m_cur;
                    m_height = o.m_height;
                    m_delta = o.m_delta;
                    if (m_height > 0)
                    {
                        for (int i = 0; i < m_height; ++i)
                        {
                            m_path[i] = o.m_path[i];
                        }
                        m_leaf = &m_path[m_height - 1];
                    }
                    else
                    {
                        m_leaf = NULL;
                    }
                    this->init_lens();
                    return *this;
                }

                inline void first()
                {
                    m_leaf = &m_path[0];
                    m_leaf->ptr = (node_t *)m_pool->addr(m_tree->m_root);
                    if (m_leaf->ptr)
                    {
                        m_leaf->cur = m_leaf->ptr->keys;
                        m_leaf->end = m_leaf->cur + m_leaf->ptr->count();
                        while (!m_leaf->ptr->is_leaf())
                        {
                            ++m_leaf; /* move down */
#ifndef __NOT_DEBUG_COW_BTREE__
                            if (m_leaf - m_path >= MAX_HEIGHT)
                            {
                                P_WARNING("construct iterator: cannot come to here!");

                                ::sleep(1);
                                ::abort();
                            }
#endif
                            m_leaf->ptr = (node_t *)m_pool->addr((m_leaf - 1)->ptr->get_child(0));
                            m_leaf->cur = m_leaf->ptr->keys;
                            m_leaf->end = m_leaf->cur + m_leaf->ptr->count();
                        }
                        m_cur = m_leaf->ptr->get_key(0);
                        m_height = m_leaf - m_path + 1;
                    }
                    else /* empty tree */
                    {
                        m_cur = -1;
                        m_height = 0;
                    }
                    m_delta = m_height / 3;
                    if (m_delta > DELTA)
                    {
                        m_delta = DELTA;
                    }
                }

                inline iterator & operator ++()
                {
                    if (unlikely(m_cur < 0)) /* already meeting end */
                    {
                        return *this;
                    }
                    if (likely(++m_leaf->cur < m_leaf->end))
                    {
                        m_cur = m_leaf->ptr->get_key(m_leaf->cur);
                        return *this;
                    }
                    while (--m_leaf >= m_path)
                    {
                        if (++m_leaf->cur < m_leaf->end)
                        {
                            while (!m_leaf->ptr->is_leaf())
                            {
#ifndef __NOT_DEBUG_COW_BTREE__
                                if (m_leaf - m_path + 1 >= m_height)
                                {
                                    P_WARNING("operator++: i + 1 >= m_height");

                                    ::sleep(1);
                                    ::abort();
                                }
#endif
                                m_leaf[1].ptr = (node_t *)m_pool->addr(m_leaf->ptr->get_child(
                                            m_leaf->cur- m_leaf->ptr->keys)); /* load child node */

                                ++m_leaf;
                                m_leaf->cur = m_leaf->ptr->keys;
                                m_leaf->end = m_leaf->cur + m_leaf->ptr->count();
                            }
#ifndef __NOT_DEBUG_COW_BTREE__
                            if (m_leaf - m_path + 1 != m_height)
                            {
                                P_WARNING("operator++: i + 1 != m_height");

                                ::sleep(1);
                                ::abort();
                            }
#endif
                            m_cur = m_leaf->ptr->get_key(m_leaf->cur);
                            return *this;
                        }
                    }
                    m_cur = -1;
                    return *this;
                }
                iterator operator ++(int)
                {
                    iterator tmp(*this);
                    this->operator ++();
                    return tmp;
                }

                inline Key operator *() const
                {
                    return m_cur;
                }
                inline operator bool() const
                {
                    return m_cur != -1;
                }
                inline uint32_t type() const { return m_tree->type(); }
                inline uint32_t payload_len() const { return m_tree->payload_len(); }
                inline void init_lens()
                {
                    const int len = this->payload_len();

                    int i = 0;
                    m_len[i] = offsetof(node_t, childs);
                    while (++i < WIDE)
                    {
                        m_len[i] = m_len[i - 1] + len;
                    }
                }
                inline void *payload() const
                {
                    return ((char *)m_leaf->ptr) + m_len[m_leaf->cur - m_leaf->ptr->keys];
                }
                inline int height() const
                {
                    return m_height;
                }
                inline size_t size() const
                {
                    return m_tree->size();
                }

                inline void find(Key id) /* get first key which >= id */
                {
                    if (unlikely(m_cur < 0)) /* already meeting end */
                    {
                        return ;
                    }
                    if (m_cur >= id) /* already matched */
                    {
                        return ;
                    }
#if (1)
                    m_leaf -= m_delta;
                    if (likely(m_leaf->ptr->get_key(m_leaf->cur) >= id))
                    {
                        while (!m_leaf->ptr->is_leaf())
                        {
                            if (m_leaf[1].ptr->get_key(m_leaf[1].cur) < id)
                            {
                                break;
                            }
                            ++m_leaf;
                        }
                    }
                    else
#endif
                    {
                        while (--m_leaf >= m_path) /* backtracking path to find matched range */
                        {
                            if (m_leaf->ptr->get_key(m_leaf->cur) >= id) /* id is in range */
                            {
                                break;
                            }
                        }
                    }
                    if (unlikely(m_leaf < m_path)) /* id is not in path */
                    {
                        m_leaf = m_path; /* process root */
                        m_leaf->cur = m_leaf->ptr->search(m_leaf->cur + 1, m_leaf->end, id);
                        if (NULL == m_leaf->cur) /* meeting root's end */
                        {
                            m_cur = -1;
                            return ;
                        }
                        if (unlikely(m_leaf->ptr->is_leaf()))
                        { /* find it (root is leaf) */
                            m_cur = m_leaf->ptr->get_key(m_leaf->cur);
                            return ;
                        }
                        m_leaf[1].ptr = (node_t *)m_pool->addr(m_leaf->ptr->get_child(
                                    m_leaf->cur - m_leaf->ptr->keys)); /* load child node */

                        ++m_leaf;
                        m_leaf->cur = m_leaf->ptr->keys;
                        m_leaf->end = m_leaf->cur + m_leaf->ptr->count();
                        /* root is in correct range now */
                    }
                    else
                    {
                        ++m_leaf;
                    }
                    while (1) /* move down & load childs */
                    {
                        m_leaf->cur = m_leaf->ptr->search(m_leaf->cur, m_leaf->end, id);
#ifndef __NOT_DEBUG_COW_BTREE__
                        if (NULL == m_leaf->cur)
                        {
                            P_WARNING("iterator find: condition not matched");

                            ::sleep(1);
                            ::abort();
                        }
#endif
                        if (m_leaf->ptr->is_leaf()) /* find it */
                        {
#ifndef __NOT_DEBUG_COW_BTREE__
                            if (m_leaf - m_path + 1 != m_height) /* just double check */
                            {
                                P_WARNING("find: i + 1 != m_height");

                                ::sleep(1);
                                ::abort();
                            }
#endif
                            m_cur = m_leaf->ptr->get_key(m_leaf->cur);
                            return ;
                        }
                        m_leaf[1].ptr = (node_t *)m_pool->addr(m_leaf->ptr->get_child(
                                    m_leaf->cur - m_leaf->ptr->keys)); /* load child node */

                        ++m_leaf;
                        m_leaf->cur = m_leaf->ptr->keys;
                        m_leaf->end = m_leaf->cur + m_leaf->ptr->count();
                    }
                }

                inline void find2(Key id) /* get first key which >= id */
                {
                    if (unlikely(m_cur < 0)) /* already meeting end */
                    {
                        return ;
                    }
                    if (m_cur >= id) /* already matched */
                    {
                        return ;
                    }
                    m_leaf = m_path;
                    while (1)
                    {
                        m_leaf->cur = m_leaf->ptr->search(m_leaf->cur, m_leaf->end, id);
                        if (NULL == m_leaf->cur)
                        { /* meeting end */
                            m_cur = -1;
                            return ;
                        }
                        if (m_leaf->ptr->is_leaf())
                        { /* find it */
#ifndef __NOT_DEBUG_COW_BTREE__
                            if (m_leaf - m_path + 1 != m_height) /* just double check */
                            {
                                P_WARNING("find2: i + 1 != m_height");

                                ::sleep(1);
                                ::abort();
                            }
#endif
                            m_cur = m_leaf->ptr->get_key(m_leaf->cur);
                            return ;
                        }
#ifndef __NOT_DEBUG_COW_BTREE__
                        if (m_leaf - m_path + 1 >= m_height)
                        {
                            P_WARNING("find2: i + 1 >= m_height");

                            ::sleep(1);
                            ::abort();
                        }
#endif
                        m_leaf[1].ptr = (node_t *)m_pool->addr(m_leaf->ptr->get_child(
                                    m_leaf->cur - m_leaf->ptr->keys)); /* load child node */

                        ++m_leaf;
                        m_leaf->cur = m_leaf->ptr->keys;
                        m_leaf->end = m_leaf->cur + m_leaf->ptr->count();
                    }
                }
        };
    private:
        struct ModifyContext
        {
            CowBtree &tree;
            vaddr_t root;
            bool has_error;
            int32_t size;
#ifndef __NOT_DEBUG_COW_BTREE__
            int32_t old_size;
            std::set<vaddr_t> need_free;
#else
            std::deque<vaddr_t> need_free;
#endif
            std::set<vaddr_t> new_alloc;

            ModifyContext(CowBtree &t): tree(t)
            {
                root = tree.m_root;
                size = 0;
                has_error = false;
#ifndef __NOT_DEBUG_COW_BTREE__
                old_size = 0;
                iterator it = tree.begin(true);
                while (*it != -1)
                {
                    ++it;
                    ++old_size;
                }
#endif
            }
            ~ModifyContext();

            bool insert(Key id, void *payload)
            {
                if (has_error)
                {
                    return false;
                }
                const bool ret = this->insert_internal(id, payload);
                if (!ret)
                {
                    has_error = true;
                }
#ifndef __NOT_DEBUG_COW_BTREE__
                else if (!this->insert_check(id))
                {
                    P_WARNING("insert check failed");

                    ::sleep(1);
                    ::abort();
                }
#endif
                return ret;
            }
            bool remove(const Key id)
            {
                if (has_error)
                {
                    return false;
                }
                bool is_removed = false;
                const bool ret = this->remove_internal(id, &is_removed);
                if (!ret)
                {
                    has_error = true;
                }
#ifndef __NOT_DEBUG_COW_BTREE__
                else if (is_removed && !this->remove_check(id))
                {
                    P_WARNING("remove check failed");

                    ::sleep(1);
                    ::abort();
                }
#endif
                return ret;
            }
            bool insert_internal(const Key id, void *payload);
            bool remove_internal(const Key id, bool *p_flag = NULL);
            bool insert_check(const Key id);
            bool remove_check(const Key id);

            void push_need_free(vaddr_t addr)
            {
#ifndef __NOT_DEBUG_COW_BTREE__
                if (need_free.find(addr) != need_free.end())
                {
                    P_WARNING("duplicate addr need to free");

                    ::sleep(1);
                    ::abort();
                }
                need_free.insert(addr);
#else
                need_free.push_back(addr);
#endif
            }
            void push_new_alloc(vaddr_t addr)
            {
#ifndef __NOT_DEBUG_COW_BTREE__
                if (new_alloc.find(addr) != new_alloc.end())
                {
                    P_WARNING("duplicate new alloced addr");

                    ::sleep(1);
                    ::abort();
                }
#endif
                new_alloc.insert(addr);
            }
            void remove_new_alloc(vaddr_t addr)
            {
#ifndef __NOT_DEBUG_COW_BTREE__
                if (new_alloc.find(addr) == new_alloc.end())
                {
                    P_WARNING("cannot find alloced addr");

                    ::sleep(1);
                    ::abort();
                }
#endif
                new_alloc.erase(addr);
            }
        };
    public:
        CowBtree(DelayPool *pool, uint8_t type, uint16_t payload_len)
            : m_pool(pool), m_type(type), m_payload_len(payload_len)
        {
            m_size = 0;
            m_root = 0;
            m_ctx = NULL;
        }
        ~CowBtree()
        {
            const int node_sz = node_size();
            const int leaf_sz = leaf_size(m_payload_len);

            int i;
            int pos[MAX_HEIGHT];
            node_t *path[MAX_HEIGHT];

            pos[0] = 0;
            path[0] = (node_t *)m_pool->addr(m_root);
            if (path[0])
            {
#ifndef __NOT_DEBUG_COW_BTREE__
                if (path[0]->is_new())
                {
                    goto ERR;
                }
#endif
                i = 0;
                while (!path[i]->is_leaf()) /* move down to first leaf */
                {
                    ++i;
#ifndef __NOT_DEBUG_COW_BTREE__
                    if (i >= MAX_HEIGHT)
                    {
                        P_WARNING("CowBtree destructor: i >= MAX_HEIGHT");

                        ::sleep(1);
                        ::abort();
                    }
#endif
                    pos[i] = 0;
                    path[i] = (node_t *)m_pool->addr(path[i - 1]->get_child(0)); /* load first child */
                }
                if (i > 0) /* root is not leaf */
                {
                    const int max = --i;
                    int count;
                    while (i >= 0)
                    {
                        if (max == i)
                        {
                            count = path[i]->count();
                            while (pos[i] < count)
                            {
#ifndef __NOT_DEBUG_COW_BTREE__
                                if (((node_t *)m_pool->addr(path[i]->get_child(pos[i])))->is_new())
                                {
                                    goto ERR;
                                }
#endif
                                m_pool->delay_free(path[i]->get_child(pos[i]), leaf_sz); /* free leaf */
                                ++pos[i];
                            }
                            --i; /* backtracking path */
                            continue;
                        }
#ifndef __NOT_DEBUG_COW_BTREE__
                        if (((node_t *)m_pool->addr(path[i]->get_child(pos[i])))->is_new())
                        {
                            goto ERR;
                        }
#endif
                        m_pool->delay_free(path[i]->get_child(pos[i]), node_sz); /* free node */
                        if (++pos[i] >= path[i]->count())
                        {
                            --i; /* backtracking path */
                            continue;
                        }
                        while (!path[i]->is_leaf()) /* move down to leaf */
                        {
                            ++i;
#ifndef __NOT_DEBUG_COW_BTREE__
                            if (i >= MAX_HEIGHT)
                            {
                                P_WARNING("CowBtree destructor: i >= MAX_HEIGHT");

                                ::sleep(1);
                                ::abort();
                            }
#endif
                            pos[i] = 0;
                            path[i] = (node_t *)m_pool->addr(path[i - 1]->get_child(pos[i - 1]));
                        }
                        --i; /* goto leaf's parent */
#ifndef __NOT_DEBUG_COW_BTREE__
                        if (max != i)
                        {
                            P_WARNING("CowBtree destructor: max != i");

                            ::sleep(1);
                            ::abort();
                        }
#endif
                    }
                    m_pool->delay_free(m_root, node_sz); /* free root node */
                }
                else 
                {
                    m_pool->delay_free(m_root, leaf_sz); /* free root leaf */
                }
#ifndef __NOT_DEBUG_COW_BTREE__
                if (0)
                {
ERR:
                    P_WARNING("CowBtree destructor: is_new check error");

                    ::sleep(1);
                    ::abort();
                }
#endif
            }
        }

        uint32_t type() const { return m_type; }
        uint32_t payload_len() const { return m_payload_len; }
        uint32_t size() const { return m_size; }

        iterator begin(bool call_first) const
        {
            return iterator(*this, call_first);
        }

        iterator find(const Key id) const
        {
            return iterator(*this, id);
        }

        bool seek(const Key id, void **payload) const
        {
            node_t *ptr = (node_t *)m_pool->addr(m_root);
            while (ptr)
            {
                int pos = ptr->search(0, id);
                if (pos < 0)
                { /* meeting end */
                    return false;
                }
                if (ptr->is_leaf())
                { /* find it */
                    if (ptr->get_key(pos) == id)
                    {
                        if (payload)
                        {
                            *payload = ((char *)ptr) + offsetof(node_t, childs) + m_payload_len * pos;
                        }
                        return true;
                    }
                    return false;
                }
                ptr = (node_t *)m_pool->addr(ptr->get_child(pos)); /* load child node */
            }
            return false;
        }

        bool init_for_modify()
        {
            if (NULL == m_ctx)
            {
                m_ctx = new (std::nothrow) ModifyContext(*this);
            }
            return NULL != m_ctx;
        }

        bool insert(const Key &key, void *payload = NULL)
        {
            if (NULL == m_ctx)
            {
                return false;
            }
            return m_ctx->insert(key, payload);
        }

        bool remove(const Key &key)
        {
            if (NULL == m_ctx)
            {
                return false;
            }
            return m_ctx->remove(key);
        }

        bool end_for_modify()
        {
            if (m_ctx)
            {
                const bool ret = !m_ctx->has_error;
                delete m_ctx;
                m_ctx = NULL;
                return ret;
            }
            return true;
        }

        size_t count_nodes(bool print) const
        {
            const int node_sz = node_size();
            const int leaf_sz = leaf_size(m_payload_len);

            int node_count = 0;
            int leaf_count = 0;
            int height = 0;

            int i;
            int pos[MAX_HEIGHT];
            node_t *path[MAX_HEIGHT];

            pos[0] = 0;
            path[0] = (node_t *)m_pool->addr(m_root);
            if (path[0])
            {
                i = 0;
                while (!path[i]->is_leaf()) /* move down to first leaf */
                {
                    ++i;
#ifndef __NOT_DEBUG_COW_BTREE__
                    if (i >= MAX_HEIGHT)
                    {
                        P_WARNING("CowBtree destructor: i >= MAX_HEIGHT");

                        ::sleep(1);
                        ::abort();
                    }
#endif
                    pos[i] = 0;
                    path[i] = (node_t *)m_pool->addr(path[i - 1]->get_child(0)); /* load first child */
                }
                height = i + 1;
                if (i > 0) /* root is not leaf */
                {
                    const int max = --i;
                    int count;
                    while (i >= 0)
                    {
                        if (max == i)
                        {
                            count = path[i]->count();
                            while (pos[i] < count)
                            {
#ifndef __NOT_DEBUG_COW_BTREE__
                                if (((node_t *)m_pool->addr(path[i]->get_child(pos[i])))->is_new())
                                {
                                    goto ERR;
                                }
#endif
                                ++leaf_count;
                                ++pos[i];
                            }
                            --i; /* backtracking path */
                            continue;
                        }
#ifndef __NOT_DEBUG_COW_BTREE__
                        if (((node_t *)m_pool->addr(path[i]->get_child(pos[i])))->is_new())
                        {
                            goto ERR;
                        }
#endif
                        ++node_count;
                        if (++pos[i] >= path[i]->count())
                        {
                            --i; /* backtracking path */
                            continue;
                        }
                        while (!path[i]->is_leaf()) /* move down to leaf */
                        {
                            ++i;
#ifndef __NOT_DEBUG_COW_BTREE__
                            if (i >= MAX_HEIGHT)
                            {
                                P_WARNING("CowBtree destructor: i >= MAX_HEIGHT");

                                ::sleep(1);
                                ::abort();
                            }
#endif
                            pos[i] = 0;
                            path[i] = (node_t *)m_pool->addr(path[i - 1]->get_child(pos[i - 1]));
                        }
                        --i; /* goto leaf's parent */
#ifndef __NOT_DEBUG_COW_BTREE__
                        if (max != i)
                        {
                            P_WARNING("CowBtree destructor: max != i");

                            ::sleep(1);
                            ::abort();
                        }
#endif
                    }
                    ++node_count;
                }
                else 
                {
                    ++leaf_count;
                }
                if (print)
                {
                    P_WARNING("height=%d, node_count: %d, leaf_count: %d,"
                            " node_sz: %d, leaf_sz: %d, node_mem=%ld, leaf_mem=%ld, total_mem=%ld",
                            height, node_count, leaf_count, node_sz, leaf_sz,
                            ((long)node_count) * node_sz, ((long)leaf_count) * leaf_sz,
                            ((long)node_count) * node_sz + ((long)leaf_count) * leaf_sz);
                }
#ifndef __NOT_DEBUG_COW_BTREE__
                if (0)
                {
ERR:
                    P_WARNING("count nodes: is_new check error");

                    ::sleep(1);
                    ::abort();
                }
#endif
            }
            return ((long)node_count) * node_sz + ((long)leaf_count) * leaf_sz;
        }

        bool validate() const
        {
            if (m_root)
            {
                Key min;
                Key max;
                uint32_t size = 0;
                const bool ret = this->validate((node_t *)m_pool->addr(m_root), min, max, size, 1);
                return ret && size == m_size;
            }
            return true;
        }
    private:
        bool validate(node_t *ptr, Key &min, Key &max, uint32_t &size, uint32_t height) const
        {
            const int x = ((WIDE + 1) >> 1);
            const int count = ptr->count();
            if (count < x)
            {
                if (0 == count || ptr != m_pool->addr(m_root)) /* root is ok */
                {
                    P_WARNING("node::count[%d] < x[%d]", count, x);
                    return false;
                }
            }
            if (ptr->is_new())
            {
                P_WARNING("node cannot be new here");
                return false;
            }
            if (height >= MAX_HEIGHT)
            {
                P_WARNING("height[%u] >= MAX_HEIGHT error", height);
                return false;
            }
            /* check order in node */
            Key last_key = ptr->get_key(0);
            for (int i = 1; i < count; ++i)
            {
                if (!(ptr->get_key(i) > last_key))
                {
                    P_WARNING("key order error in leaf node");
                    return false;
                }
                last_key = ptr->get_key(i);
            }
            if (ptr->is_leaf())
            {
                size += count;
                min = ptr->get_key(0);
                max = ptr->get_key(count - 1);
            }
            else
            {
                Key mins[WIDE];
                Key maxs[WIDE];
                for (int i = 0; i < count; ++i)
                {
                    if (!this->validate((node_t *)m_pool->addr(ptr->get_child(i)),
                                mins[i], maxs[i], size, height + 1))
                    {
                        return false;
                    }
                }
                for (int i = 0; i < count; ++i)
                {
                    if (ptr->get_key(i) != maxs[i])
                    {
                        P_WARNING("key[i] != maxs[i]");
                        return false;
                    }
                    if (!(mins[i] < maxs[i]))
                    {
                        P_WARNING("mins[i] >= maxs[i]");
                        return false;
                    }
                    if (i > 0)
                    {
                        if (!(maxs[i - 1] < mins[i]))
                        {
                            P_WARNING("maxs[i - 1] >= mins[i]");
                            return false;
                        }
                    }
                }
                min = mins[0];
                max = maxs[count - 1];
            }
            return true;
        }
    private:
        DelayPool *m_pool;
        const uint8_t m_type;
        const uint16_t m_payload_len;
        uint32_t m_size;
        vaddr_t m_root;
        ModifyContext *m_ctx;
};

template<typename TMemoryPool, int32_t WIDE, uint32_t ALIGNMENT>
bool CowBtree<TMemoryPool, WIDE, ALIGNMENT>::
    ModifyContext::insert_check(const Key id)
{
    int pos[MAX_HEIGHT];
    node_t *path[MAX_HEIGHT];

    DelayPool *const pool = tree.m_pool;
    path[0] = (node_t *)pool->addr(root);
    if (NULL == path[0]) /* empty tree */
    {
        P_WARNING("empty tree");
        return false;
    }
    int i = 0;
    while (1) /* find appropriate insert pos */
    {
        pos[i] = path[i]->search(0, id);
        if (pos[i] < 0)
        {
            P_WARNING("cannot find id[%d] in path[%d]", id, i);
            return false;
        }
        if (path[i]->is_leaf()) /* meeting leaf */
        {
            break;
        }
        path[i + 1] = (node_t *)pool->addr(path[i]->get_child(pos[i])); /* load child node */
        const Key key1 = path[i]->get_key(pos[i]);
        const Key key2 = path[i + 1]->get_key(path[i + 1]->count() - 1);
        if (key1 != key2)
        {
            P_WARNING("path[i].key != path[i + 1].max_key");

            ::sleep(1);
            ::abort();
        }
        ++i; /* move down */
        if (i >= MAX_HEIGHT)
        {
            P_WARNING("i=%d >= MAX_HEIGHT", i);

            ::sleep(1);
            ::abort();
        }
    }
    if (path[i]->get_key(pos[i]) == id) /* find it */
    {
        if (!path[i]->is_new())
        {
            return true;
        }
        while (--i >= 0)
        {
            if (!path[i]->is_new())
            {
                P_WARNING("path[%d] is not new", i);
                return false;
            }
        }
        return true;
    }
    P_WARNING("cannot find id[%d] in leaf", id);
    return false;
}

template<typename TMemoryPool, int32_t WIDE, uint32_t ALIGNMENT>
bool CowBtree<TMemoryPool, WIDE, ALIGNMENT>::
    ModifyContext::remove_check(const Key id)
{
    int pos[MAX_HEIGHT];
    node_t *path[MAX_HEIGHT];

    DelayPool *const pool = tree.m_pool;
    path[0] = (node_t *)pool->addr(root);
    if (NULL == path[0]) /* empty tree */
    {
        return true;
    }
    int i = 0;
    while (1) /* find appropriate insert pos */
    {
        pos[i] = path[i]->search(0, id);
        if (pos[i] < 0)
        {
            return true;
        }
        if (path[i]->is_leaf()) /* meeting leaf */
        {
            break;
        }
        path[i + 1] = (node_t *)pool->addr(path[i]->get_child(pos[i])); /* load child node */
        const Key key1 = path[i]->get_key(pos[i]);
        const Key key2 = path[i + 1]->get_key(path[i + 1]->count() - 1);
        if (key1 != key2)
        {
            P_WARNING("path[i].key != path[i + 1].max_key");

            ::sleep(1);
            ::abort();
        }
        ++i; /* move down */
        if (i >= MAX_HEIGHT)
        {
            P_WARNING("i=%d >= MAX_HEIGHT", i);

            ::sleep(1);
            ::abort();
        }
    }
    return path[i]->get_key(pos[i]) != id;
}

template<typename TMemoryPool, int32_t WIDE, uint32_t ALIGNMENT>
bool CowBtree<TMemoryPool, WIDE, ALIGNMENT>::
    ModifyContext::insert_internal(const Key id, void *payload)
{
    const uint32_t node_sz = node_size();
    const uint32_t leaf_sz = leaf_size(tree.m_payload_len);
    const int payload_len = tree.m_payload_len;
    const int x = ((WIDE + 1) >> 1); /* child num */
    const int y = WIDE + 1 - x; /* next child num */

    int pos[MAX_HEIGHT];
    node_t *path[MAX_HEIGHT];

    DelayPool *const pool = tree.m_pool;

    path[0] = (node_t *)pool->addr(root);
    if (likely(path[0])) /* not empty tree */
    {
        std::vector<vaddr_t> old_nodes;
        if (!path[0]->is_new())
        {
            old_nodes.push_back(root);
        }
        int i = 0;
        int j = 0;
        int count = 0;
        while (1) /* find appropriate insert pos */
        {
            j = 0;
            count = path[i]->count();
            while (1)
            {
                if (unlikely(j >= count)) /* using last one, node is full */
                {
                    --j;
                    break;
                }
                if (id <= path[i]->get_key(j)) /* find suitable range */
                {
                    break;
                }
                ++j;
            }
            pos[i] = j;
            if (path[i]->is_leaf()) /* meeting leaf */
            {
                break;
            }
            path[i + 1] = (node_t *)pool->addr(path[i]->get_child(pos[i])); /* load child node */
            if (!path[i + 1]->is_new())
            {
                old_nodes.push_back(path[i]->get_child(pos[i]));
            }
#ifndef __NOT_DEBUG_COW_BTREE__
            else if(!path[i]->is_new())
            {
                P_WARNING("child is new, but parent is not, error");

                ::sleep(1);
                ::abort();
            }
#endif
            ++i; /* move down */
#ifndef __NOT_DEBUG_COW_BTREE__
            if (i >= MAX_HEIGHT)
            {
                P_WARNING("i=%d >= MAX_HEIGHT", i);

                ::sleep(1);
                ::abort();
            }
#endif
        }
        /* now, path[i] is leaf, pos[i] is insert position */
        Key key = path[i]->get_key(pos[i]);
        if (key == id && (NULL == payload || 0 == payload_len))
        { /* id already exist && need not update payload, do nothing */
            return true;
        }
        for (size_t n = 0; n < old_nodes.size(); ++n) /* collect read version nodes need free */
        {
            push_need_free(old_nodes[n]);
        }
        if (key == id) /* id already exist */
        {
            if (path[i]->is_new()) /* leaf is write version, modify it directly */
            {
                char *mem = ((char *)path[i]) + offsetof(node_t, childs) + payload_len * pos[i];
                ::memcpy(mem, payload, payload_len);
                return true;
            }
            /* copy leaf node first */
            vaddr_t leaf = pool->alloc(leaf_sz);
            if (0 == leaf)
            {
                P_WARNING("failed to alloc leaf node, sz=%u", leaf_sz);
                return false;
            }
            push_new_alloc(leaf);
            node_t *pl = (node_t *)pool->addr(leaf);
            pl->set_leaf(1); /* leaf flag */
            count = path[i]->count();
            for (j = 0; j < count; ++j) /* copy keys */
            {
                pl->set_key(j, path[i]->get_key(j));
            }
            pl->set_new(1); /* write version */
            pl->set_count(count); /* set count */
            char *from = ((char *)path[i]) + offsetof(node_t, childs);
            char *to = ((char *)pl) + offsetof(node_t, childs);
            if (pos[i] > 0)
            {
                ::memcpy(to, from, pos[i] * payload_len);
            }
            ::memcpy(to + pos[i] * payload_len, payload, payload_len);
            if (count > pos[i] + 1)
            {
                ::memcpy(to + (pos[i] + 1) * payload_len,
                        from + (pos[i] + 1) * payload_len,
                        (count - pos[i] - 1) * payload_len);
            }
            path[i] = pl; /* replace path[i] with write version */
            /* copy from leaf to root */
            vaddr_t child = leaf;
            while (--i >= 0) /* backtracking path to root */
            {
                if (path[i]->is_new()) /* path[i] is write version, modify child pointer then return */
                {
                    path[i]->set_child(pos[i], child);
                    return true;
                }
                /* copy node */
                vaddr_t vc = pool->alloc(node_sz);
                if (0 == vc)
                {
                    P_WARNING("failed to alloc node, size=%u", node_sz);
                    return false;
                }
                push_new_alloc(vc);
                node_t *pc = (node_t *)pool->addr(vc);
                pc->set_leaf(0); /* not leaf */
                count = path[i]->count();
                for (j = 0; j < count; ++j) /* copy keys & child pointers */
                {
                    pc->set_key(j, path[i]->get_key(j));
                    if (j == pos[i]) /* using just copied child */
                    {
                        pc->set_child(j, child);
                    }
                    else
                    {
                        pc->set_child(j, path[i]->get_child(j));
                    }
                }
                pc->set_new(1); /* write version */
                pc->set_count(count); /* set count */
                path[i] = pc; /* update path */
                child = vc; /* update child pointer */
            }
            root = child; /* update write version's root */
            return true;
        }
        vaddr_t child = 0;
        vaddr_t next_child = 0;
        bool splitted = false;
        while (i >= 0) /* backtracking path to root */
        {
            node_t *&cur = path[i];
            count = cur->count();
            if (unlikely(cur->is_leaf()))
            {
                if (likely(!cur->is_new())) /* copy it first */
                {
                    vaddr_t leaf = pool->alloc(leaf_sz);
                    if (0 == leaf)
                    {
                        P_WARNING("failed to alloc leaf node, sz=%u", leaf_sz);
                        return false;
                    }
                    push_new_alloc(leaf);
                    node_t *pl = (node_t *)pool->addr(leaf);
                    pl->set_leaf(1); /* leaf flag */
                    for (j = 0; j < count; ++j) /* copy keys & count */
                    {
                        pl->set_key(j, cur->get_key(j));
                    }
                    pl->set_new(1); /* write version */
                    pl->set_count(count); /* set count */
                    if (payload_len > 0) /* copy payloads */
                    {
                        char *from = ((char *)cur) + offsetof(node_t, childs);
                        char *to = ((char *)pl) + offsetof(node_t, childs);
                        ::memcpy(to, from, count * payload_len);
                    }
                    cur = pl; /* update path */
                    child = leaf; /* set child pointer */
                }
                else
                {
                    if (i > 0)
                    {
                        child = path[i - 1]->get_child(pos[i - 1]); /* get child pointer from parent node */
                    }
                    else
                    {
                        child = root; /* root node */
                    }
                }
                key = cur->get_key(pos[i]); /* insert position is pos[i] */
                if (id > key) /* append at end */
                {
#ifndef __NOT_DEBUG_COW_BTREE__
                    if (pos[i] + 1 != count)
                    {
                        P_WARNING("insert: pos[%d] + 1 != count[%d]", pos[i], count);

                        ::sleep(1);
                        ::abort();
                    }
#endif
                    ++pos[i];
                }
                if (unlikely(WIDE == count)) /* need to split */
                {
                    next_child = pool->alloc(leaf_sz); /* alloc new leaf */
                    if (0 == next_child)
                    {
                        P_WARNING("failed to alloc leaf node, sz=%u", leaf_sz);
                        return false;
                    }
                    push_new_alloc(next_child);
                    node_t *pl = (node_t *)pool->addr(next_child);
                    pl->set_leaf(1); /* set leaf flag */
                    if (pos[i] < x) /* insert pos in child */
                    {
                        for (int j = 0; j < y; ++j) /* copy keys from child to next child */
                        {
                            pl->set_key(j, cur->get_key(j + x - 1));
                        }
                        for (int j = x - 1; j > pos[i]; --j) /* move keys in child */
                        {
                            cur->set_key(j, cur->get_key(j - 1));
                        }
                        cur->set_key(pos[i], id); /* insert key in child */
                        if (payload_len > 0)
                        {
                            char *from = ((char *)cur) + offsetof(node_t, childs);
                            char *to = ((char *)pl) + offsetof(node_t, childs);
                            ::memcpy(to, from + (x - 1) * payload_len, y * payload_len);
                            if (pos[i] < x - 1)
                            {
                                ::memmove(from + (pos[i] + 1) * payload_len,
                                        from + pos[i] * payload_len,
                                        (x - 1 - pos[i]) * payload_len);
                            }
                            if (payload)
                            {
                                ::memcpy(from + pos[i] * payload_len, payload, payload_len);
                            }
                            else
                            {
                                ::memset(from + pos[i] * payload_len, 0, payload_len);
                            }
                        }
                    }
                    else /* insert pos in next child */
                    {
                        for (int j = x; j < x + y; ++j) /* copy keys from child to next child & insert key in next child */
                        {
                            if (j < pos[i])
                            {
                                pl->set_key(j - x, cur->get_key(j));
                            }
                            else if (j == pos[i])
                            {
                                pl->set_key(j - x, id);
                            }
                            else
                            {
                                pl->set_key(j - x, cur->get_key(j - 1));
                            }
                        }
                        if (payload_len > 0)
                        {
                            char *from = ((char *)cur) + offsetof(node_t, childs);
                            char *to = ((char *)pl) + offsetof(node_t, childs);
                            if (pos[i] == x)
                            {
                                ::memcpy(to + payload_len, from + x * payload_len, (y - 1) * payload_len);
                            }
                            else if (pos[i] == x + y - 1)
                            {
                                ::memcpy(to, from + x * payload_len, (y - 1) * payload_len);
                            }
                            else
                            {
                                ::memcpy(to, from + x * payload_len, (pos[i] - x) * payload_len);
                                ::memcpy(to + (pos[i] - x + 1) * payload_len,
                                        from + pos[i] * payload_len,
                                        (x + y - 1 - pos[i]) * payload_len);
                            }
                            if (payload)
                            {
                                ::memcpy(to + (pos[i] - x) * payload_len, payload, payload_len);
                            }
                            else
                            {
                                ::memset(to + (pos[i] - x) * payload_len, 0, payload_len);
                            }
                        }
                    }
                    cur->set_count(x); /* set count */

                    pl->set_new(1); /* write version */
                    pl->set_count(y); /* set count */

                    splitted = true;
                }
                else /* have enough space */
                {
                    if (id < key) /* insert before pos[i] */
                    {
                        for (j = count; j > pos[i]; --j) /* move */
                        {
                            cur->set_key(j, cur->get_key(j - 1));
                        }
                        if (payload_len > 0)
                        {
                            char *mem = ((char *)cur) + offsetof(node_t, childs);
                            ::memmove(mem + (pos[i] + 1) * payload_len,
                                    mem + pos[i] * payload_len,
                                    (count - pos[i]) * payload_len);
                        }
                    }
                    cur->set_key(pos[i], id); /* set key */
                    cur->set_count(count + 1); /* set count */
                    if (payload_len > 0)
                    {
                        char *mem = ((char *)cur) + offsetof(node_t, childs);
                        if (payload)
                        {
                            ::memcpy(mem + pos[i] * payload_len, payload, payload_len);
                        }
                        else
                        {
                            ::memset(mem + pos[i] * payload_len, 0, payload_len);
                        }
                    }
                    splitted = false;
                }
            }
            else /* internal node */
            {
                if (unlikely(splitted)) /* should insert again */
                {
                    const Key key1 = ((node_t *)pool->addr(child))->get_key(x - 1);
                    const Key key2 = ((node_t *)pool->addr(next_child))->get_key(y - 1);

                    if (unlikely(WIDE == count)) /* need to split */
                    {
                        ++pos[i]; /* insert after pos[i], so ++ */
                        if (!cur->is_new()) /* copy it first */
                        {
                            vaddr_t vnode = pool->alloc(node_sz);
                            if (0 == vnode)
                            {
                                P_WARNING("failed to alloc node, sz=%u", node_sz);
                                return false;
                            }
                            vaddr_t vnext = pool->alloc(node_sz);
                            if (0 == vnext)
                            {
                                P_WARNING("failed to alloc node, sz=%u", node_sz);
                                pool->free(vnode, node_sz);
                                return false;
                            }
                            push_new_alloc(vnode);
                            push_new_alloc(vnext);
                            node_t *pl = (node_t *)pool->addr(vnode);
                            node_t *pr = (node_t *)pool->addr(vnext);
                            pl->set_leaf(0); /* set leaf flag */
                            pr->set_leaf(0); /* set leaf flag */
                            if (pos[i] < x) /* insert in pl */
                            {
                                for (int j = 0; j < x + y; ++j)
                                {
                                    if (j < pos[i])
                                    {
                                        if (j + 1 == pos[i])
                                        {
                                            pl->set_key(j, key1);
                                            pl->set_child(j, child); /* update child */
                                        }
                                        else
                                        {
                                            pl->set_key(j, cur->get_key(j));
                                            pl->set_child(j, cur->get_child(j));
                                        }
                                    }
                                    else if (j == pos[i])
                                    {
                                        pl->set_key(j, key2);
                                        pl->set_child(j, next_child); /* insert next child */
                                    }
                                    else if (j < x)
                                    {
                                        pl->set_key(j, cur->get_key(j - 1));
                                        pl->set_child(j, cur->get_child(j - 1));
                                    }
                                    else
                                    {
                                        pr->set_key(j - x, cur->get_key(j - 1));
                                        pr->set_child(j - x, cur->get_child(j - 1));
                                    }
                                }
                            }
                            else /* insert in pr */
                            {
                                for (int j = 0; j < x + y; ++j)
                                {
                                    if (j < x)
                                    {
                                        pl->set_key(j, cur->get_key(j));
                                        pl->set_child(j, cur->get_child(j));
                                    }
                                    else if (j < pos[i])
                                    {
                                        pr->set_key(j - x, cur->get_key(j));
                                        pr->set_child(j - x, cur->get_child(j));
                                    }
                                    else if (j == pos[i])
                                    {
                                        pr->set_key(j - x, key2);
                                        pr->set_child(j - x, next_child); /* insert next child */
                                    }
                                    else
                                    {
                                        pr->set_key(j - x, cur->get_key(j - 1));
                                        pr->set_child(j - x, cur->get_child(j - 1));
                                    }
                                }
                                if (pos[i] == x)
                                {
                                    pl->set_key(x - 1, key1);
                                    pl->set_child(x - 1, child); /* update child in pl */
                                }
                                else
                                {
                                    pr->set_key(pos[i] - x - 1, key1);
                                    pr->set_child(pos[i] - x - 1, child); /* update child in pr */
                                }
                            }
                            pl->set_new(1); /* make sure */
                            pl->set_count(x); /* set count */

                            pr->set_new(1); /* make sure */
                            pr->set_count(y); /* set count */

                            child = vnode; /* update child */
                            next_child = vnext; /* update next child */
                        }
                        else
                        {
                            vaddr_t vnext = pool->alloc(node_sz); /* alloc new node */
                            if (0 == vnext)
                            {
                                P_WARNING("failed to alloc node, sz=%u", node_sz);
                                return false;
                            }
                            push_new_alloc(vnext);
                            node_t *pr = (node_t *)pool->addr(vnext);
                            pr->set_leaf(0); /* make sure */
                            if (pos[i] < x) /* insert pos in child */
                            {
                                for (j = 0; j < y; ++j) /* copy keys & child pointers from child to next child */
                                {
                                    pr->set_key(j, cur->get_key(j + x - 1));
                                    pr->set_child(j, cur->get_child(j + x - 1));
                                }
                                for (j = x - 1; j > pos[i]; --j) /* move keys in child */
                                {
                                    cur->set_key(j, cur->get_key(j - 1));
                                    cur->set_child(j, cur->get_child(j - 1));
                                }
                                cur->set_key(pos[i] - 1, key1);
                                cur->set_child(pos[i] - 1, child); /* replace child pointer */
                                cur->set_key(pos[i], key2);
                                cur->set_child(pos[i], next_child); /* insert next child pointer */
                            }
                            else /* insert pos in next child */
                            {
                                for (int j = x; j < x + y; ++j) /* copy keys & child pointers from child to next child */
                                {
                                    if (j < pos[i])
                                    {
                                        pr->set_key(j - x, cur->get_key(j));
                                        pr->set_child(j - x, cur->get_child(j));
                                    }
                                    else if (j == pos[i])
                                    {
                                        pr->set_key(j - x, key2);
                                        pr->set_child(j - x, next_child); /* insert next child pointer */
                                    }
                                    else
                                    {
                                        pr->set_key(j - x, cur->get_key(j - 1));
                                        pr->set_child(j - x, cur->get_child(j - 1));
                                    }
                                }
                                if (pos[i] == x)
                                {
                                    cur->set_key(x - 1, key1); /* update child in cur */
                                    cur->set_child(x - 1, child); /* replace child pointer */
                                }
                                else
                                {
                                    pr->set_key(pos[i] - x - 1, key1); /* update child in pr */
                                    pr->set_child(pos[i] - x - 1, child); /* replace child pointer */
                                }
                            }
                            cur->set_new(1); /* make sure */
                            cur->set_count(x); /* set count */

                            pr->set_new(1); /* make sure */
                            pr->set_count(y); /* set count */

                            if (i > 0)
                            {
                                child = path[i - 1]->get_child(pos[i - 1]); /* get child pointer from parent node */
                            }
                            else
                            {
                                child = root; /* root node */
                            }
                            next_child = vnext; /* update next child */
                        }
                        splitted = true;
                    }
                    else /* have enough space */
                    {
                        if (!cur->is_new()) /* copy it first & insert */
                        {
                            vaddr_t node = pool->alloc(node_sz);
                            if (0 == node)
                            {
                                P_WARNING("failed to alloc node, sz=%u", node_sz);
                                return false;
                            }
                            push_new_alloc(node);
                            node_t *pl = (node_t *)pool->addr(node);
                            pl->set_leaf(0); /* internal node */
                            for (j = 0; j < pos[i]; ++j) /* copy keys & childs */
                            {
                                pl->set_key(j, cur->get_key(j));
                                pl->set_child(j, cur->get_child(j)); /* copy child pointer */
                            }
                            pl->set_key(pos[i], key1);
                            pl->set_child(pos[i], child); /* replace child pointer */
                            pl->set_key(pos[i] + 1, key2);
                            pl->set_child(pos[i] + 1, next_child); /* insert next child pointer */
                            for (++j; j < count; ++j) /* copy keys & childs */
                            {
                                pl->set_key(j + 1, cur->get_key(j));
                                pl->set_child(j + 1, cur->get_child(j)); /* copy child pointer */
                            }
                            pl->set_count(count + 1); /* set count */
                            cur = pl; /* update path */
                            child = node; /* set child pointer */
                        }
                        else /* insert it */
                        {
                            for (j = count; j > pos[i] + 1; --j) /* move */
                            {
                                cur->set_key(j, cur->get_key(j - 1));
                                cur->set_child(j, cur->get_child(j - 1));
                            }
                            cur->set_key(pos[i], key1);
                            cur->set_child(pos[i], child); /* replace child pointer */
                            cur->set_key(pos[i] + 1, key2);
                            cur->set_child(pos[i] + 1, next_child); /* insert next child pointer */
                            cur->set_count(count + 1); /* set count */
                            if (i > 0)
                            {
                                child = path[i - 1]->get_child(pos[i - 1]); /* get child pointer from parent node */
                            }
                            else
                            {
                                child = root; /* root node */
                            }
                        }
                        cur->set_new(1); /* write version */
                        splitted = false;
                    }
                }
                else /* do not need to insert, just copy */
                {
                    if (!cur->is_new()) /* copy it first */
                    {
                        vaddr_t node = pool->alloc(node_sz);
                        if (0 == node)
                        {
                            P_WARNING("failed to alloc node, sz=%u", node_sz);
                            return false;
                        }
                        push_new_alloc(node);
                        node_t *pl = (node_t *)pool->addr(node);
                        pl->set_leaf(0); /* internal node */
                        for (j = 0; j < count; ++j) /* copy keys & childs */
                        {
                            pl->set_key(j, cur->get_key(j));
                            pl->set_child(j, cur->get_child(j)); /* copy child pointer */
                        }
                        pl->set_child(pos[i], child); /* replace child pointer */
                        cur = pl; /* update path */
                        child = node; /* set child pointer */
                    }
                    else
                    {
                        cur->set_child(pos[i], child); /* replace child pointer */
                        if (i > 0)
                        {
                            child = path[i - 1]->get_child(pos[i - 1]); /* get child pointer from parent node */
                        }
                        else
                        {
                            child = root; /* root node */
                        }
                    }
                    if (id > cur->get_key(pos[i])) /* try to update split value */
                    {
                        cur->set_key(pos[i], id);
                    }
                    cur->set_new(1); /* write version */
                    cur->set_count(count); /* set count */
                }
            }
            --i;
        }
        if (splitted) /* tree's height is increasing, generate new root */
        {
            const Key key1 = ((node_t *)pool->addr(child))->get_key(x - 1);
            const Key key2 = ((node_t *)pool->addr(next_child))->get_key(y - 1);

            vaddr_t vr = pool->alloc(node_sz);
            if (0 == vr)
            {
                P_WARNING("failed to alloc node, sz=%u", node_sz);
                return false;
            }
            push_new_alloc(vr);
            node_t *node = (node_t *)pool->addr(vr);
            node->set_leaf(0);
            node->set_key(0, key1);
            node->set_new(1);
            node->set_key(1, key2);
            node->set_count(2); /* set count */
            node->set_child(0, child);
            node->set_child(1, next_child);

            root = vr;
        }
        else
        {
            root = child; /* try to update root */
        }
    }
    else /* empty tree, insert first element */
    {
        root = pool->alloc(leaf_sz);
        if (0 == root)
        {
            P_WARNING("failed to alloc leaf node, sz=%u", leaf_sz);
            return false;
        }
        push_new_alloc(root);
        node_t *pl = (node_t *)pool->addr(root);
        pl->set_leaf(1);
        pl->set_new(1);
        pl->set_key(0, id);
        pl->set_count(1); /* set count */
        if (payload_len > 0)
        {
            char *mem = ((char *)pl) + offsetof(node_t, childs);
            if (payload)
            {
                ::memcpy(mem, payload, payload_len);
            }
            else
            {
                ::memset(mem, 0, payload_len);
            }
        }
    }
    ++size;
    return true;
}

template<typename TMemoryPool, int32_t WIDE, uint32_t ALIGNMENT>
bool CowBtree<TMemoryPool, WIDE, ALIGNMENT>::
    ModifyContext::remove_internal(const Key id, bool *p_flag)
{
    const uint32_t node_sz = node_size();
    const uint32_t leaf_sz = leaf_size(tree.m_payload_len);
    const int payload_len = tree.m_payload_len;
    const int x = ((WIDE + 1) >> 1); /* child num */

    int pos[MAX_HEIGHT];
    node_t *path[MAX_HEIGHT];

    DelayPool *const pool = tree.m_pool;

    if (p_flag)
    {
        *p_flag = false;
    }
    path[0] = (node_t *)pool->addr(root);
    if (NULL == path[0]) /* empty tree */
    {
        return true;
    }
    std::vector<vaddr_t> old_nodes;
    if (!path[0]->is_new())
    {
        old_nodes.push_back(root);
    }
    int i = 0;
    int j = 0;
    int count = 0;
    while (1) /* find id */
    {
        j = 0;
        count = path[i]->count();
        while (1)
        {
            if (unlikely(j >= count)) /* meet end */
            {
                return true;
            }
            if (id <= path[i]->get_key(j)) /* find suitable range */
            {
                break;
            }
            ++j;
        }
        pos[i] = j;
        if (path[i]->is_leaf()) /* meeting leaf */
        {
            break;
        }
        path[i + 1] = (node_t *)pool->addr(path[i]->get_child(pos[i])); /* load child node */
        if (!path[i + 1]->is_new())
        {
            old_nodes.push_back(path[i]->get_child(pos[i]));
        }
#ifndef __NOT_DEBUG_COW_BTREE__
        else if (!path[i]->is_new())
        {
            P_WARNING("child is new, but parent is not, error");

            ::sleep(1);
            ::abort();
        }
#endif
        ++i; /* move down */
#ifndef __NOT_DEBUG_COW_BTREE__
        if (i >= MAX_HEIGHT)
        {
            P_WARNING("i=%d >= MAX_HEIGHT", i);

            ::sleep(1);
            ::abort();
        }
#endif
    }
    /* now, path[i] is leaf, pos[i] is insert position */
    if (path[i]->get_key(pos[i]) != id) /* id is not exist in tree */
    {
        return true;
    }
    /* find id, need to remove it */
    for (size_t n = 0; n < old_nodes.size(); ++n) /* collect read version nodes need free */
    {
        push_need_free(old_nodes[n]);
    }
    vaddr_t child = 0;
    bool collapsed = false;
    while (i >= 0)
    {
        node_t *&cur = path[i];
        count = cur->count();
        if (unlikely(cur->is_leaf()))
        {
            if (likely(!cur->is_new())) /* copy it first */
            {
                vaddr_t leaf = pool->alloc(leaf_sz);
                if (0 == leaf)
                {
                    P_WARNING("failed to alloc leaf node, sz=%u", leaf_sz);
                    return false;
                }
                push_new_alloc(leaf);
                node_t *pl = (node_t *)pool->addr(leaf);
                pl->set_leaf(1); /* leaf flag */
                for (j = 0; j < pos[i]; ++j) /* copy keys & count */
                {
                    pl->set_key(j, cur->get_key(j));
                }
                for (++j; /* skip pos[i] */
                        j < count; ++j) /* copy keys & count */
                {
                    pl->set_key(j - 1, cur->get_key(j));
                }
                if (payload_len > 0) /* copy payloads */
                {
                    char *from = ((char *)cur) + offsetof(node_t, childs);
                    char *to = ((char *)pl) + offsetof(node_t, childs);
                    if (0 == pos[i])
                    {
                        ::memcpy(to, from + payload_len, (count - 1) * payload_len);
                    }
                    else if (count == pos[i] + 1)
                    {
                        ::memcpy(to, from, (count - 1) * payload_len);
                    }
                    else
                    {
                        ::memcpy(to, from, pos[i] * payload_len);
                        ::memcpy(to + pos[i] * payload_len,
                                from + (pos[i] + 1) * payload_len,
                                (count - pos[i] - 1) * payload_len);
                    }
                }
                cur = pl; /* update path */
                child = leaf; /* set child pointer */
            }
            else /* write version already */
            {
                for (j = pos[i] + 1; j < count; ++j) /* move keys */
                {
                    cur->set_key(j - 1, cur->get_key(j));
                }
                if (payload_len > 0 && count > pos[i] + 1) /* move payloads */
                {
                    char *from = ((char *)cur) + offsetof(node_t, childs);
                    ::memmove(from + pos[i] * payload_len,
                            from + (pos[i] + 1) * payload_len,
                            (count - pos[i] - 1) * payload_len);
                }
                if (i > 0)
                {
                    child = path[i - 1]->get_child(pos[i - 1]); /* get child pointer from parent node */
                }
                else
                {
                    child = root; /* root node */
                }
            }
            cur->set_new(1); /* write version */
            cur->set_count(--count); /* set count */

            collapsed = count < x; /* check rule */
        }
        else /* internal node */
        {
            if (!cur->is_new()) /* copy it first */
            {
                vaddr_t node = pool->alloc(node_sz);
                if (0 == node)
                {
                    P_WARNING("failed to alloc node, sz=%u", node_sz);
                    return false;
                }
                push_new_alloc(node);
                node_t *pl = (node_t *)pool->addr(node);
                pl->set_leaf(0); /* not leaf */
                for (j = 0; j < count; ++j) /* copy keys & childs */
                {
                    pl->set_key(j, cur->get_key(j));
                    pl->set_child(j, cur->get_child(j)); /* copy child pointer */
                }
                pl->set_child(pos[i], child); /* replace child pointer */
                pl->set_new(1); /* write version */
                pl->set_count(count); /* set count */

                cur = pl; /* update path */
                child = node; /* set child pointer */
            }
            else /* write version already */
            {
                cur->set_child(pos[i], child); /* replace child pointer */
                if (i > 0)
                {
                    child = path[i - 1]->get_child(pos[i - 1]); /* get child pointer from parent node */
                }
                else
                {
                    child = root; /* root node */
                }
            }
            if (unlikely(collapsed)) /* need to adjust */
            {
                node_t *self = (node_t *)pool->addr(cur->get_child(pos[i]));
                node_t *pre = NULL;
                node_t *next = NULL;
                if (pos[i] + 1 < count)
                {
                    next = (node_t *)pool->addr(cur->get_child(pos[i] + 1));
                    if (next->count() <= x) /* not enough keys */
                    {
                        next = NULL;
                    }
                }
                if (NULL == next)
                {
                    if (pos[i] >= 1)
                    {
                        pre = (node_t *)pool->addr(cur->get_child(pos[i] - 1));
                        if (pre->count() <= x) /* not enough keys */
                        {
                            pre = NULL;
                        }
                    }
                }
                collapsed = false; /* set by default */
                if (next) /* borrow one key from next neighbour */
                {
                    const int sz = next->count();

                    cur->set_key(pos[i], next->get_key(0));
                    self->set_key(x - 1, next->get_key(0));
                    if (!self->is_leaf()) /* not leaf, copy child pointer */
                    {
                        self->set_child(x - 1, next->get_child(0));
                    }
                    else if (payload_len > 0) /* copy payload */
                    {
                        char *from = ((char *)next) + offsetof(node_t, childs);
                        char *to = ((char *)self) + offsetof(node_t, childs);
                        ::memcpy(to + (x - 1) * payload_len, from, payload_len);
                    }
                    self->set_count(x);

                    if (!next->is_new()) /* copy new first */
                    {
                        vaddr_t node = 0;
                        if (next->is_leaf())
                        {
                            node = pool->alloc(leaf_sz);
                            if (0 == node)
                            {
                                P_WARNING("failed to alloc leaf node, sz=%u", leaf_sz);
                                return false;
                            }
                        }
                        else
                        {
                            node = pool->alloc(node_sz);
                            if (0 == node)
                            {
                                P_WARNING("failed to alloc node, sz=%u", node_sz);
                                return false;
                            }
                        }
                        push_new_alloc(node);
                        node_t *pl = (node_t *)pool->addr(node);
                        pl->set_leaf(next->is_leaf());
                        if (next->is_leaf())
                        {
                            for (j = 1; j < sz; ++j) /* copy keys */
                            {
                                pl->set_key(j - 1, next->get_key(j));
                            }
                            if (payload_len > 0) /* copy payloads */
                            {
                                char *from = ((char *)next) + offsetof(node_t, childs);
                                char *to = ((char *)pl) + offsetof(node_t, childs);
                                ::memcpy(to, from + payload_len, (sz - 1) * payload_len);
                            }
                        }
                        else
                        {
                            for (j = 1; j < sz; ++j) /* copy keys & childs */
                            {
                                pl->set_key(j - 1, next->get_key(j));
                                pl->set_child(j - 1, next->get_child(j)); /* copy child pointer */
                            }
                        }
                        pl->set_new(1);
                        pl->set_count(sz - 1);

                        push_need_free(cur->get_child(pos[i] + 1)); /* need to free read version */
                        cur->set_child(pos[i] + 1, node); /* update parent's child pointer */
                    }
                    else /* direct modify */
                    {
                        if (next->is_leaf())
                        {
                            for (j = 1; j < sz; ++j) /* copy keys */
                            {
                                next->set_key(j - 1, next->get_key(j));
                            }
                            if (payload_len > 0) /* copy payloads */
                            {
                                char *to = ((char *)next) + offsetof(node_t, childs);
                                ::memmove(to, to + payload_len, (sz - 1) * payload_len);
                            }
                        }
                        else
                        {
                            for (j = 1; j < sz; ++j) /* move keys & childs */
                            {
                                next->set_key(j - 1, next->get_key(j));
                                next->set_child(j - 1, next->get_child(j)); /* copy child pointer */
                            }
                        }
                        next->set_count(sz - 1);
                    }
                }
                else if (pre) /* borrow one key from pre neighbour */
                {
                    const int sz = pre->count();

                    if (self->is_leaf())
                    {
                        for (j = x; j > 1; --j) /* move keys */
                        {
                            self->set_key(j - 1, self->get_key(j - 2));
                        }
                        if (payload_len > 0) /* move & copy payloads */
                        {
                            char *from = ((char *)pre) + offsetof(node_t, childs);
                            char *to = ((char *)self) + offsetof(node_t, childs);
                            ::memmove(to + payload_len, to, (x - 1) * payload_len);
                            ::memcpy(to, from + (sz - 1) * payload_len, payload_len);
                        }
                    }
                    else
                    {
                        for (j = x; j > 1; --j) /* move keys & childs */
                        {
                            self->set_key(j - 1, self->get_key(j - 2));
                            self->set_child(j - 1, self->get_child(j - 2));
                        }
                        self->set_child(0, pre->get_child(sz - 1));
                    }
                    self->set_key(0, pre->get_key(sz - 1));
                    self->set_count(x);

                    cur->set_key(pos[i] - 1, pre->get_key(sz - 2));
                    if (id == cur->get_key(pos[i])) /* try to update max value */
                    {
                        cur->set_key(pos[i], self->get_key(x - 1));
                    }

                    if (!pre->is_new()) /* copy pre first */
                    {
                        vaddr_t node = 0;
                        if (pre->is_leaf())
                        {
                            node = pool->alloc(leaf_sz);
                            if (0 == node)
                            {
                                P_WARNING("failed to alloc leaf node, sz=%u", leaf_sz);
                                return false;
                            }
                        }
                        else
                        {
                            node = pool->alloc(node_sz);
                            if (0 == node)
                            {
                                P_WARNING("failed to alloc node, sz=%u", node_sz);
                                return false;
                            }
                        }
                        push_new_alloc(node);
                        node_t *pl = (node_t *)pool->addr(node);
                        pl->set_leaf(pre->is_leaf());
                        if (pre->is_leaf())
                        {
                            for (j = 0; j < sz - 1; ++j) /* copy keys */
                            {
                                pl->set_key(j, pre->get_key(j));
                            }
                            if (payload_len > 0) /* copy payloads */
                            {
                                char *from = ((char *)pre) + offsetof(node_t, childs);
                                char *to = ((char *)pl) + offsetof(node_t, childs);
                                ::memcpy(to, from, (sz - 1) * payload_len);
                            }
                        }
                        else
                        {
                            for (j = 0; j < sz - 1; ++j) /* copy keys & childs */
                            {
                                pl->set_key(j, pre->get_key(j));
                                pl->set_child(j, pre->get_child(j)); /* copy child pointer */
                            }
                        }
                        pl->set_new(1);
                        pl->set_count(sz - 1);

                        push_need_free(cur->get_child(pos[i] - 1)); /* need to free read version */
                        cur->set_child(pos[i] - 1, node); /* update parent's child pointer */
                    }
                    else
                    {
                        pre->set_count(sz - 1);
                    }
                }
                else /* need to merge nodes */
                {
                    pre = NULL;
                    next = NULL;
                    vaddr_t addr;
                    bool is_new = false;
                    if (pos[i] + 1 < count)
                    {
                        addr = cur->get_child(pos[i] + 1);
                        next = (node_t *)pool->addr(addr);
                        is_new = next->is_new();
                    }
                    if (NULL == next)
                    {
                        addr = cur->get_child(pos[i] - 1);
                        pre = (node_t *)pool->addr(addr); /* pos[i] must greater than 0 here */
                        is_new = pre->is_new();
                    }
                    if (next)
                    {
                        const int sz = next->count();
                        if (self->is_leaf())
                        {
                            for (j = 0; j < sz; ++j)
                            {
                                self->set_key(j + x - 1, next->get_key(j));
                            }
                            if (payload_len > 0)
                            {
                                char *from = ((char *)next) + offsetof(node_t, childs);
                                char *to = ((char *)self) + offsetof(node_t, childs);
                                ::memcpy(to + (x - 1) * payload_len, from, sz * payload_len);
                            }
                        }
                        else
                        {
                            for (j = 0; j < sz; ++j)
                            {
                                self->set_key(j + x - 1, next->get_key(j));
                                self->set_child(j + x - 1, next->get_child(j));
                            }
                        }
                        self->set_count(x + sz - 1);
                        for (j = pos[i] + 1; j + 1 < count; ++j)
                        {
                            cur->set_key(j, cur->get_key(j + 1));
                            cur->set_child(j, cur->get_child(j + 1));
                        }
                        cur->set_key(pos[i], next->get_key(sz - 1));
                    }
                    else
                    {
                        const int sz = pre->count();
                        if (self->is_leaf())
                        {
                            for (j = x - 2; j >= 0; --j) /* move */
                            {
                                self->set_key(j + sz, self->get_key(j));
                            }
                            for (j = 0; j < sz; ++j)
                            {
                                self->set_key(j, pre->get_key(j));
                            }
                            if (payload_len > 0)
                            {
                                char *from = ((char *)pre) + offsetof(node_t, childs);
                                char *to = ((char *)self) + offsetof(node_t, childs);
                                ::memmove(to + sz * payload_len, to, (x - 1) * payload_len);
                                ::memcpy(to, from, sz * payload_len);
                            }
                        }
                        else
                        {
                            for (j = x - 2; j >= 0; --j) /* move */
                            {
                                self->set_key(j + sz, self->get_key(j));
                                self->set_child(j + sz, self->get_child(j));
                            }
                            for (j = 0; j < sz; ++j)
                            {
                                self->set_key(j, pre->get_key(j));
                                self->set_child(j, pre->get_child(j));
                            }
                        }
                        self->set_count(x + sz - 1);

                        if (id == cur->get_key(pos[i])) /* try to update max value */
                        {
                            cur->set_key(pos[i], self->get_key(x + sz - 2));
                        }
                        for (j = pos[i] - 1; j + 1 < count; ++j)
                        {
                            cur->set_key(j, cur->get_key(j + 1));
                            cur->set_child(j, cur->get_child(j + 1));
                        }
                    }
                    cur->set_count(--count);
                    collapsed = count < x;
                    if (is_new)
                    { /* free it immediately */
                        remove_new_alloc(addr);
                        if (self->is_leaf())
                        {
                            pool->free(addr, leaf_sz);
                        }
                        else
                        {
                            pool->free(addr, node_sz);
                        }
                    }
                    else
                    {
                        push_need_free(addr); /* need to free read version */
                    }
                }
            }
            else /* do not need to remove, just copy */
            {
                if (id == cur->get_key(pos[i])) /* try to update max value */
                {
                    /* get child's max value first, then update it */
                    node_t *pc = (node_t *)pool->addr(cur->get_child(pos[i]));
                    cur->set_key(pos[i], pc->get_key(pc->count() - 1));
                }
            }
        }
        --i;
    }
    root = child;
    node_t *pr = (node_t *)pool->addr(root);
    if (pr->count() < 2)
    {
        if (pr->is_leaf())
        {
            if (pr->count() == 0) /* empty tree now */
            { /* free it immediately */
                remove_new_alloc(root);
                pool->free(root, leaf_sz);
                root = 0;
            }
        }
        else
        {
            if (pr->count() == 1) /* tree height-- */
            { /* free it immediately */
                child = pr->get_child(0);
                remove_new_alloc(root);
                pool->free(root, node_sz);
                root = child;
            }
        }
    }
    --size;
    if (p_flag)
    {
        *p_flag = true;
    }
    return true;
}

template<typename TMemoryPool, int32_t WIDE, uint32_t ALIGNMENT>
CowBtree<TMemoryPool, WIDE, ALIGNMENT>::
    ModifyContext::~ModifyContext()
{
    const int node_sz = node_size();
    const int leaf_sz = leaf_size(tree.m_payload_len);
    DelayPool *const pool = tree.m_pool;
#ifndef __NOT_DEBUG_COW_BTREE__
    {
        iterator it = tree.begin(true);
        while (*it != -1)
        {
            ++it;
            --old_size;
        }
        if (0 != old_size)
        {
            P_WARNING("old_size check failed");

            ::sleep(1);
            ::abort();
        }
    }
#endif
    if (has_error) /* roll back */
    {
        for (typename std::set<vaddr_t>::const_iterator it = new_alloc.begin(),
                end = new_alloc.end(); it != end; ++it)
        {
            vaddr_t va = *it;
            node_t *node = (node_t *)pool->addr(va);
            if (node->is_leaf())
            {
                pool->free(va, leaf_sz);
            }
            else
            {
                pool->free(va, node_sz);
            }
        }
    }
    else /* publish write version */
    {
        for (typename std::set<vaddr_t>::const_iterator it = new_alloc.begin(),
                end = new_alloc.end(); it != end; ++it)
        {
            vaddr_t va = *it;
            node_t *node = (node_t *)pool->addr(va);
            node->set_new(0);
        }
#ifndef __NOT_DEBUG_COW_BTREE__
        for (typename std::set<vaddr_t>::const_iterator it = need_free.begin(),
                end = need_free.end(); it != end; ++it)
        {
            vaddr_t va = *it;
            node_t *node = (node_t *)pool->addr(va);
            if (node->is_leaf())
            {
                pool->delay_free(va, leaf_sz);
            }
            else
            {
                pool->delay_free(va, node_sz);
            }
        }
#else
        while (need_free.size() > 0)
        {
            vaddr_t va = need_free.front();
            node_t *node = (node_t *)pool->addr(va);
            if (node->is_leaf())
            {
                pool->delay_free(va, leaf_sz);
            }
            else
            {
                pool->delay_free(va, node_sz);
            }
            need_free.pop_front();
        }
#endif
        tree.m_root = root;
        tree.m_size += size;
#ifndef __NOT_DEBUG_COW_BTREE__
        if (!tree.validate())
        {
            P_WARNING("failed to validate tree");

            ::sleep(1);
            ::abort();
        }
#endif
    }
}

#endif
