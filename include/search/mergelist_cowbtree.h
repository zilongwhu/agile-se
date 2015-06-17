#ifndef __AGILE_SE_MERGELIST_COWBTREE_H__
#define __AGILE_SE_MERGELIST_COWBTREE_H__

#include "search/doclist.h"

template<typename CowBtree, typename SkipList>
class TSOrListImpl /* Big | Add */
{
    public:
        typedef typename CowBtree::iterator big_iterator;
        typedef typename SkipList::iterator add_iterator;
    public:
        TSOrListImpl(uint32_t sign, const CowBtree &big, const SkipList &add)
            : m_big(big.begin(false)),
            m_add(add.begin()),
            m_add_c(m_add)
        {
            m_sign = sign;
            m_curr = -1;
        }

        inline int32_t curr() const { return m_curr; }

        inline int32_t pick(uint32_t lid, uint32_t rid)
        {
            if (lid < rid) { return(m_curr = lid); }
            else { return(m_curr = rid); }
        }
        inline int32_t first()
        {
            m_big.first();
            m_add = m_add_c;
            int32_t lid = -1;
            if (m_big) { lid = *m_big; }
            int32_t rid = -1;
            if (m_add) { rid = *m_add; }
            return this->pick(lid, rid);
        }
        inline int32_t next()
        {
            if (m_curr == -1) { return -1; }

            int32_t lid;
            int32_t rid;
            if (m_add)
            {
                if (m_big)
                {
                    lid = *m_big;
                    if (lid == m_curr)
                    {
                        if (++m_big) { lid = *m_big; }
                        else /* 大表走完 */ { lid = -1; }
                    }
                    rid = *m_add;
                    if (rid == m_curr)
                    {
                        if (++m_add) { rid = *m_add; }
                        else /* 小表走完 */ { rid = -1; }
                    }
                    return this->pick(lid, rid);
                }
                else /* 只剩下小表没走完 */
                {
                    if (++m_add) { return(m_curr = *m_add); }
                    else /* 小表走完 */ { return(m_curr = -1); }
                }
            }
            else /* 只剩下大表没走完 */
            {
                if (++m_big) { return(m_curr = *m_big); }
                else /* 大表走完 */ { return(m_curr = -1); }
            }
        }
        inline int32_t find(int32_t docid)
        {
            if (m_curr == -1) { return -1; }
            if (m_curr >= docid) /* 只往前走 */ { return m_curr; }

            m_big.find(docid);
            m_add.find(docid);

            int32_t lid = -1;
            if (m_big) { lid = *m_big; }
            int32_t rid = -1;
            if (m_add) { rid = *m_add; }
            return this->pick(lid, rid);
        }
        inline uint32_t cost() const
        {
            return m_big.size() + m_add.size();
        }

        inline bool get_strategy_data(InvertStrategy::info_t &strategy_data)
        {
            if (m_curr == -1)
            {
                return false;
            }
            if (m_add && *m_add == m_curr) /* 来自小表 */
            {
                strategy_data.sign = m_sign;
                strategy_data.type = m_add.type();
                strategy_data.length = m_add.payload_len();
                if (strategy_data.length > 0)
                {
                    ::memcpy(strategy_data.result, m_add.payload(), strategy_data.length);
                }
                return true;
            }
            /* 来自大表 */
            strategy_data.sign = m_sign;
            strategy_data.type = m_big.type();
            strategy_data.length = m_big.payload_len();
            if (strategy_data.length > 0)
            {
                ::memcpy(strategy_data.result, m_big.payload(), strategy_data.length);
            }
            return true;
        }
    private:
        template<typename A, typename B> friend class TSOrList;
        template<typename A, typename B> friend class TSMergeList;
    private:
        uint32_t m_sign;
        int32_t m_curr;
        big_iterator m_big;
        add_iterator m_add;
        const add_iterator m_add_c;
};

template<typename CowBtree, typename SkipList>
class TSOrList: public DocList
{
    public:
        TSOrList(uint32_t sign, const CowBtree &big, const SkipList &add)
            : m_impl(sign, big, add)
        { }

        int32_t curr() { return m_impl.curr(); }
        int32_t first() { return m_impl.first(); }
        int32_t next() { return m_impl.next(); }
        int32_t find(int32_t docid) { return m_impl.find(docid); }
        uint32_t cost() const { return m_impl.cost(); }

        InvertStrategy::info_t *get_strategy_data(InvertStrategy &st)
        {
            if (!m_impl.get_strategy_data(m_strategy_data))
            {
                return NULL;
            }
            m_strategy_data.data = m_data;
            st.work(&m_strategy_data);
            return &m_strategy_data;
        }
    private:
        TSOrListImpl<CowBtree, SkipList> m_impl;
};

template<typename CowBtree, typename SkipList>
class TSMergeList: public DocList
{
    public:
        typedef typename CowBtree::iterator big_iterator;
        typedef typename SkipList::iterator add_iterator;
        typedef typename SkipList::iterator del_iterator;
    public:
        TSMergeList(uint32_t sign, const CowBtree &big, const SkipList &add, const SkipList &del)
            : m_impl(sign, big, add), m_del(del.begin()), m_del_c(m_del)
        { }

        int32_t curr() { return m_impl.curr(); }

        inline void check(int32_t lid)
        {
            while (1)
            {
                m_del.find(lid);
                if (!m_del) /* 黑名单走完 */ { return; }
                if (lid == *m_del) /* 命中黑名单 */
                {
                    lid = m_impl.next(); /* 尝试下一个 */
                    if (lid == -1) /* 链表走完 */ { return; }
                    continue;
                }
                return ;
            }
        }
        int32_t first()
        {
            int lid = m_impl.first();
            if (lid == -1) /* 链表走完 */ { return -1; }
            m_del = m_del_c;
            this->check(lid);
            return m_impl.curr();
        }
        int32_t next()
        {
            if (m_impl.curr() == -1) { return -1; }
            
            int32_t lid = m_impl.next();
            if (lid == -1) /* 链表走完 */ { return -1; } 
            this->check(lid);
            return m_impl.curr();
        }
        int32_t find(int32_t docid)
        {
            if (m_impl.curr() == -1) { return -1; }
            if (m_impl.curr() >= docid) /* 只往前走 */ { return m_impl.curr(); }
            int32_t lid = m_impl.find(docid);
            if (lid == -1) /* 链表走完 */ { return -1; }
            this->check(lid);
            return m_impl.curr();
        }
        uint32_t cost() const { return m_impl.cost(); }

        InvertStrategy::info_t *get_strategy_data(InvertStrategy &st)
        {
            if (!m_impl.get_strategy_data(m_strategy_data))
            {
                return NULL;
            }
            m_strategy_data.data = m_data;
            st.work(&m_strategy_data);
            return &m_strategy_data;
        }
    private:
        TSOrListImpl<CowBtree, SkipList> m_impl;
        del_iterator m_del;
        const del_iterator m_del_c;
};

template<typename CowBtree, typename SkipList>
class TSBigDiffList: public DocList
{
    public:
        typedef typename CowBtree::iterator big_iterator;
        typedef typename SkipList::iterator del_iterator;
    public:
        TSBigDiffList(uint32_t sign, const CowBtree &big, const SkipList &del)
            : m_big(big.begin(false)), m_del(del.begin()), m_del_c(m_del)
        {
            m_sign = sign;
            m_curr = -1;
        }

        int32_t curr() { return m_curr; }

        inline int32_t check(int32_t lid)
        {
            while (1)
            {
                m_del.find(lid);
                if (!m_del) /* 黑名单走完 */ { break; }
                if (lid == *m_del) /* 命中黑名单 */
                {
                    if (++m_big) { lid = *m_big; }
                    else /* 链表走完 */ { return(m_curr = -1); }
                    continue;
                }
                break;
            }
            return(m_curr = lid);
        }
        int32_t first()
        {
            m_big.first();
            if (!m_big) /* 链表走完 */ { return(m_curr = -1); }
            m_del = m_del_c;
            return this->check(*m_big);
        }
        int32_t next()
        {
            if (m_curr == -1) { return -1; }
            
            if (!++m_big) /* 链表走完 */ { return(m_curr = -1); }
            return this->check(*m_big);
        }
        int32_t find(int32_t docid)
        {
            if (m_curr == -1) { return -1; }
            if (m_curr >= docid) /* 只往前走 */ { return m_curr; }
            m_big.find(docid);
            if (!m_big) /* 链表走完 */ { return(m_curr = -1); }
            return this->check(*m_big);
        }
        uint32_t cost() const { return m_big.size(); }

        InvertStrategy::info_t *get_strategy_data(InvertStrategy &st)
        {
            m_strategy_data.data = m_data;
            m_strategy_data.sign = m_sign;
            m_strategy_data.type = m_big.type();
            m_strategy_data.length = m_big.payload_len();
            if (m_strategy_data.length > 0)
            {
                ::memcpy(m_strategy_data.result, m_big.payload(), m_strategy_data.length);
            }
            st.work(&m_strategy_data);
            return &m_strategy_data;
        }
    private:
        uint32_t m_sign;
        int32_t m_curr;
        big_iterator m_big;
        del_iterator m_del;
        const del_iterator m_del_c;
};

template<typename SkipList>
class TSAddDiffList: public DocList
{
    public:
        typedef typename SkipList::iterator add_iterator;
        typedef typename SkipList::iterator del_iterator;
    public:
        TSAddDiffList(uint32_t sign, const SkipList &add, const SkipList &del)
            : m_add(add.begin()), m_add_c(m_add), m_del(del.begin()), m_del_c(m_del)
        {
            m_sign = sign;
            m_curr = -1;
        }

        int32_t curr() { return m_curr; }

        inline int32_t check(int32_t lid)
        {
            while (1)
            {
                m_del.find(lid);
                if (!m_del) /* 黑名单走完 */ { break; }
                if (lid == *m_del) /* 命中黑名单 */
                {
                    if (++m_add) { lid = *m_add; }
                    else /* 链表走完 */ { return(m_curr = -1); }
                    continue;
                }
                break;
            }
            return(m_curr = lid);
        }
        int32_t first()
        {
            m_add = m_add_c;
            if (!m_add) /* 链表走完 */ { return(m_curr = -1); }
            m_del = m_del_c;
            return this->check(*m_add);
        }
        int32_t next()
        {
            if (m_curr == -1) { return -1; }
            
            if (!++m_add) /* 链表走完 */ { return(m_curr = -1); }
            return this->check(*m_add);
        }
        int32_t find(int32_t docid)
        {
            if (m_curr == -1) { return -1; }
            if (m_curr >= docid) /* 只往前走 */ { return m_curr; }
            m_add.find(docid);
            if (!m_add) /* 链表走完 */ { return(m_curr = -1); }
            return this->check(*m_add);
        }
        uint32_t cost() const { return m_add.size(); }

        InvertStrategy::info_t *get_strategy_data(InvertStrategy &st)
        {
            m_strategy_data.data = m_data;
            m_strategy_data.sign = m_sign;
            m_strategy_data.type = m_add.type();
            m_strategy_data.length = m_add.payload_len();
            if (m_strategy_data.length > 0)
            {
                ::memcpy(m_strategy_data.result, m_add.payload(), m_strategy_data.length);
            }
            st.work(&m_strategy_data);
            return &m_strategy_data;
        }
    private:
        uint32_t m_sign;
        int32_t m_curr;
        add_iterator m_add;
        const add_iterator m_add_c;
        del_iterator m_del;
        const del_iterator m_del_c;
};

#endif
