#ifndef  __AGILE_SE_CONJUNCTION_H__
#define  __AGILE_SE_CONJUNCTION_H__

#include <vector>
#include <algorithm>
#include "search/doclist.h"

class Conjunction: public DocList
{
    private:
        struct Compare
        {
            bool operator() (DocList *left, DocList *right) const
            {
                const uint32_t lc = left->cost();
                const uint32_t rc = right->cost();
                if (lc > rc) {
                    return true;
                } else if (lc < rc) {
                    return false;
                } else {
                    return left < right;
                }
            }
        };
    public:
        Conjunction(const std::vector<DocList *> &subs)
            : m_subs(subs), m_infos(subs.size(), NULL)
        {
            m_curr = -1;
        }
        ~Conjunction()
        {
            for (size_t i = 0, sz = m_subs.size(); i < sz; ++i)
            {
                delete m_subs[i];
            }
        }

        int32_t first()
        {
            const size_t sz = m_subs.size();
            if (sz == 0) { return (m_curr = -1); }
            /* 调用first */
            for (size_t i = 0; i < sz; ++i)
            {
                if (m_subs[i]->first() == -1)
                {
                    return (m_curr = -1);
                }
            }
            /* 升序排列 */
            Compare comp;
            std::sort(m_subs.begin(), m_subs.end(), comp);

            m_curr = m_subs[sz - 1]->curr(); /* cost最小的拉链 */
            return (m_curr = this->do_next());
        }

        int32_t next()
        {
            if (-1 == m_curr) { return -1; }
            m_curr = m_subs[m_subs.size() - 1]->next(); /* 往前走 */
            if (-1 == m_curr) { return -1; }
            return (m_curr = this->do_next());
        }

        int32_t curr()
        {
            return m_curr;
        }

        int32_t find(int32_t docid)
        {
            if (-1 == m_curr) { return -1; }
            m_curr = m_subs[m_subs.size() - 1]->find(docid); /* 往前查找 */
            if (-1 == m_curr) { return -1; }
            return (m_curr = this->do_next());
        }

        uint32_t cost() const
        {
            uint32_t min = UINT_MAX;
            for (size_t i = 0; i < m_subs.size(); ++i)
            {
                const uint32_t cost = m_subs[i]->cost();
                if (cost < min)
                {
                    min = cost;
                }
            }
            return min;
        }

        InvertStrategy::info_t *get_strategy_data(InvertStrategy &st)
        {
            for (size_t i = 0, sz = m_infos.size(); i < sz; ++i)
            {
                m_infos[i] = m_subs[i]->get_strategy_data(st);
            }
            st.and_work(m_infos, &m_strategy_data);
            return &m_strategy_data;
        }
    private:
        inline int32_t do_next()
        {
            const size_t sz = m_subs.size();
            int32_t docid = m_curr; /* 最后一条拉链的当前值 */
            size_t n = 1;
            size_t i = sz - 1;
            while (n < sz)
            {
                if (++i == sz)
                {
                    i = 0;
                }
                int32_t next = m_subs[i]->find(docid); /* 往前查找 */
                if (-1 == next)
                { /* m_subs[i]走完 */
                    return -1;
                }
                if (docid == next)
                {
                    ++n;
                }
                else
                {
                    docid = next;
                    n = 1;
                }
            }
            return docid;
        }
    private:
        int32_t m_curr;
        std::vector<DocList *> m_subs;
        std::vector<const InvertStrategy::info_t *> m_infos;
};

#endif
