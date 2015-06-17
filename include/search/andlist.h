#ifndef __AGILE_SE_ANDLIST_H__
#define __AGILE_SE_ANDLIST_H__

#include "search/duallist.h"

class AndList: public DualList
{
    public:
        AndList(DocList *left, DocList *right) :DualList(left, right) { }
        ~AndList() { }

        InvertStrategy::info_t *get_strategy_data(InvertStrategy &st)
        {
            st.work(m_left->get_strategy_data(st), m_right->get_strategy_data(st),
                    LIST_OP_AND, &m_strategy_data);
            return &m_strategy_data;
        }

        int32_t jump_find(int32_t lid, int32_t rid)
        {
            while (lid != rid)
            {
                if (lid < rid)
                {
                    lid = m_left->find(rid);
                    if (lid == -1) /* 左链走完 */ { return -1; }
                }
                else
                {
                    rid = m_right->find(lid);
                    if (rid == -1) /* 右链走完 */ { return -1; }
                }
            }
            return lid; /* 匹配到一个结果 */
        }
        int32_t first()
        {
            int32_t lid = m_left->first();
            if (lid == -1) /* 左链为空链 */ { return(m_curr = -1); }
            int32_t rid = m_right->first();
            if (rid == -1) /* 右链为空链 */ { return(m_curr = -1); }
            return(m_curr = jump_find(lid, rid));
        }
        int32_t next()
        {
            if (m_curr == -1) { return -1; }

            int32_t lid = m_left->curr();
            if (lid == m_curr) { lid = m_left->next(); }
            if (lid == -1) /* 左链走完 */ { return(m_curr = -1); }

            int32_t rid = m_right->curr();
            if (rid == m_curr) { rid = m_right->next(); }
            if (rid == -1) /* 右链走完 */ { return(m_curr = -1); }

            return(m_curr = jump_find(lid, rid));
        }
        int32_t find(int32_t docid)
        {
            if (m_curr == -1) { return -1; }
            if (m_curr >= docid) /* 只往前走 */ { return m_curr; }
            int32_t lid = m_left->find(docid);
            if (lid == -1) /* 左链走完 */ { return(m_curr = -1); }
            int32_t rid = m_right->find(docid);
            if (rid == -1) /* 右链走完 */ { return(m_curr = -1); }
            return(m_curr = jump_find(lid, rid));
        }
        uint32_t cost() const
        {
            const uint32_t lc = m_left->cost();
            const uint32_t rc = m_right->cost();
            return lc < rc ? lc : rc;
        }
};

#endif
