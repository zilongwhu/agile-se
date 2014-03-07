// =====================================================================================
// 
//       Filename:  orlist.h
// 
//    Description:  实现A|B操作
// 
//        Version:  1.0
//        Created:  12/06/2013 05:30:26 PM
//       Revision:  none
//       Compiler:  g++
// 
//         Author:  Zilong.Zhu (), zilong.whu@gmail.com
//        Company:  edu.whu
// 
// =====================================================================================

#ifndef __AGILE_SE_ORLIST_H__
#define __AGILE_SE_ORLIST_H__

#include "duallist.h"

class OrList: public DualList
{
    public:
        OrList(DocList *left, DocList *right): DualList(left, right) { }
        ~OrList() { }

        InvertStrategy::info_t *get_strategy_data(InvertStrategy &st)
        {
            st.work(m_curr == m_left->curr() ? m_left->get_strategy_data(st) : NULL,
                    m_curr == m_right->curr() ? m_right->get_strategy_data(st) : NULL,
                    LIST_OP_OR, &m_strategy_data);
            return &m_strategy_data;
        }

        int32_t pick(int32_t lid, int32_t rid)
        {
            if (lid == -1) /* 左链为空链 */ { return(m_curr = rid); }
            else if (rid == -1) /* 右链为空链 */ { return(m_curr = lid); }
            else if (lid < rid) { return(m_curr = lid); }
            else { return(m_curr = rid); }
        }
        int32_t first() { return pick(m_left->first(), m_right->first()); }
        int32_t next()
        {
            if (m_curr == -1) { return -1; }

            int32_t lid = m_left->curr();
            if (lid == m_curr) { lid = m_left->next(); }

            int32_t rid = m_right->curr();
            if (rid == m_curr) { rid = m_right->next(); }

            return pick(lid, rid);
        }
        int32_t find(int32_t docid)
        {
            if (m_curr == -1) { return -1; }
            if (m_curr >= docid) /* 只往前走 */ { return m_curr; }
            return pick(m_left->find(docid), m_right->find(docid));
        }
};

#endif
