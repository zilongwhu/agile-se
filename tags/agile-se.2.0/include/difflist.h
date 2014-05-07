// =====================================================================================
// 
//       Filename:  difflist.h
// 
//    Description:  实现A-B操作
// 
//        Version:  1.0
//        Created:  12/06/2013 05:31:26 PM
//       Revision:  none
//       Compiler:  g++
// 
//         Author:  Zilong.Zhu (), zilong.whu@gmail.com
//        Company:  edu.whu
// 
// =====================================================================================

#ifndef __AGILE_SE_DIFFLIST_H__
#define __AGILE_SE_DIFFLIST_H__

#include "duallist.h"

class DiffList: public DualList
{
    public:
        DiffList(DocList *left, DocList *right): DualList(left, right) { }
        ~DiffList() { }

        InvertStrategy::info_t *get_strategy_data(InvertStrategy &st)
        {
            m_strategy_data = *m_left->get_strategy_data(st);
            return &m_strategy_data;
        }

        int32_t first()
        {
            int lid = m_left->first();
            if (lid == -1) /* 链表走完 */ { return(m_curr = -1); }
            int32_t rid = m_right->first();
            while (lid == rid) /* 命中黑名单 */
            {
                lid = m_left->next(); /* 尝试下一个 */
                if (lid == -1) /* 链表走完 */ { return(m_curr = -1); }
                rid = m_right->find(lid);
            }
            return(m_curr = lid);
        }
        int32_t next()
        {
            if (m_curr == -1) { return -1; }
            
            int32_t lid = m_left->next();
            if (lid == -1) /* 链表走完 */ { return(m_curr = -1); } 
            return find(lid);
        }
        int32_t find(int32_t docid)
        {
            if (m_curr == -1) { return -1; }
            if (m_curr >= docid) /* 只往前走 */ { return m_curr; }
            int32_t lid = m_left->find(docid);
            if (lid == -1) /* 链表走完 */ { return(m_curr = -1); }
            int32_t rid = m_right->find(docid);
            while (lid == rid) /* 命中黑名单 */
            {
                lid = m_left->next(); /* 尝试下一个 */
                if (lid == -1) /* 链表走完 */ { return(m_curr = -1); }
                rid = m_right->find(lid);
            }
            return(m_curr = lid);
        }
};

#endif
