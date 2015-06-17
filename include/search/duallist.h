#ifndef __AGILE_SE_DUALLIST_H__
#define __AGILE_SE_DUALLIST_H__

#include <stdint.h>
#include "search/doclist.h"

class DualList: public DocList
{
    public:
        DualList(DocList *left, DocList *right)
            :m_left(left), m_right(right), m_curr(-1)
        { }
        virtual ~DualList()
        {
            if (m_left)
            {
                delete m_left;
                m_left = NULL;
            }
            if (m_right)
            {
                delete m_right;
                m_right = NULL;
            }
        }
        int32_t curr() { return m_curr; }
    protected:
        DocList *m_left;
        DocList *m_right;
        int32_t m_curr;
};

#endif
