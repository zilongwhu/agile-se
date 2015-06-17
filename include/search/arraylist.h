#ifndef __AGILE_SE_ARRAYLIST_H__
#define __AGILE_SE_ARRAYLIST_H__

#include <vector>
#include "search/doclist.h"

class ArrayList: public DocList
{
    public:
        ArrayList(int8_t type, const std::vector<int32_t> &docids);
        ~ArrayList();
        int32_t first()
        {
            if (m_impl)
            {
                return m_impl->first();
            }
            else
            {
                return -1;
            }
        }
        int32_t next()
        {
            return m_impl->next();
        }
        int32_t curr()
        {
            return m_impl->curr();
        }
        int32_t find(int32_t docid)
        {
            return m_impl->find(docid);
        }
        uint32_t cost() const
        {
            return m_impl->cost();
        }
        InvertStrategy::info_t *get_strategy_data(InvertStrategy &st)
        {
            return m_impl->get_strategy_data(st);
        }

        DocList *get_impl()
        {
            DocList *tmp = m_impl;
            m_impl = NULL;
            return tmp;
        }
    private:
        void *m_raw;
        DocList *m_impl;
};

#endif
