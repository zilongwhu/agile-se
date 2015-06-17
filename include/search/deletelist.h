#ifndef __AGILE_SE_DELETELIST_H__
#define __AGILE_SE_DELETELIST_H__

#include "search/doclist.h"

template<typename SkipList>
class DeleteList: public DocList
{
    public:
        typedef typename SkipList::iterator Iterator;

        DeleteList(Iterator it): m_it(it), m_it_c(it), m_end(0, it.list()) { }

        int32_t first()
        {
            m_it = m_it_c;
            return curr();
        }
        int32_t next()
        {
            ++m_it;
            return curr();
        }
        int32_t curr()
        {
            if (m_it != m_end)
                return *m_it;
            else
                return -1;
        }
        int32_t find(int32_t docid)
        {
            m_it.find(docid);
            return curr();
        }
        
        uint32_t cost() const
        {
            return m_it.size();
        }

        InvertStrategy::info_t *get_strategy_data(InvertStrategy &/* st */)
        {
            return NULL;
        }
    private:
        Iterator m_it;
        Iterator m_it_c;
        Iterator m_end;
};

#endif
