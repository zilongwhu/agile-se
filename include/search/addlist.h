#ifndef __AGILE_SE_ADDLIST_H__
#define __AGILE_SE_ADDLIST_H__

#include <string.h>
#include "search/doclist.h"

template<typename SkipList>
class AddList: public DocList
{
    public:
        typedef typename SkipList::iterator Iterator;

        AddList(uint32_t sign, Iterator it)
            : m_it(it), m_it_c(it), m_end(0, it.list())
        {
            m_sign = sign;
        }

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

        InvertStrategy::info_t *get_strategy_data(InvertStrategy &st)
        {
            if (m_it != m_end)
            {
                m_strategy_data.data = m_data;
                m_strategy_data.sign = m_sign;
                m_strategy_data.type = m_it.type();
                m_strategy_data.length = m_it.payload_len();
                if (m_it.payload_len() > 0)
                {
                    ::memcpy(m_strategy_data.result, m_it.payload(), m_it.payload_len());
                }
                st.work(&m_strategy_data);
                return &m_strategy_data;
            }
            return NULL;
        }
    private:
        uint32_t m_sign;
        Iterator m_it;
        Iterator m_it_c;
        Iterator m_end;
};

#endif
