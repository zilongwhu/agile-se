#ifndef __AGILE_SE_COW_BTREE_LIST_H__
#define __AGILE_SE_COW_BTREE_LIST_H__

#include "search/doclist.h"
#include "index/cow_btree.h"

template<typename CowBtree>
class CowBtreeList: public DocList
{
    public:
        typedef typename CowBtree::iterator iterator;
    public:
        CowBtreeList(uint32_t sign, iterator it)
            : m_sign(sign), m_it(it)
        { }

        int32_t first()
        {
            m_it.first();
            return *m_it;
        }
        int32_t next()
        {
            ++m_it;
            return *m_it;
        }
        int32_t curr()
        {
            return *m_it;
        }
        int32_t find(int32_t docid)
        {
            m_it.find(docid);
            return *m_it;
        }
        uint32_t cost() const
        {
            return m_it.size();
        }

        InvertStrategy::info_t *get_strategy_data(InvertStrategy &st)
        {
            if (m_it)
            {
                m_strategy_data.data = m_data;
                m_strategy_data.sign = m_sign;
                m_strategy_data.type = m_it.type();
                m_strategy_data.length = m_it.payload_len();
                if (m_strategy_data.length > 0)
                {
                    ::memcpy(m_strategy_data.result, m_it.payload(), m_strategy_data.length);
                }
                st.work(&m_strategy_data);
                return &m_strategy_data;
            }
            return NULL;
        }
    private:
        uint32_t m_sign;
        iterator m_it;
};

#endif
