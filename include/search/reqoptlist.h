#ifndef __AGILE_SE_REQUIRE_OPTIONAL_LIST_H__
#define __AGILE_SE_REQUIRE_OPTIONAL_LIST_H__

#include "search/doclist.h"

class ReqOptList: public DocList
{
    public:
        ReqOptList(DocList *req, DocList *opt)
        {
            m_req = req;
            m_opt = opt;
        }
        ~ReqOptList()
        {
            if (m_req)
            {
                delete m_req;
                m_req = NULL;
            }
            if (m_opt)
            {
                delete m_opt;
                m_opt = NULL;
            }
        }
        int32_t first()
        {
            m_opt->first();
            return m_req->first();
        }
        int32_t next()
        {
            return m_req->next();
        }
        int32_t find(int32_t docid)
        {
            return m_req->find(docid);
        }
        int32_t curr()
        {
            return m_req->curr();
        }
        uint32_t cost() const
        {
            return m_req->cost();
        }
        InvertStrategy::info_t *get_strategy_data(InvertStrategy &st)
        {
            int32_t cur = m_req->curr();
            if (-1 == cur) { return NULL; }
            if (m_opt->find(cur) == cur)
            {
                std::vector<const InvertStrategy::info_t *> infos;
                infos.push_back(m_req->get_strategy_data(st));
                infos.push_back(m_opt->get_strategy_data(st));
                st.and_work(infos, &m_strategy_data);
                return &m_strategy_data;
            }
            return m_req->get_strategy_data(st);
        }
    private:
        DocList *m_req;
        DocList *m_opt;
};

#endif
