#include <string.h>
#include <stdlib.h>
#include "search/biglist.h"
#include "search/arraylist.h"
#include "utils/log.h"

ArrayList::ArrayList(int8_t type, const std::vector<int32_t> &docids)
{
    m_raw = NULL;
    m_impl = NULL;
    if (docids.size() > 0)
    {
        bl_head_t head;
        ::bzero(&head, sizeof head);
        head.type = type;
        head.doc_num = docids.size();
        m_raw = ::malloc(sizeof(head) + sizeof(int32_t) * head.doc_num);
        if (NULL == m_raw)
        {
            P_WARNING("failed to alloc mem, doc_num=%d", head.doc_num);
            return ;
        }
        ::memcpy(m_raw, &head, sizeof head);
        ::memcpy(((char *)m_raw) + sizeof head, &docids[0], sizeof(int32_t) * head.doc_num);
        m_impl = new(std::nothrow) BigList(0, m_raw);
        if (NULL == m_impl)
        {
            ::free(m_raw);
            m_raw = NULL;
        }
    }
}

ArrayList::~ArrayList()
{
    if (m_raw)
    {
        ::free(m_raw);
        m_raw = NULL;
    }
    if (m_impl)
    {
        delete m_impl;
        m_impl = NULL;
    }
}
