#ifndef  __AGILE_SE_DISJUNCTION_H__
#define  __AGILE_SE_DISJUNCTION_H__

#include <vector>
#include <deque>
#include "search/doclist.h"

class Disjunction: public DocList
{
    private:
        struct pos_t
        {
            int32_t docid;
            int32_t offset;
        };
    public:
        Disjunction(const std::vector<DocList *> &subs)
            : m_subs(subs)
        {
            m_curr = -1;
        }
        ~Disjunction()
        {
            for (size_t i = 0, sz = m_subs.size(); i < sz; ++i)
            {
                delete m_subs[i];
            }
        }

        int32_t first()
        {
            const size_t sz = m_subs.size();
            if (sz == 0) { return (m_curr = -1); }

            m_heap.clear();
            /* 调用first */
            for (size_t i = 0; i < sz; ++i)
            {
                int32_t docid = m_subs[i]->first();
                if (-1 != docid)
                {
                    pos_t pos = { docid, int(i) };
                    m_heap.push_back(pos);
                }
            }
            if (m_heap.size() == 0) { return (m_curr = -1); }
            heapfy();
            return (m_curr = m_heap[0].docid);
        }

        int32_t next()
        {
            if (-1 == m_curr) { return -1; }

            while (1)
            {
                m_heap[0].docid = m_subs[m_heap[0].offset]->next(); /* 往前走 */
                if (-1 == m_heap[0].docid) /* 走完了 */
                {
                    /* 删除该链 */
                    size_t sz = m_heap.size();
                    if (1 == sz) /* 所有链都已走完 */
                    {
                        break;
                    }
                    m_heap[0] = m_heap[sz - 1];
                    m_heap.resize(sz - 1);
                }
                heap_adjust(0);
                if (m_heap[0].docid != m_curr)
                {
                    return (m_curr = m_heap[0].docid);
                }
                /* 当前多条拉链命中，循环调整往前走 */
            }
            return (m_curr = -1);
        }

        int32_t find(int32_t docid)
        {
            if (-1 == m_curr) { return -1; }
            if (m_curr >= docid) { return m_curr; }

            while (1)
            {
                m_heap[0].docid = m_subs[m_heap[0].offset]->find(docid); /* 往前查找 */
                if (-1 == m_heap[0].docid) /* 走完了 */
                {
                    /* 删除该链 */
                    size_t sz = m_heap.size();
                    if (1 == sz) /* 所有链都已走完 */
                    {
                        break;
                    }
                    m_heap[0] = m_heap[sz - 1];
                    m_heap.resize(sz - 1);
                }
                heap_adjust(0);
                if (m_heap[0].docid != m_curr)
                {
                    return (m_curr = m_heap[0].docid);
                }
                /* 当前多条拉链命中，循环调整往前走 */
            }
            return (m_curr = -1);
        }

        int32_t curr()
        {
            return m_curr;
        }

        uint32_t cost() const
        {
            uint32_t sum = 0;
            for (size_t i = 0; i < m_subs.size(); ++i)
            {
                sum += m_subs[i]->cost();
            }
            return sum;
        }

        InvertStrategy::info_t *get_strategy_data(InvertStrategy &st)
        {
            const int sz = m_heap.size();
            std::vector<const InvertStrategy::info_t *> infos;

            std::deque<int> matched_pos;
            matched_pos.push_back(0);
            while (matched_pos.size() > 0)
            {
                int pos = matched_pos.front();
                matched_pos.pop_front();

                infos.push_back(m_subs[m_heap[pos].offset]->get_strategy_data(st));

                int lchild = ((pos << 1) + 1);
                if (lchild < sz)
                {
                    if (m_heap[lchild].docid == m_curr)
                    {
                        matched_pos.push_back(lchild);
                    }
                    int rchild = lchild + 1;
                    if (rchild < sz)
                    {
                        if (m_heap[rchild].docid == m_curr)
                        {
                            matched_pos.push_back(rchild);
                        }
                    }
                }
            }
            st.or_work(infos, &m_strategy_data);
            return &m_strategy_data;
        }
    private:
        inline void heapfy()
        {
            for (int i = (int(m_heap.size() - 2) >> 1);
                    i >= 0; --i)
            {
                heap_adjust(i);
            }
        }

        inline void heap_adjust(int pos) /* 下沉调整 */
        { /* 最小堆 */
            const int sz = m_heap.size();
            int last = ((sz - 2) >> 1);

            /* 备份 */
            const int old_pos = pos;
            const pos_t cur = m_heap[pos];

            while (pos <= last)
            {
                int lchild = ((pos) << 1) + 1;
                int rchild = lchild + 1;
                if (rchild < sz)
                {
                    int32_t ldoc = m_heap[lchild].docid;
                    int32_t rdoc = m_heap[rchild].docid;
                    if (ldoc < rdoc)
                    {
                        if (cur.docid < ldoc)
                        {
                            break;
                        }
                        else
                        {
                            m_heap[pos] = m_heap[lchild];
                            pos = lchild;
                        }
                    }
                    else
                    {
                        if (cur.docid < rdoc)
                        {
                            break;
                        }
                        else
                        {
                            m_heap[pos] = m_heap[rchild];
                            pos = rchild;
                        }
                    }
                }
                else
                {
                    int32_t ldoc = m_heap[lchild].docid;
                    if (cur.docid < ldoc)
                    {
                        break;
                    }
                    else
                    {
                        m_heap[pos] = m_heap[lchild];
                        pos = lchild;
                    }
                }
            }
            if (old_pos != pos)
            {
                m_heap[pos] = cur;
            }
        }
    private:
        int32_t m_curr;
        std::vector<DocList *> m_subs;
        std::vector<pos_t> m_heap;
};

#endif
