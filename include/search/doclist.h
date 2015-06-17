#ifndef __AGILE_SE_DOCLIST_H__
#define __AGILE_SE_DOCLIST_H__

#include "search/invert_strategy.h"

class DocList
{
    public:
        DocList()
        {
            m_data.i64 = 0; /* 初始化data，默认为0。 */
        }
        virtual ~DocList() { }

        virtual void set_data(InvertStrategy::data_t data)
        {
            this->m_data = data;
        }

        /* 调用此函数开始迭代链表，获得第一个元素 */
        virtual int32_t first() = 0;
        /* 迭代获得下一个元素 */
        virtual int32_t next() = 0;
        /* 获取当前迭代元素 */
        virtual int32_t curr() = 0;
        /* 从当前迭代位置开始，find第一个大于等于docid的元素并更新curr值 */
        virtual int32_t find(int32_t docid) = 0;
        /* 获取操作当前拉链的开销，值越大开销越大 */
        virtual uint32_t cost() const = 0;
        /* 获得策略数据 */
        virtual InvertStrategy::info_t *
            get_strategy_data(InvertStrategy &st) = 0;
    protected:
        InvertStrategy::info_t m_strategy_data;
        InvertStrategy::data_t m_data;
    private:
        /* 禁止copy&assign */
        DocList(const DocList &);
        DocList &operator = (const DocList &);
};

#endif
