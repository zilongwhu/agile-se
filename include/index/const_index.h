#ifndef __AGILE_SE_CONST_INDEX_H__
#define __AGILE_SE_CONST_INDEX_H__

#include "index/index.h"

class ConstIndex
{
    public:
        ConstIndex() { }
        ~ConstIndex() { }

        int init(const char *path, const char *file); /* 初始化函数，调用一次 */
    public:
        const LevelIndex *get_level_index(size_t level) const
        {
            return m_idx.get_level_index(level);
        }
    private:
        Index m_idx;
};

#endif
