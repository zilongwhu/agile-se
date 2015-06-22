#include "log_utils.h"
#include "index/const_index.h"

int ConstIndex::init(const char *path, const char *file)
{
    const int ret = m_idx.init(path, file);
    if (ret < 0)
    {
        P_WARNING("failed to load const index");
        return ret;
    }
    if (!m_idx.is_base_mode())
    {
        P_FATAL("const index must in base mode");
        ::usleep(10*1000); /* 10ms */
        ::exit(-1);
    }
    P_WARNING("waiting const index loaded");
    m_idx.join(); /* wait index building */
    P_WARNING("const index loaded ok");
    return ret;
}
