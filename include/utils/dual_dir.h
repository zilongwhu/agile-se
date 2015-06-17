#ifndef __AGILE_SE_DUAL_DIR_H__
#define __AGILE_SE_DUAL_DIR_H__

#include	<string>

bool is_dir(const std::string &path);
bool mk_dir(const std::string &path);
void clear_dir(const std::string &path);

class DualDir
{
    public:
        /* 初始化，传入路径 */
        int init(const std::string &path);
        /* 获取当前使用目录路径 */
        std::string using_path();
        /* 获取当前未使用目录路径 */
        std::string writeable_path(bool clear = true);
        /* 0,1目录切换 */
        int switch_using();
    private:
        std::string m_path;
        int m_curr;
};

#endif
