// =====================================================================================
//
//       Filename:  dual_dir.cpp
//
//    Description:  
//
//        Version:  1.0
//        Created:  05/28/2014 02:08:23 PM
//       Revision:  none
//       Compiler:  g++
//
//         Author:  Zilong.Zhu (), zilong.whu@gmail.com
//        Company:  edu.whu
//
// =====================================================================================

#include    <stdlib.h>
#include	<sys/types.h>
#include	<sys/stat.h>
#include	<unistd.h>
#include	<errno.h>
#include	<fstream>
#include	"log.h"
#include	"dual_dir.h"

bool is_dir(const std::string &path)
{
    struct stat st;
    int ret = ::stat(path.c_str(), &st);
    if (ret != 0)
    {
        WARNING("failed to get stat of file[%s], errno=%d", path.c_str(), errno);
        return false;
    }
    return S_ISDIR(st.st_mode);
}

bool mk_dir(const std::string &path)
{
    struct stat st;
    int ret = ::stat(path.c_str(), &st);
    if (ret == 0)
    {
        if (S_ISDIR(st.st_mode))
        {
            return true;
        }
        WARNING("file[%s] exist, but not a dir", path.c_str());
        return false;
    }
    ret = ::mkdir(path.c_str(), 0755);
    if (ret != 0)
    {
        WARNING("failed to mkdir[%s], errno=%d", path.c_str(), errno);
        return false;
    }
    WARNING("mkdir[%s] ok", path.c_str());
    return true;
}

void clear_dir(const std::string &path)
{
    std::string stm("cd " + path + "&&rm -rf *");
    WARNING("exec: %s", stm.c_str());
    system(stm.c_str());
}

int DualDir::init(const std::string &path)
{
    if (!is_dir(path))
    {
        WARNING("file[%s] is not a dir", path.c_str());
        return -1;
    }
    m_path = path;
    return mk_dir(path + "/0") && mk_dir(path + "/1") ? 0 : -1;
}

std::string DualDir::using_path()
{
    m_curr = 0;
    {
        std::string p(m_path + "/using");
        std::ifstream fin(p.c_str());
        if (!(fin >> m_curr))
        {
            m_curr = 0;
        }
        fin.close();
    }
    return m_path + (m_curr == 0 ? "/0" : "/1");
}

std::string DualDir::writeable_path(bool clear)
{
    this->using_path();
    std::string p(m_path + (m_curr == 0 ? "/1" : "/0"));
    if (clear)
    {
        clear_dir(p);
    }
    return p;
}

int DualDir::switch_using()
{
    std::string p(m_path + "/using");
    std::ofstream fout(p.c_str());
    fout << (m_curr == 0 ? 1 : 0) << std::endl;
    fout.close();
    return fout ? 0 : -1;
}
