#include <new>
#include <fstream>
#include <string>
#include <pthread.h>
#include "index/index.h"
#include "configure.h"
#include "str_utils.h"

std::map<std::string, thread_func_t> Index::s_inc_processors;

Index::~Index()
{
    for (size_t i = 0; i < m_index.size(); ++i)
    {
        if (m_index[i])
        {
            delete m_index[i];
        }
    }
    m_index.clear();
}

int Index::init(const char *path, const char *file)
{
    P_WARNING("start to init Index");

    Config conf(path, file);
    if (conf.parse() < 0)
    {
        P_WARNING("failed to load [%s][%s]", path, file);
        return -1;
    }
    m_conf.index_path = conf["INDEX_PATH"];
    m_conf.index_meta_file = conf["INDEX_META_FILE"];
    m_conf.dump_flag_file = conf["DUMP_FLAG_FILE"];
    m_conf.inc_processor = conf["INC_PROCESSOR"];
    if (!parseInt32(conf["INC_DAS_WARNING_TIME"], m_conf.inc_das_warning_time))
    {
        m_conf.inc_das_warning_time = 60*60*24*3; /* 默认3天没更新则报警 */
    }

    P_WARNING("Index Confs:");
    P_WARNING("    [INDEX_PATH]: %s", m_conf.index_path.c_str());
    P_WARNING("    [INDEX_META_FILE]: %s", m_conf.index_meta_file.c_str());
    P_WARNING("    [DUMP_FLAG_FILE]: %s", m_conf.dump_flag_file.c_str());
    P_WARNING("    [INC_PROCESSOR]: %s", m_conf.inc_processor.c_str());
    P_WARNING("    [INC_DAS_WARNING_TIME]: %d ms", m_conf.inc_das_warning_time);

    thread_func_t proc = NULL;
    {
        std::map<std::string, thread_func_t>::const_iterator it =
            s_inc_processors.find(std::string(m_conf.inc_processor.c_str()));
        if (it == s_inc_processors.end())
        {
            P_WARNING("unregistered inc processor[%s]", m_conf.inc_processor.c_str());
            return -1;
        }
        proc = it->second;
        if (proc == NULL)
        {
            P_WARNING("invalid registered inc processor[%s]", m_conf.inc_processor.c_str());
            return -1;
        }
    }

    if (m_dual_dir.init(m_conf.index_path.c_str()) < 0)
    {
        P_WARNING("failed to init index path");
        return -1;
    }
    m_dump_fw.create(m_conf.dump_flag_file.c_str());

    std::string using_path = m_dual_dir.using_path();

    if (m_inc_reader.init(using_path.c_str(), m_conf.index_meta_file.c_str()) < 0)
    {
        P_WARNING("failed to init increment reader[%s][%s]",
                using_path.c_str(), m_conf.index_meta_file.c_str());
        return -1;
    }

    uint32_t level_num = 0;
    if (parseUInt32(conf["level_num"], level_num))
    {
        for (uint32_t i = 0; i < level_num; ++i)
        {
            char tmpbuf[256];
            ::snprintf(tmpbuf, sizeof tmpbuf, "level_%d", i);
            uint32_t level = 0;
            std::string conf_path;
            std::string conf_file;
            std::string level_name;
            if (!parseUInt32(conf[tmpbuf], level))
            {
                P_WARNING("failed to parse %s", tmpbuf);
                goto FAIL;
            }
            if (level >= m_index.size())
            {
                m_index.resize(level + 1, NULL);
            }
            ::snprintf(tmpbuf, sizeof tmpbuf, "level_%d_conf_path", i);
            conf_path = conf[tmpbuf];
            ::snprintf(tmpbuf, sizeof tmpbuf, "level_%d_conf_file", i);
            conf_file = conf[tmpbuf];
            ::snprintf(tmpbuf, sizeof tmpbuf, "level_%d_name", i);
            level_name = conf[tmpbuf];
            m_index[level] = new (std::nothrow) LevelIndex;
            if (NULL == m_index[level] || m_index[level]->
                    init(conf_path.c_str(), conf_file.c_str()) < 0)
            {
                goto FAIL;
            }
            ::snprintf(tmpbuf, sizeof tmpbuf, "L%u_%s", level, level_name.c_str());
            m_level2dirname[level] = tmpbuf;
        }
        if (0)
        {
FAIL:
            for (size_t i = 0; i < m_index.size(); ++i)
            {
                if (m_index[i])
                {
                    delete m_index[i];
                }
            }
            m_index.clear();
            return -1;
        }

        std::map<size_t, std::string>::const_iterator it = m_level2dirname.begin();
        while (it != m_level2dirname.end())
        {
            P_WARNING("level[%d]'s dirname: %s", int(it->first), it->second.c_str());
            ++it;
        }
    }
    if (m_level2dirname.size() == 0)
    {
        P_WARNING("must have at least one level");
        return -1;
    }
    P_WARNING("level num: %u", level_num);

    int ret = ::pthread_create(&m_inc_tid, NULL, proc, this);
    if (ret != 0)
    {
        P_WARNING("failed to create increment_thread, ret=%d", ret);
        return -1;
    }

    P_WARNING("init Index ok");
    return 0;
}

void Index::recycle()
{
    for (size_t i = 0; i < m_index.size(); ++i)
    {
        if (m_index[i])
        {
            m_index[i]->recycle();
        }
    }
}

int Index::dump(const char *path)
{
    P_WARNING("start to dump Index");

    std::string path2;
    if (NULL == path) {
        path2 = m_dual_dir.writeable_path();
    } else {
        path2 = path;
    }
    for (size_t i = 0; i < m_index.size(); ++i)
    {
        if (m_index[i])
        {
            std::string dir = path2 + "/" + m_level2dirname[i];
            if (mk_dir(dir))
            {
                if (this->is_base_mode())
                {
                    m_index[i]->try2merge(true);
                }
                P_WARNING("start to dump at: %s", dir.c_str());
                m_index[i]->dump(dir.c_str());
                P_WARNING("dump ok at: %s", dir.c_str());
            }
        }
    }

    if (m_inc_reader.dumpMeta(path2.c_str(), m_conf.index_meta_file.c_str()) < 0)
    {
        P_WARNING("failed to dump increment reader meta");
        return -1;
    }
    FILE *fp = ::fopen((path2 + "/" + m_conf.index_meta_file.c_str()).c_str(), "a+");
    if (fp)
    {
        ::fprintf(fp, "\n");
        for (size_t i = 0; i < m_index.size(); ++i)
        {
            if (m_index[i])
            {
                ::fprintf(fp, "doc num of %s: %d\n",
                        m_level2dirname[i].c_str(), int(m_index[i]->doc_num()));
            }
        }
        ::fclose(fp);
    }

    if (NULL == path && m_dual_dir.switch_using() < 0)
    {
        P_WARNING("failed to switch using file");
    }

    P_WARNING("dump Index ok");
    return 0;
}

void Index::print_meta() const
{
    for (size_t i = 0; i < m_index.size(); ++i)
    {
        if (m_index[i])
        {
            m_index[i]->print_meta();
        }
    }
}

void Index::print_list() const
{
    for (size_t i = 0; i < m_index.size(); ++i)
    {
        if (m_index[i])
        {
            m_index[i]->print_list();
        }
    }
}

void Index::exc_cmd() const
{
    for (size_t i = 0; i < m_index.size(); ++i)
    {
        if (m_index[i])
        {
            m_index[i]->exc_cmd();
        }
    }
}

bool Index::try2dump()
{
    if (m_dump_fw.check_and_update_timestamp() > 0)
    {
        this->dump();
        return true;
    }
    return false;
}

void Index::try_print_meta()
{
    for (size_t i = 0; i < m_index.size(); ++i)
    {
        if (m_index[i])
        {
            m_index[i]->try_print_meta();
        }
    }
}

void Index::try_print_list()
{
    for (size_t i = 0; i < m_index.size(); ++i)
    {
        if (m_index[i])
        {
            m_index[i]->try_print_list();
        }
    }
}

void Index::try_exc_cmd()
{
    for (size_t i = 0; i < m_index.size(); ++i)
    {
        if (m_index[i])
        {
            m_index[i]->try_exc_cmd();
        }
    }
}

void Index::try2merge()
{
    for (size_t i = 0; i < m_index.size(); ++i)
    {
        if (m_index[i])
        {
            m_index[i]->try2merge();
        }
    }
}
