#include <fstream>
#include <string>
#include "configure.h"
#include "str_utils.h"
#include "index/level_index.h"

int LevelIndex::init(const char *path, const char *file)
{
    P_WARNING("start to init Index");

    Config conf(path, file);
    if (conf.parse() < 0)
    {
        P_WARNING("failed to load [%s][%s]", path, file);
        return -1;
    }
    m_conf.index_name = conf["INDEX_NAME"];
    const bool has_index_path = conf.get("INDEX_PATH", m_conf.index_path);
    m_conf.forward_path = conf["FORWARD_PATH"];
    m_conf.forward_file = conf["FORWARD_FILE"];

    m_conf.dump_flag_file = conf["DUMP_FLAG_FILE"];
    m_conf.print_meta_flag_file = conf["PRINT_META_FLAG_FILE"];

    if (!parseInt32(conf["REBUILD_INDEX"], m_conf.rebuild_index))
    {
        m_conf.rebuild_index = 0;
    }
    if (!parseInt32(conf["MERGE_INTERVAL"], m_conf.merge_interval))
    {
        m_conf.merge_interval = 60*30; /* 默认半小时merge一次 */
    }

    m_has_invert = false;
    if (conf.get("INVERT_PATH", m_conf.invert_path))
    {
        m_conf.invert_file = conf["INVERT_FILE"];
        m_conf.print_list_flag_file = conf["PRINT_LIST_FLAG_FILE"];
        m_conf.print_list_file = conf["PRINT_LIST_FILE"];
        m_has_invert = true;
    }

    P_WARNING("Index Confs:");
    P_WARNING("    [INDEX_NAME]: %s", m_conf.index_name.c_str());
    P_WARNING("    [INDEX_PATH]: %s", m_conf.index_path.c_str());
    P_WARNING("    [FORWARD_PATH]: %s", m_conf.forward_path.c_str());
    P_WARNING("    [FORWARD_FILE]: %s", m_conf.forward_file.c_str());
    if (m_has_invert)
    {
        P_WARNING("    [INVERT_PATH]: %s", m_conf.invert_path.c_str());
        P_WARNING("    [INVERT_FILE]: %s", m_conf.invert_file.c_str());
    }
    P_WARNING("    [DUMP_FLAG_FILE]: %s", m_conf.dump_flag_file.c_str());
    P_WARNING("    [PRINT_META_FLAG_FILE]: %s", m_conf.print_meta_flag_file.c_str());
    if (m_has_invert)
    {
        P_WARNING("    [PRINT_LIST_FLAG_FILE]: %s", m_conf.print_list_flag_file.c_str());
        P_WARNING("    [PRINT_LIST_FILE]: %s", m_conf.print_list_file.c_str());
    }
    P_WARNING("    [REBUILD_INDEX]: %d", m_conf.rebuild_index);
    P_WARNING("    [MERGE_INTERVAL]: %d s", m_conf.merge_interval);

    if (has_index_path && m_dual_dir.init(m_conf.index_path.c_str()) < 0)
    {
        P_WARNING("failed to init index path");
        return -1;
    }
    m_dump_fw.create(m_conf.dump_flag_file.c_str());
    m_print_meta_fw.create(m_conf.print_meta_flag_file.c_str());
    if (m_has_invert)
    {
        m_print_list_fw.create(m_conf.print_list_flag_file.c_str());
    }

    std::string using_path = m_dual_dir.using_path();
    if (m_has_invert)
    {
        P_WARNING("start to init invert index");
        if (m_invert.init(m_conf.invert_path.c_str(), m_conf.invert_file.c_str()) < 0)
        {
            P_WARNING("failed to init invert index");
            return -1;
        }
        P_WARNING("init invert index ok");

        if (!m_conf.rebuild_index)
        {
            P_WARNING("start to load invert index");
            if (!m_invert.load(using_path.c_str()))
            {
                P_WARNING("failed to load invert index");
                return -1;
            }
            P_WARNING("load invert index ok");
        }
    }
    P_WARNING("start to init forward index");
    if (m_forward.init(m_conf.forward_path.c_str(), m_conf.forward_file.c_str()) < 0)
    {
        P_WARNING("failed to init forward index");
        return -1;
    }
    P_WARNING("init forward index ok");
    if (!m_has_invert && m_forward.has_id_mapper())
    {
        P_WARNING("LevelIndex without InvertIndex must not have IDMapper in ForwardIndex");
        return -1;
    }

    if (!m_conf.rebuild_index)
    {
        P_WARNING("start to load forward index");
        if (!m_forward.load(using_path.c_str()))
        {
            P_WARNING("failed to load forward index");
            return -1;
        }
        P_WARNING("load forward index ok");
    }

    P_WARNING("init Index ok");
    return 0;
}

int LevelIndex::dump(const char *path)
{
    P_WARNING("start to dump Index: %s", m_conf.index_name.c_str());

    std::string path2;
    if (NULL == path) {
        path2 = m_dual_dir.writeable_path();
    } else {
        path2 = path;
    }
    if (m_has_invert)
    {
        P_WARNING("start to dump invert index");
        if (m_invert.dump(path2.c_str()) < 0)
        {
            P_WARNING("failed to dump invert index");
            return -1;
        }
        P_WARNING("dump invert index ok");
    }

    P_WARNING("start to dump forward index");
    if (m_forward.dump(path2.c_str()) < 0)
    {
        P_WARNING("failed to dump forward index");
        return -1;
    }
    P_WARNING("dump forward index ok");

    if (NULL == path && m_dual_dir.switch_using() < 0)
    {
        P_WARNING("failed to switch using file");
    }

    P_WARNING("dump Index ok: %s", m_conf.index_name.c_str());
    return 0;
}
