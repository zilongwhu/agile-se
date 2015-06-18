#include <fstream>
#include <string>
#include "index/index.h"
#include "configure.h"
#include "str_utils.h"

std::map<std::string, thread_func_t> Index::s_inc_processors;

int Index::init(const char *path, const char *file)
{
    P_WARNING("start to init Index");

    Config conf(path, file);
    if (conf.parse() != 0)
    {
        P_WARNING("failed to load [%s][%s]", path, file);
        return -1;
    }
    m_conf.invert_path = conf["INVERT_PATH"];
    m_conf.invert_file = conf["INVERT_FILE"];
    m_conf.forward_path = conf["FORWARD_PATH"];
    m_conf.forward_file = conf["FORWARD_FILE"];
    m_conf.index_path = conf["INDEX_PATH"];
    m_conf.index_meta_file = conf["INDEX_META_FILE"];
    m_conf.dump_flag_file = conf["DUMP_FLAG_FILE"];
    m_conf.print_meta_flag_file = conf["PRINT_META_FLAG_FILE"];
    m_conf.print_list_flag_file = conf["PRINT_LIST_FLAG_FILE"];
    m_conf.print_list_file = conf["PRINT_LIST_FILE"];
    m_conf.inc_processor = conf["INC_PROCESSOR"];
    if (!parseInt32(conf["INC_DAS_WARNING_TIME"], m_conf.inc_das_warning_time))
    {
        m_conf.inc_das_warning_time = 60*60*24*3; /* 默认3天没更新则报警 */
    }
    if (!parseInt32(conf["REBUILD_INDEX"], m_conf.rebuild_index))
    {
        m_conf.rebuild_index = 0;
    }
    if (!parseInt32(conf["BASE_MODE"], m_conf.base_mode))
    {
        m_conf.base_mode = 0;
    }
    if (!parseInt32(conf["MERGE_INTERVAL"], m_conf.merge_interval))
    {
        m_conf.merge_interval = 60*30; /* 默认半小时merge一次 */
    }

    P_WARNING("Index Confs:");
    P_WARNING("    [INVERT_PATH]: %s", m_conf.invert_path.c_str());
    P_WARNING("    [INVERT_FILE]: %s", m_conf.invert_file.c_str());
    P_WARNING("    [FORWARD_PATH]: %s", m_conf.forward_path.c_str());
    P_WARNING("    [FORWARD_FILE]: %s", m_conf.forward_file.c_str());
    P_WARNING("    [INDEX_PATH]: %s", m_conf.index_path.c_str());
    P_WARNING("    [INDEX_META_FILE]: %s", m_conf.index_meta_file.c_str());
    P_WARNING("    [DUMP_FLAG_FILE]: %s", m_conf.dump_flag_file.c_str());
    P_WARNING("    [PRINT_META_FLAG_FILE]: %s", m_conf.print_meta_flag_file.c_str());
    P_WARNING("    [PRINT_LIST_FLAG_FILE]: %s", m_conf.print_list_flag_file.c_str());
    P_WARNING("    [PRINT_LIST_FILE]: %s", m_conf.print_list_file.c_str());
    P_WARNING("    [INC_PROCESSOR]: %s", m_conf.inc_processor.c_str());
    P_WARNING("    [INC_DAS_WARNING_TIME]: %d ms", m_conf.inc_das_warning_time);
    P_WARNING("    [REBUILD_INDEX]: %d", m_conf.rebuild_index);
    P_WARNING("    [BASE_MODE]: %s", m_conf.base_mode != 0 ? "True" : "False");
    P_WARNING("    [MERGE_INTERVAL]: %d s", m_conf.merge_interval);

    if (m_conf.base_mode != 0 && m_conf.rebuild_index == 0)
    {
        m_conf.rebuild_index = 1;
        P_WARNING("base mode must rebuild index, set rebuild_index = 1");
    }

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
    m_print_meta_fw.create(m_conf.print_meta_flag_file.c_str());
    m_print_list_fw.create(m_conf.print_list_flag_file.c_str());

    std::string using_path = m_dual_dir.using_path();

    if (m_inc_reader.init(using_path.c_str(), m_conf.index_meta_file.c_str()) < 0)
    {
        P_WARNING("failed to init increment reader[%s][%s]",
                using_path.c_str(), m_conf.index_meta_file.c_str());
        return -1;
    }

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

    P_WARNING("start to init forward index");
    if (m_forward.init(m_conf.forward_path.c_str(), m_conf.forward_file.c_str()) < 0)
    {
        P_WARNING("failed to init forward index");
        return -1;
    }
    P_WARNING("init forward index ok");

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

    int ret = ::pthread_create(&m_inc_tid, NULL, proc, this);
    if (ret != 0)
    {
        P_WARNING("failed to create increment_thread, ret=%d", ret);
        return -1;
    }

    P_WARNING("init Index ok");
    if (m_conf.base_mode != 0)
    {
        ::pthread_join(m_inc_tid, NULL);
        P_WARNING("build index ok in base mode, exiting");
        ::exit(0);
    }
    return 0;
}

int Index::dump()
{
    P_WARNING("start to dump Index");

    std::string path = m_dual_dir.writeable_path();
    if (m_conf.base_mode)
    {
        m_invert.mergeAll(0);
    }
    P_WARNING("start to dump invert index");
    if (m_invert.dump(path.c_str()) < 0)
    {
        P_WARNING("failed to dump invert index");
        return -1;
    }
    P_WARNING("dump invert index ok");

    P_WARNING("start to dump forward index");
    if (m_forward.dump(path.c_str()) < 0)
    {
        P_WARNING("failed to dump forward index");
        return -1;
    }
    P_WARNING("dump forward index ok");

    if (m_inc_reader.dumpMeta(path.c_str(), m_conf.index_meta_file.c_str()) < 0)
    {
        P_WARNING("failed to dump increment reader meta");
        return -1;
    }
    FILE *fp = ::fopen((path + "/" + m_conf.index_meta_file.c_str()).c_str(), "a+");
    if (fp)
    {
        ::fprintf(fp, "\n");
        ::fprintf(fp, "invert_doc_num: %d\n", int(m_invert.doc_num()));
        ::fprintf(fp, "forward_doc_num: %d\n", int(m_forward.doc_num()));
        ::fclose(fp);
    }

    if (m_dual_dir.switch_using() < 0)
    {
        P_WARNING("failed to switch using file");
    }

    P_WARNING("dump Index ok");
    return 0;
}
