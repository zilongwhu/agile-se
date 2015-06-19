#include "index/const_index.h"
#include "inc/inc_builder.h"
#include "str_utils.h"
#include "configure.h"

std::map<std::string, base_func_t> ConstIndex::s_base_processors;

int ConstIndex::init(const char *path, const char *file)
{
    P_WARNING("start to init ConstIndex");
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
    m_conf.base_processor = conf["BASE_PROCESSOR"];

    P_WARNING("ConstIndex Confs:");
    P_WARNING("    [INVERT_PATH]: %s", m_conf.invert_path.c_str());
    P_WARNING("    [INVERT_FILE]: %s", m_conf.invert_file.c_str());
    P_WARNING("    [FORWARD_PATH]: %s", m_conf.forward_path.c_str());
    P_WARNING("    [FORWARD_FILE]: %s", m_conf.forward_file.c_str());
    P_WARNING("    [BASE_PROCESSOR]: %s", m_conf.base_processor.c_str());

    P_WARNING("start to init invert index");
    if (m_invert.init(m_conf.invert_path.c_str(), m_conf.invert_file.c_str()) < 0)
    {
        P_WARNING("failed to init invert index");
        return -1;
    }
    P_WARNING("init invert index ok");

    P_WARNING("start to init forward index");
    if (m_forward.init(m_conf.forward_path.c_str(), m_conf.forward_file.c_str()) < 0)
    {
        P_WARNING("failed to init forward index");
        return -1;
    }
    P_WARNING("init forward index ok");

    return 0;
}

bool ConstIndex::load(const char *dir)
{
    return m_forward.load(dir) && m_invert.load(dir);
}

bool ConstIndex::dump(const char *dir)
{
    return m_forward.dump(dir) && m_invert.dump(dir);
}

bool ConstIndex::build(const char *path, const char *file)
{
    base_func_t proc = NULL;
    {
        std::map<std::string, base_func_t>::const_iterator it =
            s_base_processors.find(std::string(m_conf.base_processor.c_str()));
        if (it == s_base_processors.end())
        {
            P_WARNING("unregistered base processor[%s]", m_conf.base_processor.c_str());
            return false;
        }
        proc = it->second;
        if (proc == NULL)
        {
            P_WARNING("invalid registered base processor[%s]", m_conf.base_processor.c_str());
            return false;
        }
    }
    base_build_t *build_args = new (std::nothrow) base_build_t;
    if (NULL == build_args)
    {
        P_WARNING("failed to new base_build_t");
        return false;
    }
    build_args->invert = &m_invert;
    build_args->forward = &m_forward;
    if (build_args->reader.init(path, file) < 0)
    {
        P_WARNING("failed to open base file for read");
        delete build_args;
        return false;
    }
    P_WARNING("start to load base file");
    if (!(*proc)(*build_args))
    {
        P_WARNING("failed to load base file");
        delete build_args;
        return false;
    }
    P_WARNING("load base file ok");
    delete build_args;
    return true;
}

bool default_base_builder(base_build_t &args)
{
    using namespace inc;

    IncReader &reader = args.reader;
    char *line = reader._line_buf;

    const size_t log_buffer_length = 4096;
    char *log_buffer = new char[log_buffer_length];

    bool ret = true;
    uint32_t total = 0;
    while (1)
    {
        const int n = reader.next();
        if (n <= 0)
        {
            if (0 == n)
            {
                P_WARNING("reach EOF, total lines=%u", total);
                break;
            }
            P_WARNING("reader returns -1");
            ret = false;
            break;
        }
        ++total;
        std::vector<std::string> columns;
        split(line, "\t", columns);
        if (columns.size() < 2)
        {
            P_MYLOG("must has [level, oid]");
            continue;
        }
        const int32_t level = ::strtol(columns[0].c_str(), NULL, 10);
        const int32_t oid = ::strtoul(columns[1].c_str(), NULL, 10);
        cJSON *forward_json = NULL;
        cJSON *invert_json = NULL;
        std::vector<forward_data_t> fields;
        std::vector<invert_data_t> inverts;
        switch (level)
        {
            case INVERT_LEVEL:
                if (columns.size() < 4)
                {
                    P_MYLOG("invert update need at least 4 fields");
                }
                else if (NULL == (forward_json = parse_forward_json(columns[2], fields)))
                {
                    P_MYLOG("failed to parse forward json");
                }
                else if (NULL == (invert_json = parse_invert_json(columns[3], inverts)))
                {
                    P_MYLOG("failed to parse invert json");
                }
                else if (!args.forward->update(oid, fields))
                {
                    P_MYLOG("failed to update forward index");
                }
                else
                {
                    args.invert->remove(oid);
                    for (size_t i = 0; i < inverts.size(); ++i)
                    {
                        args.invert->insert(inverts[i].key, inverts[i].type, oid, inverts[i].value);
                    }
                    P_TRACE("update ok, level=%d, oid=%d", level, oid);
                }
                break;
        }
        if (forward_json)
        {
            cJSON_Delete(forward_json);
        }
        if (invert_json)
        {
            cJSON_Delete(invert_json);
        }
    }
    delete [] log_buffer;
    return ret;
}
