// =====================================================================================
//
//       Filename:  forward_index.cpp
//
//    Description:  
//
//        Version:  1.0
//        Created:  03/05/2014 05:15:20 PM
//       Revision:  none
//       Compiler:  g++
//
//         Author:  Zilong.Zhu (), zilong.whu@gmail.com
//        Company:  edu.whu
//
// =====================================================================================

#include "log.h"
#include "configure.h"
#include "forward_index.h"

std::map<std::string, FieldParser_creater> g_field_parsers;

struct FieldConfig
{
    std::string name;
    std::string parser;
    int type;
    int offset;
    int size;
};

void ForwardIndex::cleanup(Hash::node_t *node, intptr_t arg)
{
    ForwardIndex *ptr = (ForwardIndex *)arg;
    if (NULL == ptr || ptr->m_delayed_list.size() == 0)
    {
        FATAL("should not run to here");
        ::abort();
    }
    cleanup_data_t &cd = ptr->m_delayed_list.front();
    if (node->value != cd.addr)
    {
        FATAL("should not run to here");
        ::abort();
    }
    cd.clean();
    ptr->m_pool.free(cd.addr, ptr->m_info_size);
    ptr->m_delayed_list.pop_front();
}

ForwardIndex::~ForwardIndex()
{
    if (m_dict)
    {
        delete m_dict;
        m_dict = NULL;
    }
    __gnu_cxx::hash_map<std::string, FieldDes>::iterator it = m_fields.begin();
    while (it != m_fields.end())
    {
        if (it->second.parser)
        {
            delete it->second.parser;
        }
        ++it;
    }
    m_fields.clear();
}

int ForwardIndex::init(const char *path, const char *file)
{
    Config config(path, file);
    if (config.parse() < 0)
    {
        WARNING("failed parse config[%s:%s]", path, file);
        return -1;
    }
    int field_num = 0;
    if (!config.get("field_num", field_num) || field_num <= 0)
    {
        WARNING("failed to get field_num");
        return -1;
    }
    int max_size = 0;
    std::vector<FieldConfig> fields;
    for (int i = 0; i < field_num; ++i)
    {
        FieldConfig field;
        char buffer[256];

        ::snprintf(buffer, sizeof(buffer), "field_%d_name", i);
        if (!config.get(buffer, field.name))
        {
            WARNING("failed to get %s", buffer);
            return -1;
        }
        ::snprintf(buffer, sizeof(buffer), "field_%d_type", i);
        std::string type;
        if (!config.get(buffer, type))
        {
            WARNING("failed to get %s", buffer);
            return -1;
        }
        if ("int" == type)
        {
            field.type = INT_TYPE;
            field.size = sizeof(int);
        }
        else if ("float" == type)
        {
            field.type = FLOAT_TYPE;
            field.size = sizeof(float);
        }
        else if ("self_define" == type)
        {
            field.type = SELF_DEFINE_TYPE;
            field.size = sizeof(void *);
        }
        else
        {
            WARNING("invalid field_type of [%s] = %s", buffer, type.c_str());
            return -1;
        }
        if (SELF_DEFINE_TYPE == field.type)
        {
            ::snprintf(buffer, sizeof(buffer), "field_%d_parser", i);
            if (!config.get(buffer, field.parser))
            {
                WARNING("failed to get %s", buffer);
                return -1;
            }
        }
        ::snprintf(buffer, sizeof(buffer), "field_%d_offset", i);
        if (!config.get(buffer, field.offset))
        {
            WARNING("failed to get %s", buffer);
            return -1;
        }
        if (field.offset < 0 || field.offset % field.size != 0)
        {
            WARNING("invalid %s[%d] of field[%s]", buffer, field.offset, field.name.c_str());
            return -1;
        }
        fields.push_back(field);
        if (max_size < field.offset + field.size)
        {
            max_size = field.offset + field.size;
        }
    }
    WARNING("info_size = %d", max_size);

    std::vector<int> placeholder(max_size, -1);
    for (size_t i = 0; i < fields.size(); ++i)
    {
        for (int j = 0; j < fields[i].size; ++j)
        {
            if (placeholder[fields[i].offset + j] != -1)
            {
                WARNING("conflict configs at place[%d]", fields[i].offset + j);
                return -1;
            }
            placeholder[fields[i].offset + j] = fields[i].offset;
        }
    }
    for (size_t i = 0; i < placeholder.size(); ++i)
    {
        if (placeholder[i] != -1)
        {
            for (size_t j = 0; j < fields.size(); ++j)
            {
                if (fields[j].offset == placeholder[i])
                {
                    WARNING("field[%04d], type[%d], name[%s] ", i, fields[j].type, fields[j].name.c_str());
                    break;
                }
            }
        }
    }
    for (size_t i = 0; i < fields.size(); ++i)
    {
        FieldDes fd;

        if (m_fields.find(fields[i].name) != m_fields.end())
        {
            WARNING("duplicate field name[%s]", fields[i].name.c_str());
            goto FAIL;
        }
        fd.offset = fields[i].offset;
        fd.array_offset = fields[i].offset / fields[i].size;
        fd.type = fields[i].type;
        if (SELF_DEFINE_TYPE == fd.type)
        {
            std::map<std::string, FieldParser_creater>::iterator it = g_field_parsers.find(fields[i].parser);
            if (it == g_field_parsers.end())
            {
                WARNING("unsupported field parser[%s]", fields[i].parser.c_str());
                goto FAIL;
            }
            fd.parser = (*it->second)();
            if (NULL == fd.parser)
            {
                WARNING("failed to create field parser[%s]", fields[i].parser.c_str());
                goto FAIL;
            }
        }
        else
        {
            fd.parser = NULL;
        }

        m_fields.insert(std::make_pair(fields[i].name, fd));
    }
    m_info_size = max_size;
    if (NodePool::init_pool(&m_pool) < 0)
    {
        WARNING("failed to call init_pool");
        goto FAIL;
    }
    int mem_page_size;
    if (!config.get("mem_page_size", mem_page_size) || mem_page_size <= 0)
    {
        WARNING("failed to get mem_page_size");
        goto FAIL;
    }
    if (m_pool.register_item(m_info_size, mem_page_size) < 0)
    {
        WARNING("failed to register info_size to mempool");
        goto FAIL;
    }
    int node_page_size;
    if (!config.get("node_page_size", node_page_size) || node_page_size <= 0)
    {
        WARNING("failed to get node_page_size");
        goto FAIL;
    }
    if (m_pool.register_item(sizeof(NodePool::ObjectType), node_page_size) < 0)
    {
        WARNING("failed to register node_size to mempool");
        goto FAIL;
    }
    int max_items_num;
    if (!config.get("max_items_num", max_items_num) || max_items_num <= 0)
    {
        WARNING("failed to get max_items_num");
        goto FAIL;
    }
    if (m_pool.init(max_items_num) < 0)
    {
        WARNING("failed to init mempool");
        goto FAIL;
    }
    m_node_pool.init(&m_pool);
    int bucket_size;
    if (!config.get("bucket_size", bucket_size) || bucket_size <= 0)
    {
        WARNING("failed to get bucket_size");
        goto FAIL;
    }
    m_dict = new Hash(bucket_size);
    if (NULL == m_dict)
    {
        WARNING("failed to init hash dict");
        goto FAIL;
    }
    m_dict->set_pool(&m_node_pool);
    m_dict->set_cleanup(cleanup, (intptr_t)this);
    {
        __gnu_cxx::hash_map<std::string, FieldDes>::iterator it = m_fields.begin();
        while (it != m_fields.end())
        {
            if (SELF_DEFINE_TYPE == it->second.type)
            {
                m_cleanup_data.fields_need_free.push_back(
                        std::make_pair(it->second.array_offset, it->second.parser));
            }
            ++it;
        }
    }
    WARNING("mem_page_size[%d], node_page_size[%d], max_items_num[%d], bucket_size[%d]",
            mem_page_size, node_page_size, max_items_num, bucket_size);
    return 0;
FAIL:
    __gnu_cxx::hash_map<std::string, FieldDes>::iterator it = m_fields.begin();
    while (it != m_fields.end())
    {
        if (it->second.parser)
        {
            delete it->second.parser;
        }
        ++it;
    }
    m_fields.clear();
    return -1;
}

int ForwardIndex::get_offset_by_name(const char *name) const
{
    __gnu_cxx::hash_map<std::string, FieldDes>::const_iterator it = m_fields.find(name);
    if (it == m_fields.end())
    {
        return -1;
    }
    return it->second.offset;
}

void *ForwardIndex::get_info_by_id(long id) const
{
    vaddr_t *value = m_dict->find(id);
    if (value)
    {
        return m_pool.addr(*value);
    }
    return NULL;
}

bool ForwardIndex::update(long id, const std::vector<std::pair<std::string, std::string> > &kvs)
{
    std::vector<std::pair<std::string, cJSON *> > tmp;
    for (size_t i = 0; i < kvs.size(); ++i)
    {
        cJSON *json = cJSON_Parse(kvs[i].second.c_str());
        if (NULL == json)
        {
            break;
        }
        tmp.push_back(std::make_pair(kvs[i].first, json));
    }
    bool ret = true;
    if (tmp.size() < kvs.size())
    {
        ret = false;
        goto CLEAN;
    }
    ret = this->update(id, tmp);
CLEAN:
    for (size_t i = 0; i < tmp.size(); ++i)
    {
        cJSON_Delete(tmp[i].second);
    }
    return ret;
}

bool ForwardIndex::update(long id, const std::vector<std::pair<std::string, cJSON *> > &kvs)
{
    vaddr_t vnew = m_pool.alloc(m_info_size);
    void *mem = m_pool.addr(vnew);
    if (NULL == mem)
    {
        return false;
    }
    vaddr_t vold = 0;
    void *old = NULL;
    {
        vaddr_t *pv = m_dict->find(id);
        if (pv)
        {
            vold = *pv;
            old = m_pool.addr(vold);
        }
    }
    if (old)
    {
        ::memcpy(mem, old, m_info_size);
    }
    else
    {
        ::memset(mem, 0, m_info_size);
    }
    cleanup_data_t cd;
    cd.mem = NULL;
    cd.addr = 0;
    for (size_t i = 0; i < kvs.size(); ++i)
    {
        const std::pair<std::string, cJSON *> &kv = kvs[i];
        __gnu_cxx::hash_map<std::string, FieldDes>::const_iterator it = m_fields.find(kv.first);
        if (it == m_fields.end())
        {
            continue;
        }
        int array_offset = it->second.array_offset;
        int type = it->second.type;
        if (INT_TYPE == type)
        {
            if (kv.second->type == cJSON_Number)
            {
                ((int *)mem)[array_offset] = kv.second->valueint;
            }
        }
        else if (FLOAT_TYPE == type)
        {
            if (kv.second->type == cJSON_Number)
            {
                ((float *)mem)[array_offset] = kv.second->valuedouble;
            }
        }
        else
        {
            FieldParser *parser = it->second.parser;
            void *ptr = NULL;
            if (parser->parse(kv.second, ptr))
            {
                ((void **)mem)[array_offset] = ptr;
                cd.fields_need_free.push_back(std::make_pair(array_offset, parser));
            }
        }
    }
    if (!m_dict->insert(id, vnew))
    {
        cd.mem = mem;
        cd.addr = vnew;
        cd.clean();
        m_pool.free(vnew, m_info_size);
        return false;
    }
    if (old)
    {
        cd.mem = old;
        cd.addr = vold;
        m_delayed_list.push_back(cd);
    }
    return true;
}

void ForwardIndex::remove(long id)
{
    vaddr_t *ptr;
    if (m_dict->remove(id, ptr))
    {
        m_cleanup_data.mem = m_pool.addr(*ptr);
        m_cleanup_data.addr = *ptr;
        m_delayed_list.push_back(m_cleanup_data);
    }
}
