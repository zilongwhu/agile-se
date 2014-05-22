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

#include <sstream>
#include <fstream>
#include "log.h"
#include "configure.h"
#include "forward_index.h"
#include "json2pb.h"
#include <google/protobuf/descriptor.h>

using namespace google::protobuf;

struct FieldConfig
{
    std::string name;
    std::string pb_name;
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
}

int ForwardIndex::init(const char *path, const char *file)
{
    Config config(path, file);
    if (config.parse() < 0)
    {
        WARNING("failed parse config[%s:%s]", path, file);
        return -1;
    }
    std::ostringstream oss;
    int field_num = 0;
    if (!config.get("field_num", field_num) || field_num <= 0)
    {
        WARNING("failed to get field_num");
        return -1;
    }
    oss << "field_num: " << field_num << std::endl;
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
        oss << buffer << ": " << field.name << std::endl;

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
        oss << buffer << ": " << type << std::endl;

        if (SELF_DEFINE_TYPE == field.type)
        {
            ::snprintf(buffer, sizeof(buffer), "field_%d_pb_name", i);
            if (!config.get(buffer, field.pb_name))
            {
                WARNING("failed to get %s", buffer);
                return -1;
            }
            oss << buffer << ": " << field.pb_name << std::endl;
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
        oss << buffer << ": " << field.offset << std::endl;

        fields.push_back(field);
        if (max_size < field.offset + field.size)
        {
            max_size = field.offset + field.size;
        }
    }
    WARNING("info_size = %d", max_size);
    oss << "info_size: " << max_size << std::endl;

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
    const DescriptorPool *pl = DescriptorPool::generated_pool();
    if (NULL == pl)
    {
        WARNING("failed to get google::protobuf::DescriptorPool");
        return -1;
    }
    MessageFactory *mf = MessageFactory::generated_factory();
    if (NULL == mf)
    {
        WARNING("failed to get google::protobuf::MessageFactory");
        return -1;
    }
    for (size_t i = 0; i < fields.size(); ++i)
    {
        FieldDes fd;

        if (m_fields.find(fields[i].name) != m_fields.end())
        {
            WARNING("duplicate field name[%s]", fields[i].name.c_str());
            return -1;
        }
        fd.offset = fields[i].offset;
        fd.array_offset = fields[i].offset / fields[i].size;
        fd.type = fields[i].type;
        if (SELF_DEFINE_TYPE == fd.type)
        {
            const Descriptor *des = pl->FindMessageTypeByName(fields[i].pb_name);
            if (NULL == des)
            {
                WARNING("failed to get google::protobuf::Descriptor of [%s]", fields[i].pb_name.c_str());
                return -1;
            }
            fd.default_message = mf->GetPrototype(des);
            if (NULL == fd.default_message)
            {
                WARNING("failed to get default message for [%s]", fields[i].pb_name.c_str());
                return -1;
            }
        }
        else
        {
            fd.default_message = NULL;
        }

        m_fields.insert(std::make_pair(fields[i].name, fd));
    }
    m_info_size = max_size;
    if (NodePool::init_pool(&m_pool) < 0)
    {
        WARNING("failed to call init_pool");
        return -1;
    }
    if (m_pool.register_item(m_info_size) < 0)
    {
        WARNING("failed to register info_size to mempool");
        return -1;
    }
    if (m_pool.register_item(sizeof(NodePool::ObjectType)) < 0)
    {
        WARNING("failed to register node_size to mempool");
        return -1;
    }
    int max_items_num;
    if (!config.get("max_items_num", max_items_num) || max_items_num <= 0)
    {
        WARNING("failed to get max_items_num");
        return -1;
    }
    if (m_pool.init(max_items_num) < 0)
    {
        WARNING("failed to init mempool");
        return -1;
    }
    m_node_pool.init(&m_pool);
    int bucket_size;
    if (!config.get("bucket_size", bucket_size) || bucket_size <= 0)
    {
        WARNING("failed to get bucket_size");
        return -1;
    }
    m_dict = new Hash(bucket_size);
    if (NULL == m_dict)
    {
        WARNING("failed to init hash dict");
        return -1;
    }
    m_dict->set_pool(&m_node_pool);
    m_dict->set_cleanup(cleanup, (intptr_t)this);
    {
        __gnu_cxx::hash_map<std::string, FieldDes>::iterator it = m_fields.begin();
        while (it != m_fields.end())
        {
            if (SELF_DEFINE_TYPE == it->second.type)
            {
                m_cleanup_data.fields_need_free.push_back(it->second.array_offset);
            }
            ++it;
        }
    }
    m_meta = oss.str();
    WARNING("max_items_num[%d], bucket_size[%d]", max_items_num, bucket_size);
    return 0;
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

void *ForwardIndex::get_info_by_id(int32_t id) const
{
    vaddr_t *value = m_dict->find(id);
    if (value)
    {
        return m_pool.addr(*value);
    }
    return NULL;
}

bool ForwardIndex::update(int32_t id, const std::vector<std::pair<std::string, std::string> > &kvs)
{
    std::vector<std::pair<std::string, cJSON *> > tmp;
    for (size_t i = 0; i < kvs.size(); ++i)
    {
        cJSON *json = cJSON_Parse(kvs[i].second.c_str());
        if (NULL == json)
        {
            WARNING("failed to parse json[%s]", kvs[i].second.c_str());
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

bool ForwardIndex::update(int32_t id, const std::vector<std::pair<std::string, cJSON *> > &kvs)
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
            Message *ptr = it->second.default_message->New();
            if (NULL == ptr)
            {
                WARNING("failed to new protobuf from default message, field_name[%s]", kv.first.c_str());
                goto FAIL;
            }
            ((void **)mem)[array_offset] = ptr;
            cd.fields_need_free.push_back(array_offset);
            if (cJSON_String == kv.second->type)
            {
                if (!json2pb(*ptr, kv.second->valuestring))
                {
                    WARNING("failed to parse from json, field_name[%s]", kv.first.c_str());
                    goto FAIL;
                }
            }
            else if (cJSON_Object == kv.second->type)
            {
                if (!json2pb(*ptr, kv.second))
                {
                    WARNING("failed to parse from json, field_name[%s]", kv.first.c_str());
                    goto FAIL;
                }
            }
            else
            {
                WARNING("invalid json type, field_name[%s]", kv.first.c_str());
                goto FAIL;
            }
        }
    }
    if (!m_dict->insert(id, vnew))
    {
FAIL:
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

void ForwardIndex::remove(int32_t id)
{
    vaddr_t addr;
    if (m_dict->remove(id, &addr))
    {
        m_cleanup_data.mem = m_pool.addr(addr);
        m_cleanup_data.addr = addr;
        m_delayed_list.push_back(m_cleanup_data);
    }
}

void ForwardIndex::print_meta() const
{
    m_pool.print_meta();

    WARNING("m_dict:");
    WARNING("    size=%lu", (uint64_t)m_dict->size());
    WARNING("    mem=%lu", (uint64_t)m_dict->mem_used());
    WARNING("    total_mem=%lu", (uint64_t)m_dict->size() * m_info_size);
}

bool ForwardIndex::dump(const char *dir) const
{
    if (NULL == dir || '\0' == *dir)
    {
        WARNING("empty dir error");
        return false;
    }
    WARNING("start to write dir[%s]", dir);
    std::string path(dir);
    if ('/' != path[path.length() - 1])
    {
        path += "/";
    }
    FILE *idx = ::fopen((path + "forward.idx").c_str(), "wb");
    if (NULL == idx)
    {
        WARNING("failed to open file[%sforward.idx] for write", path.c_str());
        return false;
    }
    FILE *data = ::fopen((path + "forward.data").c_str(), "wb");
    if (NULL == data)
    {
        ::fclose(idx);

        WARNING("failed to open file[%sforward.data] for write", path.c_str());
        return false;
    }
    bool ret = true;
    size_t offset = 0;
    size_t size = m_dict->size();
    uint32_t length = 0;
    Message *message = NULL;
    Hash::iterator it = m_dict->begin();
    const std::vector<int> &self_defines = m_cleanup_data.fields_need_free;

    size_t buffer_size = 1024*1024;
    char *buffer = new char[buffer_size];
    if (NULL == buffer)
    {
        WARNING("failed to init buffer");
        goto FAIL;
    }
    {
        std::ofstream ofs((path + "forward.meta").c_str());
        ofs << "total: " << size << std::endl << m_meta;
        if (!ofs)
        {
            WARNING("failed to write meta info");
            goto FAIL;
        }
    }

    if (::fwrite(&size, sizeof(size), 1, idx) != 1)
    {
        WARNING("failed to write size");
        goto FAIL;
    }
    if (::fwrite(&m_info_size, sizeof(m_info_size), 1, idx) != 1)
    {
        WARNING("failed to write m_info_size");
        goto FAIL;
    }
    while (it)
    {
        int32_t key = it.key();
        void *mem = m_pool.addr(it.value());
        length = m_info_size;
        for (size_t i = 0; i < self_defines.size(); ++i)
        {
            message = ((Message **)mem)[self_defines[i]];
            if (message)
            {
                length += message->ByteSize();
            }
        }
        if (length > buffer_size)
        {
            char *new_buffer = new char[length];
            if (NULL == new_buffer)
            {
                WARNING("failed to new buffer, length=%u", length);
                goto FAIL;
            }
            delete [] buffer;
            buffer = new_buffer;
            buffer_size = length;
        }
        ::memcpy(buffer, mem, m_info_size);
        length = m_info_size;
        for (size_t i = 0; i < self_defines.size(); ++i)
        {
            message = ((Message **)mem)[self_defines[i]];
            if (message)
            {
                if (!message->SerializeToArray(buffer + length, buffer_size - length))
                {
                    WARNING("failed to serialize self define field to bytes");
                    goto FAIL;
                }
                uint32_t len = message->ByteSize();
                if (sizeof(void *) == sizeof(uint32_t))
                {
                    uint16_t *arr = (uint16_t *)message;
                    arr[0] = length;
                    arr[1] = len;
                }
                else
                {
                    uint32_t *arr = (uint32_t *)message;
                    arr[0] = length;
                    arr[1] = len;
                }
                length += len;
            }
        }
        if (::fwrite(buffer, length, 1, data) != 1)
        {
            WARNING("failed to write to data file");
            goto FAIL;
        }
        if (::fwrite(&key, sizeof(key), 1, idx) != 1)
        {
            WARNING("failed to write key to idx file");
            goto FAIL;
        }
        if (::fwrite(&offset, sizeof(offset), 1, idx) != 1)
        {
            WARNING("failed to write offset to idx file");
            goto FAIL;
        }
        if (::fwrite(&length, sizeof(length), 1, idx) != 1)
        {
            WARNING("failed to write length to idx file");
            goto FAIL;
        }
        offset += length;
        ++it;
    }
    WARNING("write to dir[%s] ok", dir);
    if (0)
    {
FAIL:
        ret = false;
    }
    ::fclose(data);
    ::fclose(idx);
    if (buffer)
    {
        delete [] buffer;
    }
    return ret;
}
