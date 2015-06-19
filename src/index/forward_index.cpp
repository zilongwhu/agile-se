#include <string>
#include <sstream>
#include <fstream>
#include "configure.h"
#include "log_utils.h"
#include "json2pb.h"
#include "encode_util.h"
#include "str_utils.h"
#include "index/forward_index.h"
#include <google/protobuf/descriptor.h>

std::map<std::string, IDMapper_creater> g_id_mappers;

using namespace google::protobuf;

cJSON *parse_forward_json(const std::string &json, std::vector<forward_data_t> &values)
{
    values.clear();

    cJSON *object = cJSON_Parse(json.c_str());
    if (NULL == object)
    {
        P_WARNING("failed to parse json<%s>", json.c_str());
        return NULL;
    }
    forward_data_t data;
    if (cJSON_Object != object->type)
    {
        P_WARNING("invalid json<%s>, must be an Object", json.c_str());
        goto FAIL;
    }
    for (cJSON *c = object->child; c; c = c->next)
    {
        if (cJSON_Array != c->type) {
            if (data.set(c)) {
                values.push_back(data);
            } else {
                goto FAIL;
            }
        } else {
            P_WARNING("invalid json<%s>, value of <%s> cannot be an Array",
                    json.c_str(), c->string);
            goto FAIL;
        }
    }
    return object;
FAIL:
    values.clear();
    cJSON_Delete(object);
    return NULL;
}

struct FieldConfig
{
    std::string name;
    std::string pb_name;
    int type;
    int offset;
    int size;
    double default_value;
};

void ForwardIndex::cleanup(Hash::node_t *node, intptr_t arg)
{
    ForwardIndex *ptr = (ForwardIndex *)arg;
    if (NULL == ptr || ptr->m_delayed_list.size() == 0)
    {
        P_FATAL("should not run to here");
        ::abort();
    }
    cleanup_data_t &cd = ptr->m_delayed_list.front();
    if (node->value.addr != cd.addr)
    {
        P_FATAL("should not run to here");
        ::abort();
    }
    cd.clean(&ptr->m_pool);
    ptr->m_pool.free(cd.addr, ptr->m_info_size);
    ptr->m_delayed_list.pop_front();
}

ForwardIndex::~ForwardIndex()
{
    if (m_idmap)
    {
        delete m_idmap;
        m_idmap = NULL;
    }
    if (m_dict)
    {
        cleanup_data_t cd(m_cleanup_data);
        Hash::iterator it = m_dict->begin();
        while (it)
        {
            cd.addr = it.value().addr;
            cd.mem = m_pool.addr(cd.addr);
            cd.clean(&m_pool);
            ++it;
        }
        m_dict->set_cleanup(NULL, 0); /* 已经手动释放，将回调清空 */
        delete m_dict;
        m_dict = NULL;
    }
    if (m_map)
    {
        delete m_map;
        m_map = NULL;
    }
    m_pool.set_delayed_time(0); /* 关闭延迟功能 */
    m_pool.recycle();
}

int ForwardIndex::init(const char *path, const char *file)
{
    TRY
    {
        Config conf(path, file);
        if (conf.parse() < 0)
        {
            P_WARNING("failed parse config[%s:%s]", path, file);
            return -1;
        }
        std::ostringstream oss;
        int max_size = 0;
        std::vector<FieldConfig> fields;
        uint32_t field_size = 0;
        if (!parseUInt32(conf["FieldSize"], field_size))
        {
            P_WARNING("failed to get FieldSize[uint32_t]");
            return -1;
        }
        for (uint32_t i = 0; i < field_size; ++i)
        {
            oss << "[@Fields]" << std::endl;

            char tmpbuf[256];
            FieldConfig field;

            ::snprintf(tmpbuf, sizeof tmpbuf, "field_%d_name", i);
            field.name = conf[tmpbuf];
            oss << "name: " << field.name << std::endl;

            std::string type;
            ::snprintf(tmpbuf, sizeof tmpbuf, "field_%d_type", i);
            type = conf[tmpbuf];
            oss << "type: " << type << std::endl;

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
            else if ("proto" == type)
            {
                field.type = PROTO_TYPE;
                field.size = sizeof(void *);
            }
            else if ("binary" == type)
            {
                field.type = BINARY_TYPE;
                field.size = sizeof(vaddr_t);;
            }
            else
            {
                P_WARNING("invalid field type=%s, i=%d", type.c_str(), int(i));
                return -1;
            }

            field.default_value = 0;
            if (PROTO_TYPE == field.type)
            {
                ::snprintf(tmpbuf, sizeof tmpbuf, "field_%d_pb_name", i);
                field.pb_name = conf[tmpbuf];
                oss << "pb_name: " << field.pb_name << std::endl;
            }
            else if (BINARY_TYPE != field.type)
            {
                std::string ds;
                ::snprintf(tmpbuf, sizeof tmpbuf, "field_%d_default", i);
                if (conf.get(tmpbuf, ds) && !parseDouble(ds, field.default_value))
                {
                    P_WARNING("invalid default value[%s]", tmpbuf);
                    return -1;
                }
                oss << "default: " << field.default_value << std::endl;
            }

            field.offset = ((max_size + field.size - 1) & (~(field.size - 1)));
            oss << "offset: " << field.offset << std::endl;

            fields.push_back(field);
            max_size = field.offset + field.size;

            oss << std::endl;
        }
        P_WARNING("info_size = %d", max_size);

        std::vector<int> placeholder(max_size, -1);
        for (size_t i = 0; i < fields.size(); ++i)
        {
            for (int j = 0; j < fields[i].size; ++j)
            {
                if (placeholder[fields[i].offset + j] != -1)
                {
                    P_WARNING("conflict configs at place[%d]", fields[i].offset + j);
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
                        P_WARNING("field[%04d], type[%d], name[%s] ", i, fields[j].type, fields[j].name.c_str());
                        break;
                    }
                }
            }
        }
        const DescriptorPool *pl = DescriptorPool::generated_pool();
        if (NULL == pl)
        {
            P_WARNING("failed to get google::protobuf::DescriptorPool");
            return -1;
        }
        MessageFactory *mf = MessageFactory::generated_factory();
        if (NULL == mf)
        {
            P_WARNING("failed to get google::protobuf::MessageFactory");
            return -1;
        }
        for (size_t i = 0; i < fields.size(); ++i)
        {
            FieldDes fd;

            if (m_fields.find(fields[i].name) != m_fields.end())
            {
                P_WARNING("duplicate field name[%s]", fields[i].name.c_str());
                return -1;
            }
            fd.offset = fields[i].offset;
            fd.array_offset = fields[i].offset / fields[i].size;
            fd.type = fields[i].type;
            if (PROTO_TYPE == fd.type)
            {
                const Descriptor *des = pl->FindMessageTypeByName(fields[i].pb_name);
                if (NULL == des)
                {
                    P_WARNING("failed to get google::protobuf::Descriptor of [%s]", fields[i].pb_name.c_str());
                    return -1;
                }
                fd.default_message = mf->GetPrototype(des);
                if (NULL == fd.default_message)
                {
                    P_WARNING("failed to get default message for [%s]", fields[i].pb_name.c_str());
                    return -1;
                }
            }
            else
            {
                fd.default_value = fields[i].default_value;
            }

            m_fields.insert(std::make_pair(fields[i].name, fd));
        }
        m_info_size = max_size;
        m_default_messages.resize(m_info_size/sizeof(void *), NULL);
        if (m_node_pool.init(&m_pool) < 0)
        {
            P_WARNING("failed to init NodePool");
            return -1;
        }
        if (m_id_pool.init(&m_pool) < 0)
        {
            P_WARNING("failed to init IDPool");
            return -1;
        }
        if (m_pool.register_item(m_info_size) < 0)
        {
            P_WARNING("failed to register info_size to mempool");
            return -1;
        }
        for (size_t i = 0; i < m_binary_size.size(); ++i)
        {
            if (m_pool.register_item(m_binary_size[i]) < 0)
            {
                P_WARNING("failed to register binary_size=%u to mempool", m_binary_size[i]);
                return -1;
            }
        }
        std::string mapper;
        if (conf.get("id_mapper", mapper))
        {
            std::map<std::string, IDMapper_creater>::const_iterator it = g_id_mappers.find(mapper);
            if (it == g_id_mappers.end())
            {
                P_WARNING("unregistered id mapper[%s]", mapper.c_str());
                return -1;
            }
            m_map = (*it->second)();
            if (NULL == m_map)
            {
                P_WARNING("failed to create id mapper[%s]", mapper.c_str());
                return -1;
            }
            P_WARNING("using id mapper[%s]", mapper.c_str());
        }
        uint32_t max_items_num;
        uint32_t bucket_size;
        if (!parseUInt32(conf["max_items_num"], max_items_num))
        {
            P_WARNING("max_items_num must be uint32_t");
            goto FAIL;
        }
        if (m_pool.init(max_items_num) < 0)
        {
            P_WARNING("failed to init mempool");
            goto FAIL;
        }
        if (!parseUInt32(conf["bucket_size"], bucket_size))
        {
            P_WARNING("bucket_size must be uint32_t");
            goto FAIL;
        }
        m_idmap = new IDMap(bucket_size);
        if (NULL == m_idmap)
        {
            P_WARNING("failed to init id map");
            goto FAIL;
        }
        m_idmap->set_pool(&m_id_pool);
        m_dict = new Hash(bucket_size);
        if (NULL == m_dict)
        {
            P_WARNING("failed to init hash dict");
            goto FAIL;
        }
        m_dict->set_pool(&m_node_pool);
        m_dict->set_cleanup(cleanup, (intptr_t)this);
        {
            __gnu_cxx::hash_map<std::string, FieldDes>::iterator it = m_fields.begin();
            while (it != m_fields.end())
            {
                if (BINARY_TYPE == it->second.type)
                {
                    m_cleanup_data.binary_fields.push_back(it->second.array_offset);
                }
                else if (PROTO_TYPE == it->second.type)
                {
                    m_cleanup_data.protobuf_fields.push_back(it->second.array_offset);
                    m_default_messages[it->second.array_offset] = it->second.default_message;
                }
                else if (it->second.default_value >= 0.000001
                        || it->second.default_value <= -0.000001)
                {
                    if (INT_TYPE == it->second.type)
                    {
                        m_default_values.push_back(std::make_pair(((it->second.array_offset << 2)), it->second.default_value));
                    }
                    else
                    {
                        m_default_values.push_back(std::make_pair(((it->second.array_offset << 2) | 0x1), it->second.default_value));
                    }
                }
                m_field_names.push_back(std::make_pair(it->second.offset, it->first));
                ++it;
            }
            std::sort(m_cleanup_data.binary_fields.begin(), m_cleanup_data.binary_fields.end());
            std::sort(m_cleanup_data.protobuf_fields.begin(), m_cleanup_data.protobuf_fields.end());
            std::sort(m_default_values.begin(), m_default_values.end());
            std::sort(m_field_names.begin(), m_field_names.end());
        }
        m_meta = oss.str();
        {
            std::vector<std::string> elems;
            split(m_meta, "\n", elems);

            for (size_t i = 0; i < elems.size(); ++i)
            {
                P_WARNING("%s", elems[i].c_str());
            }
        }
        P_WARNING("max_items_num[%d], bucket_size[%d]", max_items_num, bucket_size);
        return 0;
    }
    WARNING_CATCH("failed to init forward index");
FAIL:
    if (m_idmap)
    {
        delete m_idmap;
        m_idmap = NULL;
    }
    if (m_dict)
    {
        delete m_dict;
        m_dict = NULL;
    }
    if (m_map)
    {
        delete m_map;
        m_map = NULL;
    }
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

int ForwardIndex::get_array_offset_by_name(const char *name) const
{
    __gnu_cxx::hash_map<std::string, FieldDes>::const_iterator it = m_fields.find(name);
    if (it == m_fields.end())
    {
        return -1;
    }
    return it->second.array_offset;
}

void *ForwardIndex::get_info_by_id(int32_t id, int32_t *oid) const
{
    value_t *value = m_dict->find(id);
    if (value)
    {
        if (oid)
        {
            *oid = value->oid;
        }
        return m_pool.addr(value->addr);
    }
    return NULL;
}

int32_t ForwardIndex::get_id_by_oid(int32_t oid) const
{
    int32_t *value = m_idmap->find(oid);
    if (value)
    {
        return *value;
    }
    return -1;
}

bool ForwardIndex::update(int32_t oid, const std::vector<forward_data_t> &fields, ids_t *p_ids)
{
    vaddr_t vnew = m_pool.alloc(m_info_size);
    void *mem = m_pool.addr(vnew);
    if (NULL == mem)
    {
        P_WARNING("failed to alloc mem for update, oid=%d", oid);
        return false;
    }
    int32_t id;
    const value_t value = { oid, vnew };
    /* ========== get old value ========== */
    int32_t old_id = -1;
    vaddr_t vold = 0;
    void *old = NULL;
    {
        const int32_t *p_old_id = m_idmap->find(oid); /* try to get old internal id */
        if (p_old_id)
        {
            old_id = *p_old_id;
            const value_t *pv = m_dict->find(old_id);
            if (pv)
            {
                vold = pv->addr;
                old = m_pool.addr(vold);
                if (NULL == old)
                {
                    P_FATAL("oid[%d] old_id[%d]'s old is NULL", oid, old_id);
                }
            }
            else /* m_idmap does not match m_dict */
            {
                P_FATAL("oid[%d] is mapped to id[%d], but id[%d] doesn't exist in m_dict", oid, old_id, old_id);

                old_id = -1;
                m_idmap->remove(oid);
            }
        }
    }
    /* ========== init data ========== */
    if (old)
    {
        ::memcpy(mem, old, m_info_size); /* copy old data, then modify it */
    }
    else /* brand new data with default values setted */
    {
        ::memset(mem, 0, m_info_size); /* default value is 0 */
        for (size_t i = 0; i < m_default_values.size(); ++i) /* set configed default values */
        {
            uint32_t f = m_default_values[i].first;
            if (f & 0x1)
            {
                ((float *)mem)[f >> 2] = m_default_values[i].second;
            }
            else
            {
                ((int *)mem)[f >> 2] = int(m_default_values[i].second);
            }
        }
    }
    /* ========== modify data ========== */
    cleanup_data_t cd;
    cd.mem = NULL;
    cd.addr = 0;
    for (size_t i = 0; i < fields.size(); ++i)
    {
        const forward_data_t &field = fields[i];
        __gnu_cxx::hash_map<std::string, FieldDes>::const_iterator it
            = m_fields.find(field.key);
        if (it == m_fields.end())
        {
            continue;
        }
        const int array_offset = it->second.array_offset;
        const int type = it->second.type;
        if (INT_TYPE == type)
        {
            if (cJSON_Number == field.value->type)
            {
                ((int *)mem)[array_offset] = field.value->valueint;
            }
        }
        else if (FLOAT_TYPE == type)
        {
            if (cJSON_Number == field.value->type)
            {
                ((float *)mem)[array_offset] = field.value->valuedouble;
            }
        }
        else if (BINARY_TYPE == type)
        {
            if (cJSON_String == field.value->type && field.value->valuestring)
            {
                const int inlen = ::strlen(field.value->valuestring);
                int outlen = sizeof(m_binary_buffer);
                if (::std_base64_decode(m_binary_buffer, &outlen, field.value->valuestring, inlen) != 0)
                {
                    P_WARNING("failed to decode binary[%s], field_name[%s]",
                            field.value->valuestring, field.key);
                    goto FAIL;
                }
                uint32_t binary_size = 0;
                for (size_t j = 0; j < m_binary_size.size(); ++j)
                {
                    if (m_binary_size[j] >= outlen + sizeof(uint32_t) + sizeof(uint32_t))
                    {
                        binary_size = m_binary_size[j];
                        break;
                    }
                }
                if (0 == binary_size)
                {
                    P_WARNING("too long binary length=%d, field_name[%s]", outlen, field.key);
                    goto FAIL;
                }
                vaddr_t vbinary = m_pool.alloc(binary_size);
                uint32_t *binary = (uint32_t *)m_pool.addr(vbinary);
                if (NULL == binary)
                {
                    P_WARNING("failed to alloc binary, size=%u", binary_size);
                    goto FAIL;
                }
                binary[0] = outlen; /* real length */
                binary[1] = binary_size; /* alloced length */
                ::memcpy(binary + 2, m_binary_buffer, outlen);
                ((vaddr_t *)mem)[array_offset] = vbinary;
                cd.binary_fields.push_back(array_offset);
            }
            else
            {
                P_WARNING("invalid json type, field_name[%s]", field.key);
                goto FAIL;
            }
        }
        else
        {
            Message *ptr = it->second.default_message->New();
            if (NULL == ptr)
            {
                P_WARNING("failed to new protobuf from default message, field_name[%s]", field.key);
                goto FAIL;
            }
            ((void **)mem)[array_offset] = ptr;
            cd.protobuf_fields.push_back(array_offset);
            if (cJSON_String == field.value->type)
            {
                if (!json2pb(*ptr, field.value->valuestring))
                {
                    P_WARNING("failed to parse from json, field_name[%s]", field.key);
                    goto FAIL;
                }
            }
            else if (cJSON_Object == field.value->type)
            {
                if (!json2pb(*ptr, field.value))
                {
                    P_WARNING("failed to parse from json, field_name[%s]", field.key);
                    goto FAIL;
                }
            }
            else
            {
                P_WARNING("invalid json type, field_name[%s]", field.key);
                goto FAIL;
            }
        }
    }
    id = m_map ? m_map->map(oid, mem) : oid;
    if (id < 0)
    {
        P_WARNING("failed to map oid[%d]", oid);
        goto FAIL;
    }
    P_TRACE("map oid[%d] to id[%d]", oid, id);

    if (old_id != id && !m_idmap->insert(oid, id))
        /* modify m_idmap first, it is used by inc thread only */
    {
        P_WARNING("failed to map oid[%d] => id[%d]", oid, id);
        goto FAIL;
    }
    if (!m_dict->insert(id, value))
    {
        if (old_id != id) /* rollback m_idmap */
        {
            if (old)
            {
                if (!m_idmap->insert(oid, old_id))
                {
                    P_FATAL("failed to rollback m_idmap, oid=%d, old_id=%d", oid, old_id);
                }
            }
            else
            {
                m_idmap->remove(oid);
            }
        }
FAIL:
        cd.mem = mem;
        cd.addr = vnew;
        cd.clean(&m_pool);
        m_pool.free(vnew, m_info_size);
        return false;
    }
    if (old)
    {
        if (old_id != id)
        {
            m_dict->remove(old_id);
        }
        cd.mem = old;
        cd.addr = vold;
        m_delayed_list.push_back(cd);
    }
    if (p_ids)
    {
        p_ids->old_id = old_id;
        p_ids->new_id = id;
    }
    return true;
}

bool ForwardIndex::remove(int32_t oid, int32_t *p_id)
{
    int32_t id;
    if (m_idmap->remove(oid, &id)) /* remove & get inertnal id from oid */
    {
        value_t value;
        if (m_dict->remove(id, &value))
        {
            if (value.oid != oid)
            {
                P_FATAL("oid[%d] => id[%d], but value.oid[%d] != oid[%d]", oid, id, value.oid, oid);
            }
            m_cleanup_data.mem = m_pool.addr(value.addr);
            m_cleanup_data.addr = value.addr;
            m_delayed_list.push_back(m_cleanup_data);
            if (p_id)
            {
                *p_id = id;
            }
            if (m_map)
            {
                m_map->remove(oid);
            }
            return true;
        }
        else
        {
            P_FATAL("oid[%d] => id[%d], but id[%d] is not exist in m_dict", oid, id, id);
        }
    }
    return false;
}

void ForwardIndex::print_meta() const
{
    m_pool.print_meta();

    P_WARNING("m_dict:");
    P_WARNING("    size=%lu", (uint64_t)m_dict->size());
    P_WARNING("    mem=%lu", (uint64_t)m_dict->mem_used());
    P_WARNING("    total_mem=%lu", (uint64_t)m_dict->size() * m_info_size);

    P_WARNING("m_idmap:");
    P_WARNING("    size=%lu", (uint64_t)m_idmap->size());
    P_WARNING("    mem=%lu", (uint64_t)m_idmap->mem_used());
}

bool ForwardIndex::dump(const char *dir, FSInterface *fs) const
{
    typedef FSInterface::File File;
    if (NULL == fs)
    {
        fs = &DefaultFS::s_default;
    }

    if (NULL == dir || '\0' == *dir)
    {
        P_WARNING("empty dir error");
        return false;
    }
    P_WARNING("start to write dir[%s]", dir);
    std::string path(dir);
    if ('/' != path[path.length() - 1])
    {
        path += "/";
    }
    File idx = fs->fopen((path + "forward.idx").c_str(), "wb");
    if (NULL == idx)
    {
        P_WARNING("failed to open file[%sforward.idx] for write", path.c_str());
        return false;
    }
    File data = fs->fopen((path + "forward.data").c_str(), "wb");
    if (NULL == data)
    {
        fs->fclose(idx);

        P_WARNING("failed to open file[%sforward.data] for write", path.c_str());
        return false;
    }
    bool ret = true;
    size_t offset = 0;
    size_t size = m_dict->size();
    uint32_t length = 0;
    Message *message = NULL;
    Hash::iterator it = m_dict->begin();
    const std::vector<int> &binaries = m_cleanup_data.binary_fields;
    const std::vector<int> &protos = m_cleanup_data.protobuf_fields;

    size_t buffer_size = 1024*1024;
    char *buffer = new char[buffer_size];
    if (NULL == buffer)
    {
        P_WARNING("failed to init buffer");
        goto FAIL;
    }
    {
        File meta = fs->fopen((path + "forward.meta").c_str(), "w+");
        if((fs->fprintf(meta,"total:%lu\n"
                        "info_size:%lu\n\n"
                        "%s\n",
                        size, m_info_size, m_meta.c_str()))<0)
        {
            P_WARNING("failed to write meta info");
            goto FAIL;
        }
        fs->fclose(meta);
    }
    if (fs->fwrite(&size, sizeof(size), 1, idx) != 1)
    {
        P_WARNING("failed to write size");
        goto FAIL;
    }
    if (fs->fwrite(&m_info_size, sizeof(m_info_size), 1, idx) != 1)
    {
        P_WARNING("failed to write m_info_size");
        goto FAIL;
    }
    while (it)
    {
        int32_t oid = it.value().oid;
        int32_t id = it.key();
        void *mem = m_pool.addr(it.value().addr);
        length = m_info_size;
        for (size_t i = 0; i < protos.size(); ++i)
        {
            message = ((Message **)mem)[protos[i]];
            if (message)
            {
                length += message->ByteSize();
            }
        }
        for (size_t i = 0; i < binaries.size(); ++i)
        {
            vaddr_t vbinary = ((vaddr_t *)mem)[binaries[i]];
            if (vbinary)
            {
                uint32_t *binary = (uint32_t *)m_pool.addr(vbinary);
                length += binary[0];
            }
        }
        if (length > buffer_size)
        {
            char *new_buffer = new char[length];
            if (NULL == new_buffer)
            {
                P_WARNING("failed to new buffer, length=%u", length);
                goto FAIL;
            }
            delete [] buffer;
            buffer = new_buffer;
            buffer_size = length;
        }
        ::memcpy(buffer, mem, m_info_size);
        length = m_info_size;
        for (size_t i = 0; i < protos.size(); ++i)
        {
            message = ((Message **)buffer)[protos[i]];
            if (message)
            {
                if (!message->SerializeToArray(buffer + length, buffer_size - length))
                {
                    P_WARNING("failed to serialize self define field to bytes");
                    goto FAIL;
                }
                uint32_t len = message->ByteSize();
                ((Message **)buffer)[protos[i]] = (Message *)(intptr_t)len;
                length += len;
            }
        }
        for (size_t i = 0; i < binaries.size(); ++i)
        {
            vaddr_t vbinary = ((vaddr_t *)mem)[binaries[i]];
            if (vbinary)
            {
                uint32_t *binary = (uint32_t *)m_pool.addr(vbinary);
                ::memcpy(buffer + length, binary + 2, binary[0]);
                ((vaddr_t *)buffer)[binaries[i]] = (vaddr_t)binary[0];
                length += binary[0];
            }
        }
        if (fs->fwrite(buffer, length, 1, data) != 1)
        {
            P_WARNING("failed to write to data file");
            goto FAIL;
        }
        if (fs->fwrite(&oid, sizeof(oid), 1, idx) != 1)
        {
            P_WARNING("failed to write oid to idx file");
            goto FAIL;
        }
        if (fs->fwrite(&id, sizeof(id), 1, idx) != 1)
        {
            P_WARNING("failed to write id to idx file");
            goto FAIL;
        }
        if (fs->fwrite(&offset, sizeof(offset), 1, idx) != 1)
        {
            P_WARNING("failed to write offset to idx file");
            goto FAIL;
        }
        if (fs->fwrite(&length, sizeof(length), 1, idx) != 1)
        {
            P_WARNING("failed to write length to idx file");
            goto FAIL;
        }
        offset += length;
        ++it;
    }
    if (m_map)
    {
        if (!m_map->dump(dir, fs))
        {
            P_WARNING("failed to dump id mapper");
            goto FAIL;
        }
        P_WARNING("dump id mapper ok");
    }
    P_WARNING("write to dir[%s] ok", dir);
    if (0)
    {
FAIL:
        ret = false;
    }
    fs->fclose(data);
    fs->fclose(idx);
    if (buffer)
    {
        delete [] buffer;
    }
    return ret;
}

bool ForwardIndex::load(const char *dir, FSInterface *fs)
{
    typedef FSInterface::File File;
    if (NULL == fs)
    {
        fs = &DefaultFS::s_default;
    }

    if (NULL == dir || '\0' == *dir)
    {
        P_WARNING("empty dir error");
        return false;
    }
    P_WARNING("start to read dir[%s]", dir);
    std::string path(dir);
    if ('/' != path[path.length() - 1])
    {
        path += "/";
    }
    uint32_t total = 0;
    uint32_t info_size = 0;
    TRY
    {
        Config conf(std::string(path.c_str(), path.length() - 1).c_str(), "forward.meta");
        if (conf.parse() < 0)
        {
            P_WARNING("failed parse config[%sforward.meta]", path.c_str());
            return false;
        }
        if (!parseUInt32(conf["total"], total))
        {
            P_WARNING("failed to parse total");
            return false;
        }
        if (!parseUInt32(conf["info_size"], info_size))
        {
            P_WARNING("failed to parse info_size");
            return false;
        }
        P_WARNING("total=%u, m_info_size=%u, info_size=%u", total, (uint32_t)m_info_size, info_size);
        if (m_info_size < info_size)
        {
            P_WARNING("m_info_size is smaller, error");
            return false;
        }
    }
    WARNING_CATCH_EXC(return false, "failed to get total & info_size from forward.meta");
    File idx = fs->fopen((path + "forward.idx").c_str(), "rb");
    if (NULL == idx)
    {
        P_WARNING("failed to open file[%sforward.idx] for read", path.c_str());
        return false;
    }
    File data = fs->fopen((path + "forward.data").c_str(), "rb");
    if (NULL == data)
    {
        fs->fclose(idx);

        P_WARNING("failed to open file[%sforward.data] for read", path.c_str());
        return false;
    }
    bool ret = true;
    size_t offset = 0;
    Message *message = NULL;
    const std::vector<int> &binaries = m_cleanup_data.binary_fields;
    const std::vector<int> &protos = m_cleanup_data.protobuf_fields;

    size_t buffer_size = 1024*1024;
    buffer_size = buffer_size > info_size ? buffer_size : info_size;
    char *buffer = new char[buffer_size];
    if (NULL == buffer)
    {
        P_WARNING("failed to init buffer");
        goto FAIL;
    }
    {
        size_t tmp = 0;
        if (fs->fread(&tmp, sizeof(tmp), 1, idx) != 1)
        {
            P_WARNING("failed to read size from idx");
            goto FAIL;
        }
        if (tmp != total)
        {
            P_WARNING("meta show total=%u, buf idx say total=%u", total, (uint32_t)tmp);
            goto FAIL;
        }
        if (fs->fread(&tmp, sizeof(tmp), 1, idx) != 1)
        {
            P_WARNING("failed to read info_size from idx");
            goto FAIL;
        }
        if (tmp != info_size)
        {
            P_WARNING("meta show info_size=%u, buf idx say info_size=%u", info_size, (uint32_t)tmp);
            goto FAIL;
        }
    }
    P_WARNING("start to read dir[%s]", dir);
    for (uint32_t i = 0; i < total; ++i)
    {
        int32_t id;
        int32_t oid;
        size_t tmp;                             /* offset */
        uint32_t length;

        if (fs->fread(&oid, sizeof(oid), 1, idx) != 1)
        {
            P_WARNING("failed to get oid from idx, i=%u", i);
            goto FAIL;
        }
        if (fs->fread(&id, sizeof(id), 1, idx) != 1)
        {
            P_WARNING("failed to get id from idx, i=%u", i);
            goto FAIL;
        }
        if (fs->fread(&tmp, sizeof(tmp), 1, idx) != 1)
        {
            P_WARNING("failed to get offset from idx, i=%u", i);
            goto FAIL;
        }
        if (tmp != offset)
        {
            P_WARNING("offset error, offset from idx=%lu, real offset=%lu, i=%u", (uint64_t)tmp, (uint64_t)offset, i);
            goto FAIL;
        }
        if (fs->fread(&length, sizeof(length), 1, idx) != 1)
        {
            P_WARNING("failed to get length from idx, i=%u", i);
            goto FAIL;
        }
        if (length > buffer_size)
        {
            char *new_buffer = new char[length];
            if (NULL == new_buffer)
            {
                P_WARNING("failed to new buffer, length=%u", length);
                goto FAIL;
            }
            delete [] buffer;
            buffer = new_buffer;
            buffer_size = length;
        }
        if (fs->fread(buffer, length, 1, data) != 1)
        {
            P_WARNING("failed to get data, i=%u", i);
            goto FAIL;
        }
        offset += length;
        vaddr_t vnew = m_pool.alloc(m_info_size);
        void *mem = m_pool.addr(vnew);
        if (NULL == mem)
        {
            P_WARNING("failed to alloc mem, i=%u", i);
            goto FAIL;
        }
        const value_t value = { oid, vnew };
        ::memcpy(mem, buffer, info_size);
        if (m_info_size > info_size)
        {
            ::memset(((char *)mem) + info_size, 0, m_info_size - info_size);
        }
        uint32_t len = info_size;
        bool fail = false;
        for (size_t n = 0; n < protos.size(); ++n)
        {
            if ((protos[n] + 1) * sizeof(void *) > info_size)
            {
                break;
            }
            uint32_t tmp_len = (uint32_t)(intptr_t)((void **)mem)[protos[n]];
            if (tmp_len > 0)
            {
                message = m_default_messages[protos[n]]->New();
                if (NULL == message)
                {
                    P_WARNING("failed to create message");
                    fail = true;
                }
                else if (!message->ParseFromArray(buffer + len, tmp_len))
                {
                    P_WARNING("failed to deserialize message");
                    fail = true;
                }
            }
            else
            {
                message = NULL;
            }
            ((Message **)mem)[protos[n]] = message;
            len += tmp_len;
        }
        for (size_t n = 0; n < binaries.size(); ++n)
        {
            if ((binaries[n] + 1) * sizeof(vaddr_t) > info_size)
            {
                break;
            }
            vaddr_t vbinary = 0;
            uint32_t tmp_len = (uint32_t)((vaddr_t *)mem)[binaries[n]];
            if (tmp_len > 0)
            {
                uint32_t binary_size = 0;
                for (size_t k = 0; k < m_binary_size.size(); ++k)
                {
                    if (m_binary_size[k] >= tmp_len + sizeof(uint32_t) + sizeof(uint32_t))
                    {
                        binary_size = m_binary_size[k];
                        break;
                    }
                }
                if (0 == binary_size)
                {
                    P_WARNING("too long binary length=%d", tmp_len);
                    fail = true;
                }
                else
                {
                    vbinary = m_pool.alloc(binary_size);
                    uint32_t *binary = (uint32_t *)m_pool.addr(vbinary);
                    if (NULL == binary)
                    {
                        P_WARNING("failed to alloc binary, size=%u", binary_size);
                        fail = true;
                    }
                    else
                    {
                        binary[0] = tmp_len;
                        binary[1] = binary_size;
                        ::memcpy(binary + 2, buffer + len, tmp_len);
                    }
                }
            }
            ((vaddr_t *)mem)[binaries[n]] = vbinary;
            len += tmp_len;
        }
        for (size_t n = 0; n < m_default_values.size(); ++n)
        {
            uint32_t f = m_default_values[n].first;
            if ((f & ~0x3) < info_size)
            {
                continue;
            }
            if (f & 0x1)
            {
                ((float *)mem)[f >> 2] = m_default_values[n].second;
            }
            else
            {
                ((int *)mem)[f >> 2] = int(m_default_values[n].second);
            }
        }
        if (len != length)
        {
            m_cleanup_data.mem = mem;
            m_cleanup_data.addr = vnew;
            m_cleanup_data.clean(&m_pool);
            P_WARNING("corrupted data file");
            goto FAIL;
        }
        if (fail || !m_idmap->insert(oid, id) || !m_dict->insert(id, value))
        {
            m_cleanup_data.mem = mem;
            m_cleanup_data.addr = vnew;
            m_cleanup_data.clean(&m_pool);
            P_WARNING("failed to insert id=%d, oid=%d, i=%u", id, oid, i);
            goto FAIL;
        }
    }
    if (m_map)
    {
        if (!m_map->load(dir, fs))
        {
            P_WARNING("failed to load id mapper");
            goto FAIL;
        }
        P_WARNING("load id mapper ok");
    }
    P_WARNING("read dir[%s] ok", dir);
    if (0)
    {
FAIL:
        ret = false;
    }
    fs->fclose(data);
    fs->fclose(idx);
    if (buffer)
    {
        delete [] buffer;
    }
    return ret;
}
