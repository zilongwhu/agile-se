#include <new>
#include "index/signdict.h"

SignDict::SignDict()
{
    m_dict = NULL;
    m_buffer = NULL;
    m_max_id = 1;
    m_buffer_pos = 0;
    m_buffer_size = 0;
}

SignDict::~SignDict()
{
    if (m_dict)
    {
        delete m_dict;
        m_dict = NULL;
    }
    if (m_buffer)
    {
        delete [] m_buffer;
        m_buffer = NULL;
    }
    m_max_id = 1;
    m_buffer_pos = 0;
    m_buffer_size = 0;
}

int SignDict::init(ObjectPool *pool, uint32_t bucket_size, uint32_t buffer_size)
{
    if (NULL == pool || 0 == bucket_size || 0 == buffer_size)
    {
        P_WARNING("invalid args: pool=%p, bucket_size=%u, buffer_size=%u",
                pool, bucket_size, buffer_size);
        return -1;
    }
    m_ids.reserve(bucket_size);
    m_dict = new Hash(bucket_size);
    if (NULL == m_dict)
    {
        P_WARNING("failed to new Hash, bucket_size=%u", bucket_size);
        return -1;
    }
    m_dict->set_pool(pool);
    m_buffer = new char[buffer_size];
    if (NULL == m_buffer)
    {
        P_WARNING("failed to new buffer, size=%u", buffer_size);
        delete m_dict;
        m_dict = NULL;
        return -1;
    }
    m_max_id = 1;
    m_buffer_pos = 0;
    m_buffer_size = buffer_size;
    P_WARNING("init sign dict ok");
    return 0;
}

void SignDict::print_meta() const
{
    P_WARNING("m_sign2id:");
    P_WARNING("    size=%lu", (uint64_t)m_dict->size());
    P_WARNING("    mem=%lu", (uint64_t)m_dict->mem_used());
    P_WARNING("    buffer_size=%u", m_buffer_size);
    P_WARNING("    ids_mem=%u", uint32_t(m_ids.capacity() * sizeof(std::pair<uint32_t, uint32_t>)));
}

bool SignDict::find(uint64_t sign, uint32_t &id) const /* thread safe */
{
    key_t key;
    key.sign1 = sign >> 32;
    key.sign2 = sign;
    value_t *pv = m_dict->find(key);
    if (pv)
    {
        id = pv->id;
        return true;
    }
    return false;
}

bool SignDict::find(uint32_t id, std::string &word) const /* do not use this func */
{
    --id;
    if (id >= m_ids.size())
    {
        word.clear();
        return false;
    }
    word = std::string(m_buffer + m_ids[id].first, m_ids[id].second);
    return true;
}

bool SignDict::find_or_insert(uint64_t sign, const char *word, uint32_t len, uint32_t &id) /* not thread safe */
{
#ifndef P_LOG_WORD
#define P_LOG_WORD( _fmt_, args... )
#endif
    if (NULL == word || 0 == len)
    {
        P_WARNING("invalid args: word=%p, len=%u", word, len);
        return false;
    }
    key_t key;
    key.sign1 = sign >> 32;
    key.sign2 = sign;
    value_t *pv = m_dict->find(key);
    if (pv)
    {
        id = pv->id;
    }
    else
    {
        if (m_buffer_pos + len > m_buffer_size)
        {
            char *buffer = new (std::nothrow) char[m_buffer_size << 1];
            if (NULL == buffer)
            {
                P_WARNING("failed to alloc buffer, size=%u", (uint32_t)(m_buffer_size << 1));
                return false;
            }
            if (m_buffer_pos > 0)
            {
                ::memcpy(buffer, m_buffer, m_buffer_pos);
            }
            delete [] m_buffer;
            m_buffer = buffer;
            m_buffer_size <<= 1;
        }
        m_ids.reserve(m_ids.size() + 1);

        value_t value;
        value.id = m_max_id;
        value.offset = m_buffer_pos;
        value.length = len;
        if (!m_dict->insert(key, value))
        {
            P_WARNING("failed to insert word[%*s], sign=%lu", len, word, sign);
            return false;
        }
        ::memcpy(m_buffer + m_buffer_pos, word, len);
        m_buffer_pos += len;
        id = m_max_id++;
        m_ids.push_back(std::make_pair(value.offset, value.length));
        P_LOG_WORD("insert word[%*s] ok, id=%u, sign=%lu", len, word, id, sign);
    }
    return true;
#undef P_LOG_WORD
}

bool SignDict::dump(const char *dir, FSInterface *fs) const
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
    P_WARNING("start to dump signdict");
    {
        P_WARNING("start to write %ssigndict.idx", path.c_str());
        File idx = fs->fopen((path + "signdict.idx").c_str(), "wb");
        if (NULL == idx)
        {
            P_WARNING("failed to open %ssigndict.idx for write", path.c_str());
            return false;
        }
        key_t key;
        value_t value;
        Hash::iterator it = m_dict->begin();
        size_t size = m_dict->size();
        if (fs->fwrite(&size, sizeof(size), 1, idx) != 1)
        {
            P_WARNING("failed to write size to idx");
            goto fail0;
        }
        while (it)
        {
            key = it.key();
            value = it.value();
            if (fs->fwrite(&key, sizeof(key), 1, idx) != 1)
            {
                P_WARNING("failed to write key to idx");
                goto fail0;
            }
            if (fs->fwrite(&value, sizeof(value), 1, idx) != 1)
            {
                fs->fclose(idx);
                goto fail0;
            }
            ++it;
        }
        if (fs->fwrite(&m_ids[0], sizeof(m_ids[0]), m_ids.size(), idx) != m_ids.size())
        {
            P_WARNING("failed to write m_ids to idx");
            goto fail0;
        }
        fs->fclose(idx);
        P_WARNING("write %ssigndict.idx ok", path.c_str());
        if (0)
        {
fail0:
            fs->fclose(idx);
            return false;
        }
    }
    {
        P_WARNING("start to write %ssigndict.data", path.c_str());
        File data = fs->fopen((path + "signdict.data").c_str(), "wb");
        if (NULL == data)
        {
            P_WARNING("failed to open %ssigndict.data for write", path.c_str());
            return false;
        }
        if (fs->fwrite(&m_buffer_pos, sizeof(m_buffer_pos), 1, data) != 1)
        {
            P_WARNING("failed to write m_buffer_pos to %ssigndict.data", path.c_str());
            goto fail1;
        }
        if (fs->fwrite(&m_buffer_size, sizeof(m_buffer_size), 1, data) != 1)
        {
            P_WARNING("failed to write m_buffer_size to %ssigndict.data", path.c_str());
            goto fail1;
        }
        if (m_buffer_pos > 0 && fs->fwrite(m_buffer, 1, m_buffer_pos, data) != m_buffer_pos)
        {
            P_WARNING("failed to write m_buffer to %ssigndict.data", path.c_str());
            goto fail1;
        }
        fs->fclose(data);
        P_WARNING("write %ssigndict.data ok", path.c_str());
        if (0)
        {
fail1:
            fs->fclose(data);
            return false;
        }
    }
    {
        P_WARNING("start to write %ssigndict.meta", path.c_str());
        File meta = fs->fopen((path + "signdict.meta").c_str(), "w");
        if (NULL == meta)
        {
            P_WARNING("failed to open %ssigndict.meta for write", path.c_str());
            return false;
        }
        for (size_t i = 0; i < m_ids.size(); ++i)
        {
            fs->fprintf(meta, "%s\n", std::string(m_buffer + m_ids[i].first, m_ids[i].second).c_str());
        }
        fs->fclose(meta);
        P_WARNING("write %ssigndict.meta ok", path.c_str());
    }
    P_WARNING("dump signdict ok");
    return true;
}

bool SignDict::load(const char *dir, FSInterface *fs)
{
    typedef FSInterface::File File;
    if (NULL == fs)
    {
        fs = &DefaultFS::s_default;
    }

    m_dict->clear();
    m_buffer_pos = 0;
    m_ids.clear();

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
    P_WARNING("start to read signdict");
    {
        P_WARNING("start to read %ssigndict.idx", path.c_str());
        File idx = fs->fopen((path + "signdict.idx").c_str(), "rb");
        if (NULL == idx)
        {
            P_WARNING("failed to open %ssigndict.idx for read", path.c_str());
            return false;
        }
        size_t size = 0;
        if (fs->fread(&size, sizeof(size), 1, idx) != 1)
        {
            P_WARNING("failed to read size from idx");
            goto fail0;
        }
        key_t key;
        value_t value;
        for (size_t i = 0; i < size; ++i)
        {
            if (fs->fread(&key, sizeof(key), 1, idx) != 1)
            {
                P_WARNING("failed to read key from idx");
                goto fail0;
            }
            if (fs->fread(&value, sizeof(value), 1, idx) != 1)
            {
                P_WARNING("failed to read value from idx");
                goto fail0;
            }
            if (!m_dict->insert(key, value))
            {
                P_WARNING("failed to insert key=value to m_dict");
                goto fail0;
            }
        }
        try
        {
            m_ids.resize(size);
        }
        catch (...)
        {
            P_WARNING("failed to resize m_ids, size=%lu", (uint64_t)size);
            goto fail0;
        }
        if (fs->fread(&m_ids[0], sizeof(m_ids[0]), m_ids.size(), idx) != m_ids.size())
        {
            P_WARNING("failed to read m_ids from idx");
            goto fail0;
        }
        fs->fclose(idx);
        P_WARNING("read %ssigndict.idx ok", path.c_str());
        if (0)
        {
fail0:
            fs->fclose(idx);
            return false;
        }
    }
    {
        P_WARNING("start to read %ssigndict.data", path.c_str());
        File data = fs->fopen((path + "signdict.data").c_str(), "rb");
        if (NULL == data)
        {
            P_WARNING("failed to open %ssigndict.data for read", path.c_str());
            return false;
        }
        uint32_t buffer_pos;
        uint32_t buffer_size;
        char *buffer = NULL;
        if (fs->fread(&buffer_pos, sizeof(buffer_pos), 1, data) != 1)
        {
            P_WARNING("failed to read buffer_pos from %ssigndict.data", path.c_str());
            goto fail1;
        }
        if (fs->fread(&buffer_size, sizeof(buffer_size), 1, data) != 1)
        {
            P_WARNING("failed to read buffer_size from %ssigndict.data", path.c_str());
            goto fail1;
        }
        buffer = new (std::nothrow) char[buffer_size];
        if (NULL == buffer)
        {
            P_WARNING("failed to alloc mem for buffer, buffer_size=%u", buffer_size);
            goto fail1;
        }
        if (buffer_pos > 0 && fs->fread(buffer, 1, buffer_pos, data) != buffer_pos)
        {
            delete [] buffer;

            P_WARNING("failed to read m_buffer to %ssigndict.data", path.c_str());
            goto fail1;
        }
        fs->fclose(data);
        P_WARNING("read %ssigndict.data ok", path.c_str());
        if (m_buffer)
        {
            delete [] m_buffer;
        }
        m_buffer = buffer;
        m_buffer_pos = buffer_pos;
        m_buffer_size = buffer_size;
        if (0)
        {
fail1:
            fs->fclose(data);
            return false;
        }
    }
    m_max_id = m_ids.size() + 1;
    P_WARNING("read signdict ok");
    return true;
}
