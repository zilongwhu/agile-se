#include <new>
#include "index/signdict.h"

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
