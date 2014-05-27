// =====================================================================================
//
//       Filename:  signdict.cpp
//
//    Description:  
//
//        Version:  1.0
//        Created:  05/26/2014 06:15:07 PM
//       Revision:  none
//       Compiler:  g++
//
//         Author:  Zilong.Zhu (), zilong.whu@gmail.com
//        Company:  edu.whu
//
// =====================================================================================

#include <new>
#include "signdict.h"

bool SignDict::dump(const char *dir) const
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
    WARNING("start to dump signdict");
    {
        WARNING("start to write %ssigndict.idx", path.c_str());
        FILE *idx = ::fopen((path + "signdict.idx").c_str(), "wb");
        if (NULL == idx)
        {
            WARNING("failed to open %ssigndict.idx for write", path.c_str());
            return false;
        }
        key_t key;
        value_t value;
        Hash::iterator it = m_dict->begin();
        size_t size = m_dict->size();
        if (::fwrite(&size, sizeof(size), 1, idx) != 1)
        {
            WARNING("failed to write size to idx");
            goto fail0;
        }
        while (it)
        {
            key = it.key();
            value = it.value();
            if (::fwrite(&key, sizeof(key), 1, idx) != 1)
            {
                WARNING("failed to write key to idx");
                goto fail0;
            }
            if (::fwrite(&value, sizeof(value), 1, idx) != 1)
            {
                ::fclose(idx);
                goto fail0;
            }
            ++it;
        }
        if (::fwrite(&m_ids[0], sizeof(m_ids[0]), m_ids.size(), idx) != m_ids.size())
        {
            WARNING("failed to write m_ids to idx");
            goto fail0;
        }
        ::fclose(idx);
        WARNING("write %ssigndict.idx ok", path.c_str());
        if (0)
        {
fail0:
            ::fclose(idx);
            return false;
        }
    }
    {
        WARNING("start to write %ssigndict.data", path.c_str());
        FILE *data = ::fopen((path + "signdict.data").c_str(), "wb");
        if (NULL == data)
        {
            WARNING("failed to open %ssigndict.data for write", path.c_str());
            return false;
        }
        if (::fwrite(&m_buffer_pos, sizeof(m_buffer_pos), 1, data) != 1)
        {
            WARNING("failed to write m_buffer_pos to %ssigndict.data", path.c_str());
            goto fail1;
        }
        if (::fwrite(&m_buffer_size, sizeof(m_buffer_size), 1, data) != 1)
        {
            WARNING("failed to write m_buffer_size to %ssigndict.data", path.c_str());
            goto fail1;
        }
        if (m_buffer_pos > 0 && ::fwrite(m_buffer, 1, m_buffer_pos, data) != m_buffer_pos)
        {
            WARNING("failed to write m_buffer to %ssigndict.data", path.c_str());
            goto fail1;
        }
        ::fclose(data);
        WARNING("write %ssigndict.data ok", path.c_str());
        if (0)
        {
fail1:
            ::fclose(data);
            return false;
        }
    }
    {
        WARNING("start to write %ssigndict.meta", path.c_str());
        FILE *meta = ::fopen((path + "signdict.meta").c_str(), "w");
        if (NULL == meta)
        {
            WARNING("failed to open %ssigndict.meta for write", path.c_str());
            return false;
        }
        for (size_t i = 0; i < m_ids.size(); ++i)
        {
            ::fprintf(meta, "%s\n", std::string(m_buffer + m_ids[i].first, m_ids[i].second).c_str());
        }
        ::fclose(meta);
        WARNING("write %ssigndict.meta ok", path.c_str());
    }
    WARNING("dump signdict ok");
    return false;
}

bool SignDict::load(const char *dir)
{
    m_dict->clear();
    m_buffer_pos = 0;
    m_ids.clear();

    if (NULL == dir || '\0' == *dir)
    {
        WARNING("empty dir error");
        return false;
    }
    WARNING("start to read dir[%s]", dir);
    std::string path(dir);
    if ('/' != path[path.length() - 1])
    {
        path += "/";
    }
    WARNING("start to read signdict");
    {
        WARNING("start to read %ssigndict.idx", path.c_str());
        FILE *idx = ::fopen((path + "signdict.idx").c_str(), "rb");
        if (NULL == idx)
        {
            WARNING("failed to open %ssigndict.idx for read", path.c_str());
            return false;
        }
        size_t size = 0;
        if (::fread(&size, sizeof(size), 1, idx) != 1)
        {
            WARNING("failed to read size from idx");
            goto fail0;
        }
        key_t key;
        value_t value;
        for (size_t i = 0; i < size; ++i)
        {
            if (::fread(&key, sizeof(key), 1, idx) != 1)
            {
                WARNING("failed to read key from idx");
                goto fail0;
            }
            if (::fread(&value, sizeof(value), 1, idx) != 1)
            {
                WARNING("failed to read value from idx");
                goto fail0;
            }
            if (!m_dict->insert(key, value))
            {
                WARNING("failed to insert key=value to m_dict");
                goto fail0;
            }
        }
        try
        {
            m_ids.resize(size);
        }
        catch (...)
        {
            WARNING("failed to resize m_ids, size=%lu", (uint64_t)size);
            goto fail0;
        }
        if (::fread(&m_ids[0], sizeof(m_ids[0]), m_ids.size(), idx) != m_ids.size())
        {
            WARNING("failed to read m_ids from idx");
            goto fail0;
        }
        ::fclose(idx);
        WARNING("read %ssigndict.idx ok", path.c_str());
        if (0)
        {
fail0:
            ::fclose(idx);
            return false;
        }
    }
    {
        WARNING("start to read %ssigndict.data", path.c_str());
        FILE *data = ::fopen((path + "signdict.data").c_str(), "rb");
        if (NULL == data)
        {
            WARNING("failed to open %ssigndict.data for read", path.c_str());
            return false;
        }
        uint32_t buffer_pos;
        uint32_t buffer_size;
        if (::fread(&buffer_pos, sizeof(buffer_pos), 1, data) != 1)
        {
            WARNING("failed to read buffer_pos from %ssigndict.data", path.c_str());
            goto fail1;
        }
        if (::fwrite(&buffer_size, sizeof(buffer_size), 1, data) != 1)
        {
            WARNING("failed to read buffer_size from %ssigndict.data", path.c_str());
            goto fail1;
        }
        char *buffer = new (std::nothrow) char[buffer_size];
        if (NULL == buffer)
        {
            WARNING("failed to alloc mem for buffer, buffer_size=%u", buffer_size);
            goto fail1;
        }
        if (buffer_pos > 0 && ::fwrite(buffer, 1, buffer_pos, data) != buffer_pos)
        {
            delete [] buffer;

            WARNING("failed to write m_buffer to %ssigndict.data", path.c_str());
            goto fail1;
        }
        ::fclose(data);
        WARNING("read %ssigndict.data ok", path.c_str());
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
            ::fclose(data);
            return false;
        }
    }
    WARNING("read signdict ok");
    return false;
}
