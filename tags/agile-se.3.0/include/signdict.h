// =====================================================================================
//
//       Filename:  signdict.h
//
//    Description:  
//
//        Version:  1.0
//        Created:  05/07/2014 11:13:22 AM
//       Revision:  none
//       Compiler:  g++
//
//         Author:  Zilong.Zhu (), zilong.whu@gmail.com
//        Company:  edu.whu
//
// =====================================================================================

#ifndef __AGILE_SE_SIGN_DICT_H__
#define __AGILE_SE_SIGN_DICT_H__

#include <string>
#include <vector>
#include "hashtable.h"

class SignDict
{
    public:
        struct key_t
        {
            uint32_t sign1;
            uint32_t sign2;

            bool operator == (const key_t &o) const
            {
                return sign1 == o.sign1 && sign2 == o.sign2;
            }
        };
        struct hash_fun_t
        {
            size_t operator()(const key_t &o) const
            {
                return ((((uint64_t)o.sign1) << 32) | o.sign2);
            }
        };
        struct value_t
        {
            uint32_t id;
            uint32_t offset;
            uint32_t length;
        };
        typedef HashTable<key_t, value_t, hash_fun_t> Hash;
        typedef Hash::ObjectPool ObjectPool;
    private:
        SignDict(const SignDict &);
        SignDict &operator =(const SignDict &);
    public:
        SignDict()
        {
            m_dict = NULL;
            m_buffer = NULL;
            m_max_id = 1;
            m_buffer_pos = 0;
            m_buffer_size = 0;
        }
        ~SignDict()
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

        int init(ObjectPool *pool, uint32_t bucket_size, uint32_t buffer_size)
        {
            if (NULL == pool || 0 == bucket_size || 0 == buffer_size)
            {
                WARNING("invalid args: pool=%p, bucket_size=%u, buffer_size=%u",
                        pool, bucket_size, buffer_size);
                return -1;
            }
            m_ids.reserve(bucket_size);
            m_dict = new Hash(bucket_size);
            if (NULL == m_dict)
            {
                WARNING("failed to new Hash, bucket_size=%u", bucket_size);
                return -1;
            }
            m_dict->set_pool(pool);
            m_buffer = new char[buffer_size];
            if (NULL == m_buffer)
            {
                WARNING("failed to new buffer, size=%u", buffer_size);
                delete m_dict;
                m_dict = NULL;
                return -1;
            }
            m_max_id = 1;
            m_buffer_pos = 0;
            m_buffer_size = buffer_size;
            WARNING("init sign dict ok");
            return 0;
        }

        bool find(uint64_t sign, uint32_t &id) const /* thread safe */
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

        bool find(uint32_t id, std::string &word) const /* do not use this func */
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

        bool find_or_insert(uint64_t sign, const char *word, uint32_t len, uint32_t &id) /* not thread safe */
        {
            if (NULL == word || 0 == len)
            {
                WARNING("invalid args: word=%p, len=%u", word, len);
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
                        WARNING("failed to alloc buffer, size=%u", (uint32_t)(m_buffer_size << 1));
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
                    WARNING("failed to insert word[%*s], sign=%lu", len, word, sign);
                    return false;
                }
                ::memcpy(m_buffer + m_buffer_pos, word, len);
                m_buffer_pos += len;
                id = m_max_id++;
                m_ids.push_back(std::make_pair(value.offset, value.length));
                WARNING("insert word[%*s] ok, id=%u, sign=%lu", len, word, id, sign);
            }
            return true;
        }

        const uint32_t idnum() const { return m_max_id - 1; }
    private:
        Hash *m_dict;
        char *m_buffer;
        uint32_t m_max_id;
        uint32_t m_buffer_pos;
        uint32_t m_buffer_size;
        std::vector<std::pair<uint32_t, uint32_t> > m_ids;
};

#endif
