// =====================================================================================
//
//       Filename:  invert_type.h
//
//    Description:  
//
//        Version:  1.0
//        Created:  03/07/2014 10:29:46 AM
//       Revision:  none
//       Compiler:  g++
//
//         Author:  Zilong.Zhu (), zilong.whu@gmail.com
//        Company:  edu.whu
//
// =====================================================================================

#ifndef __AGILE_SE_INVERT_TYPE_H__
#define __AGILE_SE_INVERT_TYPE_H__

#include <openssl/md5.h>
#include "invert_parser.h"
#include "signdict.h"

struct InvertType
{
    uint8_t type;
    uint16_t payload_len;
    char prefix[32];
    InvertParser *parser;
};

struct InvertTypes
{
    struct InvertType types[256];               /*  invert type is offset */
    SignDict *sign2id_dict;
    std::string m_meta;

    InvertTypes()
    {
        this->clear();
    }
    ~InvertTypes()
    {
        this->destroy();
    }

    int init(const char *path, const char *file);

    bool is_valid_type(uint8_t type) const
    {
        return types[type].type == type;
    }

    void set_sign_dict(SignDict *dict)
    {
        sign2id_dict = dict;
    }

    bool create_sign(const char *keystr, uint8_t type, char *buffer, uint32_t &buffer_len, uint64_t &sign) const
    {
        int len = ::snprintf(buffer, buffer_len, "%s%s", types[type].prefix, keystr);
        if (len < 0)
        {
            WARNING("snprintf error");
            return false;
        }
        if (len >= (int)buffer_len)
        {
            WARNING("too long word[%s], type[%d]", keystr, (int)type);
            return false;
        }
        buffer_len = len;

        unsigned int md5res[4];
        MD5((unsigned char*)buffer,(unsigned int)len,(unsigned char*)md5res);

        sign = md5res[0]+md5res[1];
        sign <<= 32;
        sign |= md5res[2]+md5res[3];

        return true;
    }

    uint32_t get_sign(const char *keystr, uint8_t type) const /* 0 is invalid */
    {
        char buffer[256];
        uint32_t len = sizeof(buffer);
        uint64_t sign;
        uint32_t id = 0;
        if (this->create_sign(keystr, type, buffer, len, sign))
        {
            sign2id_dict->find(sign, id);
        }
        return id;
    }

    uint32_t record_sign(const char *keystr, uint8_t type) /* 0 is invalid */
    {
        char buffer[256];
        uint32_t len = sizeof(buffer);
        uint64_t sign;
        uint32_t id = 0;
        if (this->create_sign(keystr, type, buffer, len, sign))
        {
            sign2id_dict->find_or_insert(sign, buffer, len, id);
        }
        return id;
    }

    void destroy()
    {
        for (size_t i = 0; i < sizeof(types)/sizeof(types[0]); ++i)
        {
            if (types[i].parser)
            {
                delete types[i].parser;
            }
        }
        this->clear();
    }
    void clear()
    {
        ::memset(types, 0, sizeof types);
        for (size_t i = 0; i < sizeof(types)/sizeof(types[0]); ++i)
        {
            types[i].type = 0xFF;
        }
        sign2id_dict = NULL;
        m_meta.clear();
    }
};

#endif
