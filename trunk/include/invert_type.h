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

    InvertTypes()
    {
        ::memset(types, 0, sizeof types);
        for (size_t i = 0; i < sizeof(types)/sizeof(types[0]); ++i)
        {
            types[i].type = 0xFF;
        }
    }

    int init(const char *path, const char *file);

    bool is_valid_type(uint8_t type) const
    {
        return types[type].type == type;
    }
    uint64_t get_sign(const char *keystr, uint8_t type) const
    {
        char buffer[256];

        ::snprintf(buffer, sizeof buffer, "%s%s", types[type].prefix, keystr);
        buffer[sizeof(buffer) - 1] = '\0';

        unsigned int md5res[4];
        MD5((unsigned char*)buffer,(unsigned int)::strlen(buffer),(unsigned char*)md5res);

        uint64_t sign = md5res[0]+md5res[1];
        sign <<= 32;
        sign |= md5res[2]+md5res[3];
        return sign;
    }
};

#endif
