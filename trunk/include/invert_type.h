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

#include "invert_parser.h"

struct InvertType
{
    uint8_t type;
    uint16_t payload_len;
    char prefix[32];
    InvertParser *parser;
};

struct InvTypes
{
    struct InvType types[UINT8_MAX];                  /*  invert type is offset */

    InvTypes()
    {
        ::memset(types, 0, sizeof types);
        for (size_t i = 0; i < sizeof(types)/sizeof(types[0]); ++i)
        {
            types[i].type = -1;
        }
    }
    bool is_valid_type(uint8_t type) const
    {
        return types[type].type == type;
    }
    uint64_t get_sign(const char *keystr, int8_t type) const
    {
        /* not implemented yet */
        return 0;
    }
};

#endif
