// =====================================================================================
//
//       Filename:  invert_parser.h
//
//    Description:  
//
//        Version:  1.0
//        Created:  03/07/2014 10:24:34 AM
//       Revision:  none
//       Compiler:  g++
//
//         Author:  Zilong.Zhu (), zilong.whu@gmail.com
//        Company:  edu.whu
//
// =====================================================================================

#ifndef __AGILE_SE_INVERT_PARSER_H__
#define __AGILE_SE_INVERT_PARSER_H__

#include <map>
#include <string>
#include "cJSON.h"

class InvertParser
{
    public:
        InvertParser() { }
        virtual ~ InvertParser() { }

        virtual void *parse(cJSON *json) = 0;
};

typedef InvertParser *(*InvertParser_creater)();
extern std::map<std::string, InvertParser_creater> g_invert_parsers;

#define DECLARE_INVERT_PARSER(parser) InvertParser *create_invert_##parser()

#define DEFINE_INVERT_PARSER(parser) \
    InvertParser *create_invert_##parser()\
    {\
        return new parser;\
    }

#define REGISTER_INVERT_PARSER(parser) \
    do\
    {\
        g_invert_parsers[#parser] = create_invert_##parser;\
    } while(0)

#endif
