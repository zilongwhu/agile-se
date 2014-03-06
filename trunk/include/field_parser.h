// =====================================================================================
//
//       Filename:  field_parser.h
//
//    Description:  
//
//        Version:  1.0
//        Created:  03/05/2014 06:03:18 PM
//       Revision:  none
//       Compiler:  g++
//
//         Author:  Zilong.Zhu (), zilong.whu@gmail.com
//        Company:  edu.whu
//
// =====================================================================================

#ifndef __AGILE_SE_FIELD_PARSER_H__
#define __AGILE_SE_FIELD_PARSER_H__

#include <map>
#include <string>
#include "cJSON.h"

class FieldParser
{
    public:
        FieldParser() { }
        virtual ~ FieldParser() { }

        virtual bool parse(cJSON *json, void *&obj) = 0;
        virtual void *clone(void *obj) = 0;
        virtual void destory(void *obj) = 0;
        virtual bool serialize(void *obj, void *buf, size_t buf_len, size_t &body_len) = 0;
        virtual bool deserialize(void *buf, size_t len, void *&obj) = 0;
};

typedef FieldParser *(*FieldParser_creater)();
extern std::map<std::string, FieldParser_creater> g_field_parsers;

#define DECLARE_FIELD_PARSER(parser) FieldParser *create_field_##parser()

#define DEFINE_FIELD_PARSER(parser) \
    FieldParser *create_field_##parser()\
    {\
        return new parser;\
    }

#define REGISTER_FIELD_PARSER(parser) \
    do\
    {\
        g_field_parsers[#parser] = create_field_##parser;\
    } while(0)

#endif
