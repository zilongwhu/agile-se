// =====================================================================================
//
//       Filename:  invert_type.cpp
//
//    Description:  
//
//        Version:  1.0
//        Created:  03/07/2014 12:24:48 PM
//       Revision:  none
//       Compiler:  g++
//
//         Author:  Zilong.Zhu (), zilong.whu@gmail.com
//        Company:  edu.whu
//
// =====================================================================================

#include "log.h"
#include "configure.h"
#include "invert_type.h"

std::map<std::string, InvertParser_creater> g_invert_parsers;

int InvertTypes::init(const char *path, const char *file)
{
    Config config(path, file);
    if (config.parse() < 0)
    {
        WARNING("failed parse config[%s:%s]", path, file);
        return -1;
    }
    int invert_num;
    if (!config.get("invert_num", invert_num))
    {
        WARNING("failed to get invert_num");
        return -1;
    }
    if (invert_num <= 0 || invert_num >= 0xFF)
    {
        WARNING("invalid invert_num[%d], should in [0, 255)", invert_num);
        return -1;
    }
    char buffer[256];
    for (int i = 0; i < invert_num; ++i)
    {
        int type;
        ::snprintf(buffer, sizeof buffer, "invert_%d_type", i);
        if (!config.get(buffer, type))
        {
            WARNING("failed to get %s", buffer);
            goto FAIL;
        }
        int length;
        ::snprintf(buffer, sizeof buffer, "invert_%d_payload_len", i);
        if (!config.get(buffer, length))
        {
            WARNING("failed to get %s", buffer);
            goto FAIL;
        }
        std::string prefix;
        ::snprintf(buffer, sizeof buffer, "invert_%d_prefix", i);
        if (!config.get(buffer, prefix))
        {
            WARNING("failed to get %s", buffer);
            goto FAIL;
        }
        std::string parser;
        ::snprintf(buffer, sizeof buffer, "invert_%d_parser", i);
        if (!config.get(buffer, parser))
        {
            WARNING("failed to get %s", buffer);
            goto FAIL;
        }
        WARNING("invert[%d]: type=%d, payload_len=%d, prefix=%s, parser=%s",
                i, type, length, prefix.c_str(), parser.c_str());
        if (type < 0 || type >= 0xFF
                || length < 0 || length > 0xFFFF
                || prefix.length() == 0
                || prefix.length() >= sizeof(types[i].prefix)
                || parser.length() == 0)
        {
            WARNING("invalid config for invert[%d]", i);
            goto FAIL;
        }
        std::map<std::string, InvertParser_creater>::iterator it = g_invert_parsers.find(parser);
        if (it == g_invert_parsers.end())
        {
            WARNING("unsupported invert parser[%s]", parser.c_str());
            goto FAIL;
        }
        types[i].type = type;
        types[i].payload_len = length;
        ::snprintf(types[i].prefix, sizeof(types[i].prefix), "%s", prefix.c_str());
        types[i].parser = (*it->second)();
    }
    return 0;
FAIL:
    this->destroy();
    return -1;
}
