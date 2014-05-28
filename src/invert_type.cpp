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

#include <sstream>
#include "log.h"
#include "configure.h"
#include "invert_type.h"

std::map<std::string, InvertParser_creater> g_invert_parsers;

int InvertTypes::init(const char *path, const char *file)
{
    std::ostringstream oss;
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
    oss << "invert_num: " << invert_num << std::endl;
    char buffer[256];
    for (int i = 0; i < invert_num; ++i)
    {
        oss << std::endl;

        int type;
        ::snprintf(buffer, sizeof buffer, "invert_%d_type", i);
        if (!config.get(buffer, type))
        {
            WARNING("failed to get %s", buffer);
            goto FAIL;
        }
        oss << buffer << ": " << type << std::endl;
        int length;
        ::snprintf(buffer, sizeof buffer, "invert_%d_payload_len", i);
        if (!config.get(buffer, length))
        {
            WARNING("failed to get %s", buffer);
            goto FAIL;
        }
        oss << buffer << ": " << length << std::endl;
        std::string prefix;
        ::snprintf(buffer, sizeof buffer, "invert_%d_prefix", i);
        if (!config.get(buffer, prefix))
        {
            WARNING("failed to get %s", buffer);
            goto FAIL;
        }
        oss << buffer << ": " << prefix << std::endl;
        std::string parser;
        ::snprintf(buffer, sizeof buffer, "invert_%d_parser", i);
        if (!config.get(buffer, parser))
        {
            WARNING("failed to get %s", buffer);
            goto FAIL;
        }
        oss << buffer << ": " << parser << std::endl;
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
        types[type].type = type;
        types[type].payload_len = length;
        ::snprintf(types[type].prefix, sizeof(types[type].prefix), "%s", prefix.c_str());
        types[type].parser = (*it->second)();
    }
    m_meta = oss.str();
    return 0;
FAIL:
    this->destroy();
    return -1;
}
