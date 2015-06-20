#include <sstream>
#include <stdio.h>
#include <openssl/md5.h>
#include "configure.h"
#include "log_utils.h"
#include "index/invert_type.h"

std::map<std::string, InvertParser_creater> g_invert_parsers;

int InvertTypes::init(const char *path, const char *file)
{
    std::ostringstream oss;
    Config config(path, file);
    if (config.parse() < 0)
    {
        P_WARNING("failed parse config[%s:%s]", path, file);
        return -1;
    }
    int invert_num;
    if (!config.get("invert_num", invert_num))
    {
        P_WARNING("failed to get invert_num");
        return -1;
    }
    if (invert_num <= 0 || invert_num >= 0xFF)
    {
        P_WARNING("invalid invert_num[%d], should in [0, 255)", invert_num);
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
            P_WARNING("failed to get %s", buffer);
            goto FAIL;
        }
        oss << buffer << ": " << type << std::endl;
        int length;
        ::snprintf(buffer, sizeof buffer, "invert_%d_payload_len", i);
        if (!config.get(buffer, length))
        {
            P_WARNING("failed to get %s", buffer);
            goto FAIL;
        }
        oss << buffer << ": " << length << std::endl;
        std::string prefix;
        ::snprintf(buffer, sizeof buffer, "invert_%d_prefix", i);
        if (!config.get(buffer, prefix))
        {
            P_WARNING("failed to get %s", buffer);
            goto FAIL;
        }
        oss << buffer << ": " << prefix << std::endl;
        std::string parser;
        ::snprintf(buffer, sizeof buffer, "invert_%d_parser", i);
        if (!config.get(buffer, parser))
        {
            P_WARNING("failed to get %s", buffer);
            goto FAIL;
        }
        oss << buffer << ": " << parser << std::endl;
        P_WARNING("invert[%d]: type=%d, payload_len=%d, prefix=%s, parser=%s",
                i, type, length, prefix.c_str(), parser.c_str());
        if (type < 0 || type >= 0xFF
                || length < 0 || length > 0xFFFF
                || prefix.length() == 0
                || prefix.length() >= sizeof(types[i].prefix)
                || parser.length() == 0)
        {
            P_WARNING("invalid config for invert[%d]", i);
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

bool InvertTypes::is_valid_type(uint8_t type) const
{
    return types[type].type == type;
}

bool InvertTypes::create_sign(const char *keystr, uint8_t type,
        char *buffer, uint32_t &buffer_len, uint64_t &sign) const
{
    int len = ::snprintf(buffer, buffer_len, "%s%s", types[type].prefix, keystr);
    if (len < 0)
    {
        P_WARNING("snprintf error");
        return false;
    }
    if (len >= (int)buffer_len)
    {
        P_WARNING("too long word[%s], type[%d]", keystr, (int)type);
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

uint32_t InvertTypes::get_sign(const char *keystr, uint8_t type) const /* 0 is invalid */
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

uint32_t InvertTypes::record_sign(const char *keystr, uint8_t type) /* 0 is invalid */
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

void InvertTypes::destroy()
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

void InvertTypes::clear()
{
    ::memset(types, 0, sizeof types);
    for (size_t i = 0; i < sizeof(types)/sizeof(types[0]); ++i)
    {
        types[i].type = 0xFF;
    }
    sign2id_dict = NULL;
    m_meta.clear();
}
