#ifndef __AGILE_SE_INVERT_TYPE_H__
#define __AGILE_SE_INVERT_TYPE_H__

#include "index/signdict.h"
#include "index/invert_parser.h"

struct InvertType
{
    uint8_t type;
    uint16_t payload_len;
    char prefix[32];
    InvertParser *parser;
};

struct InvertTypes
{
    struct InvertType types[256]; /* invert type is offset */
    SignDict *sign2id_dict;
    std::string m_meta;

    InvertTypes() { this->clear(); }
    ~InvertTypes() { this->destroy(); }

    int init(const char *path, const char *file);
    void set_sign_dict(SignDict *dict)
    {
        sign2id_dict = dict;
    }

    bool create_sign(const char *keystr, uint8_t type,
            char *buffer, uint32_t &buffer_len, uint64_t &sign) const;

    bool is_valid_type(uint8_t type) const;
    uint32_t get_sign(const char *keystr, uint8_t type) const; /* 0 is invalid */
    uint32_t record_sign(const char *keystr, uint8_t type); /* 0 is invalid */

    void destroy();
    void clear();
};

#endif
