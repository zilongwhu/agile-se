#ifndef __AGILE_SE_INC_BUILDER_H__
#define __AGILE_SE_INC_BUILDER_H__

#include <stdint.h>
#include <stdlib.h>
#include <errno.h>
#include <string>
#include <vector>
#include "cJSON.h"
#include "log_utils.h"
#include "index/index.h"

namespace inc
{
    enum { FORWARD_LEVEL = 2, INVERT_LEVEL = 3, };
    enum { OP_INSERT = 0, OP_DELETE = 1, OP_UPDATE = 2 };

    struct forward_data_t
    {
        const char *key;
        cJSON *value;

        bool build(cJSON *value)
        {
            key = value->string;
            if (NULL == key || '\0' == key[0])
            {
                P_WARNING("invalid key");
                return false;
            }
            this->value = value;
            return true;
        }
    };
    
    struct invert_data_t
    {
        int type;
        const char *key;
        cJSON *value;

        bool build(int type, cJSON *value)
        {
            key = value->string;
            if (NULL == key || '\0' == key[0])
            {
                P_WARNING("invalid key");
                return false;
            }
            this->type = type;
            this->value = value;
            return true;
        }
    };

    cJSON *parse_forward_json(const std::string &json, std::vector<forward_data_t> &values);
    cJSON *parse_invert_json(const std::string &json, std::vector<invert_data_t> &values);
    
    struct inc_builder_t;
    typedef void (*inc_timer_t)(inc_builder_t *args);

    struct inc_builder_t
    {
        inc_builder_t()
        {
            idx = NULL;
            timer = NULL;
        }

        Index *idx;
        inc_timer_t timer;
    };
    
    int inc_build_index(inc_builder_t *args);
};

#endif
