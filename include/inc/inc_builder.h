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
