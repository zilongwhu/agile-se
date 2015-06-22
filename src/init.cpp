#include "parse/parser.h"
#include "pool/delaypool.h"
#include "index/index.h"
#include "index/const_index.h"

void init_time_updater();

namespace inc
{
    DECLARE_INC_PROCESSOR(das_processor);
}

void init_nbslib()
{
    init_time_updater();
    init_operator_priority();

    REGISTER_INC_PROCESSOR(inc::das_processor);
}
